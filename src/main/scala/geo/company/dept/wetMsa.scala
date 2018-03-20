package geo.company.dept

import java.text.SimpleDateFormat
import java.util.Date

import sparkavro.KryoRegistration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

object wetMsa {

  // dropHeader func
  def dropHeader(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx == 0) {
        lines.drop(1)
      }
      lines
    })
  }


  // Parse Precipitation data
  def parsePrecipLines(line:String) ={
    val fields = line.split(",")
    val wban = fields(0)
    val ymd = fields(1)
    val hour = fields(2)
    val precipitation = fields(3).trim()
    (wban, ymd, hour, precipitation)
  }

  // Parse Station data
  def parseStatLines(line:String) ={
    val fields = line.split("\\|")
    val wban = fields(0)
    val city = fields(6)
    val state = fields(7)
    (wban, city, state)
  }

  // Parse msa_loc data
  def parseMsaLoc(line:String) ={
    val fields = line.split("\\t")
    val city = fields(0).trim
    val state = fields(1).trim
    val msa = fields(2)
    (city.toUpperCase, state.toUpperCase, msa.replaceAll("\"","").trim)
  }

  // Parse Population data
  def parsePopLines(line:String) ={
    val fields = line.split(",")
    if (fields.length >1) {
      val msa = fields(3).trim.replaceAll("\"", "")
      val state = fields(4).trim.replaceAll("\"", "")
      val pop = fields(13).trim
      val lsad = fields(5).trim
      (msa.replaceAll("\"", "")+", "+state, pop, lsad)
    }

    else {
      ("unknown","unknown","unknown")
    }
  }

  def main(args: Array[String]) ={

    // Defining variables
    val precip_fp = "hdfs://quickstart.cloudera:8020/user/cloudera/FP/pfiles/precip/*.txt"
    val msa_loc_fp = "hdfs://quickstart.cloudera:8020/user/cloudera/FP/pfiles/msa_loc/*.txt"
    val pop_fp = "hdfs://quickstart.cloudera:8020/user/cloudera/FP/pfiles/pop/*.csv"
    val stat_fp = "hdfs://quickstart.cloudera:8020/user/cloudera/FP/pfiles/stat/*.txt"

    // Initializing Spark Context
    val sparkConf = new SparkConf().setAppName("spark table write process") // DONE - master (local or YARN) is set while submitting the job
      .set("spark.hadoop.validateOutputSpecs", "false").set("spark.storage.memoryFraction", "1").setMaster("local")
      .set("spark.sql.parquet.compression.codec", "gzip")
    KryoRegistration.register(sparkConf)
    val sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    // Enables reading directory recursively
    val blockSize = 1024 * 1024 * 256 // Same as HDFS block size
    /*Intializing the spark context with block size values*/
    sc.hadoopConfiguration.setInt("dfs.blocksize", blockSize)
    sc.hadoopConfiguration.setInt("parquet.block.size", blockSize)

    val sqlContext = new SQLContext(sc)
    // val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val vconf = new Configuration()
    val fs = FileSystem.get(vconf)
    import sqlContext.implicits._

    val regexStrip = "[{}']".r
    val theConfigFileName = args(0)

    val paramFile = sc.textFile(theConfigFileName).filter(f => (f.trim.length() > 0 && f.substring(0, 1) != "#"))
      .map(line => regexStrip.replaceAllIn(line, ""))
    val parameters = paramFile
      .map(line => {
        val fields = line.split("=")
        (fields(0).toString, fields(1).toString)
      })

    val pm = parameters.collect
    val p = Map(pm: _*)

    val dateFormatter = new SimpleDateFormat("yyyyMMddhhmmss")
    val submittedDateConvert = new Date()
    val run_id = dateFormatter.format(submittedDateConvert)

    val p_hms_server = p.apply("HIVE_METASTORE_URL")
    val p_nn_server = p.apply("NAME_NODE_URL")
    val dbName = p.apply("HIVE_FACT_DB")
    val reducer = p.apply("p_numReducers")
    val output_fp = "hdfs://quickstart.cloudera:8020/user/cloudera/FP/output/wettestpopulation/"


    /* Working with Precip data */

    // Load precip file in RDD
    val precipLines = sc.textFile(precip_fp)
    // Remove Header
    val wHPrecip: RDD[String] = dropHeader(precipLines)
    // Parse precip lines
    val precipParsedLines = wHPrecip.map(parsePrecipLines)
    // Format Data types (WBAN, Year, Month, Hour, Precipitation)
    val forPrecipData = precipParsedLines.map(x=> (x._1, (x._2.substring(0, 4)), (x._2.substring(4, 6)), x._3, x._4))
    // Convert RDD to DF
    val precipDF = forPrecipData.toDF("WBAN", "Year", "Month", "Hour", "Precipitation")
    // Change data type of the hour and precip in DF
    val precipFDF = precipDF.withColumn("HourTemp", precipDF.col("Hour").cast(IntegerType)).drop("Hour").withColumnRenamed("HourTemp", "Hour")
    val precipFilterDF = precipFDF.withColumn("PrecipitationTemp", precipFDF.col("Precipitation").cast(FloatType)).drop("Precipitation").withColumnRenamed("PrecipitationTemp","Precipitation")
    // Remove data between between 12:00midnight to 7:00a.m
    val ignoreNt= precipFilterDF.filter(precipFilterDF("Hour") > 7)
    // Ignore empty precip column and data that contains "T"
    val ignoreEmp = ignoreNt.filter(ignoreNt("Precipitation") > 0)
    // Groupby WBAN, Year and Month then sum the precip
    val precipDFFinal = ignoreEmp.groupBy("WBAN","Year","Month").sum("Precipitation")


    /* Working with Station data */

    // Load station file in RDD
    val statLines = sc.textFile(stat_fp)
    // Removing Header
    val wHStat: RDD[String] = dropHeader(statLines)
    // Parse stat lines
    val statParsedLines = wHStat.map(parseStatLines)
    // Convert RDD to DF
    val statDF = statParsedLines.toDF("WBAN", "Name", "State")
    // Join precip data with station data on WBAN
    val joinPrecipStat = precipDFFinal.join(statDF, precipDFFinal("WBAN")=== statDF("WBAN"))



    /* Working with msa_loc data */

    // Load msa_loc file in RDD
    val msaLocLines = sc.textFile(msa_loc_fp)
    // Remove header
    val wHMsaLoc: RDD[String] = dropHeader(msaLocLines)
    // Parse msa_loc lines
    val parsedMsaLocLines = wHMsaLoc.map(parseMsaLoc)
    // Convert RDD to DF
    val msaLocDF = parsedMsaLocLines.toDF("CITY","STATE","msa")
    // Join msa_loc to precipStat by Name
    val joinMsaStat = joinPrecipStat.join(msaLocDF, (joinPrecipStat("Name")=== msaLocDF("CITY")) && (joinPrecipStat("State")=== msaLocDF("STATE")))
    // Groupby msa, year, month and take avg of the precipitation
    val finalMsaLoc = joinMsaStat.groupBy("msa","Year", "Month").avg("sum(Precipitation)").withColumnRenamed("avg(sum(Precipitation))","totalRainfall")


    /* Working with Population data */

    // Load pop file in RDD
    val popLines = sc.textFile(pop_fp)
    // Parse pop lines
    val parsedPopLines = popLines.map(parsePopLines)
    // Filter the header and get all lines which are MSA qualified
    val filParsedPopLines = parsedPopLines.filter(x=> x._1!="unknown" && x._3.equals("Metropolitan Statistical Area")).map(x=>(x._1,x._2))
    // Convert RDD to DF
    val popDF = filParsedPopLines.toDF("msa","pop")
    // Join finalMsaLoc to popDF
    val joinPopMsaLoc = finalMsaLoc.join(popDF, finalMsaLoc("msa") === popDF("msa")).select(popDF("msa"),popDF("pop"),finalMsaLoc("Year"),finalMsaLoc("Month"),finalMsaLoc("totalRainfall"))
    // Add column wettestpopulation and find the product of pop and totalRainfall
    val wettestPopDF = joinPopMsaLoc.withColumn("WettestPopulation", joinPopMsaLoc("pop").cast("Int")*joinPopMsaLoc("totalRainfall"))
    // Sort and repartition DF in one output file
    val wettestPopDf_repartitioned = wettestPopDF.sort("WettestPopulation").repartition(1)
    // Save output in csv format
    wettestPopDf_repartitioned.write.format("com.databricks.spark.csv").option("delimiter", "\t").mode("overwrite").save("hdfs://quickstart.cloudera:8020/user/cloudera/FP/output/wettestpopulation/")
    sc.stop()

  }
}