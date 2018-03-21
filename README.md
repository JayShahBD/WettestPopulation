# WettestPopulation
Program that calculates the population wetness of an MSA

1. Files you need to compile and run the code.
  i.    msa_loc.txt : City-State-MSA data 
  ii.   201505precip.txt : Precipitation-YMD-Hour-WBAN data
  iii.  201505station.txt : WBAN-City-State data
  iv.   pop_2015.csv : MSA-State-Population-LSAD data
    You can download the first 3 files from : http://www.ncdc.noaa.gov/orders/qclcd/QCLCD201505.zip
    You can download the population file from: https://www.census.gov/data/tables/2016/demo/popest/total-metro-and-micro-statistical-areas.html#ds
2. copy_file.sh : Base script to copy files from "Edge node" to HDFS
3. mkcp.sh : Make directories and copy files
4. hive_table_result.hql : Hive Query to create tables and load final result from Spark
5. wetMsa.scala : Scala code for the workflow
6. pom.xml : Dependencies 
