package sparkavro

import com.esotericsoftware.kryo.Kryo
import org.apache.avro.generic.GenericRecord
import org.apache.spark.SparkConf
import org.apache.spark.serializer.{KryoRegistrator, KryoSerializer}

class KryoRegistration extends KryoRegistrator{

  override def registerClasses(kryo: Kryo): Unit = {

    kryo.register(classOf[GenericRecord])
  }
}

object KryoRegistration{

  def register(conf : SparkConf): Unit = {

    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[KryoRegistration].getName)
  }
}