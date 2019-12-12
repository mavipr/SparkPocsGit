package com.bigdata.spark.sparkStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object kafkaProducer {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("kafkaProducer").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("kafkaProducer").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("kafkaProducer").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    import scala.util.Try
    //val path = args(0)
    val path="D:\\work\\Sparkstudymaterials\\dataset\\Path"
    val data = spark.sparkContext.textFile(path)
    val topic ="indba"

    data.foreachPartition(a => {
    //mani,32,chn
      import java.util._

      val props = new java.util.Properties()
      //  props.put("metadata.broker.list", "localhost:9092")
      //      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("bootstrap.servers", "localhost:9092")

      // import kafka.producer._
      // val config = new ProducerConfig(props)
      val producer = new KafkaProducer[String, String](props)

      a.foreach(x => {
        println(x)
        producer.send(new ProducerRecord[String, String](topic.toString(), x.toString)) //sending to kafka broker
        //(topic, "venu,32,hyd")
        //(indpak,"anu,56,mas")
        Thread.sleep(5000)

      })

    })
    ssc.start() // Start the computation
    ssc.awaitTermination()
  }
}