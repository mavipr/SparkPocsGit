package com.bigdata.spark.sparkStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object kafkaConsumer {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("kafkaConsumer").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("kafkaConsumer").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("kafkaConsumer").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    import scala.util.Try
    val topics = "indba"
    val brokers = "localhost:9092"
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "bootstrap.servers"->"localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kaf",
      "key.serializer"->"org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer"->"org.apache.kafka.common.serialization.StringSerializer",
      "key.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer")

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,         // this is to run the
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    val lines = messages  // lines is a dStream  map(x=>x.value) or map(x=>x.key)

    lines.foreachRDD { x =>

      println(s"processing first line : $x")
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      //val df1=spark.read.format("csv").option("delimiter"," ").load(data)
      //val dfschema=df1.schema()

      val df = x.map(x=>x.value).map(x => x.split(",")).map(x => (x(0), x(1), x(2))).toDF("name", "age", "city")
      // val df = spark.read.format("csv").schema(dfschema).load(data)
      df.show()
    }
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

   }
}