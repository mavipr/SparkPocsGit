package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Helloworld {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Helloworld").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Helloworld").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("Helloworld").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

      println("Hello mani! now are big data engineer")

    spark.stop()
  }
}