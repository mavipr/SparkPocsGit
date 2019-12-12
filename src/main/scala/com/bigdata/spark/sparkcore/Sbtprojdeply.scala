package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Sbtprojdeply {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Sbtprojdeply").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Sbtprojdeply").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("Sbtprojdeply").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val indata ="file:///D:\\work\\Sparkstudymaterials\\dataset\\2008.csv"
    val rdd = sc.textFile(indata)
    val proc = rdd.map(_.split(",")).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5))).filter(x=>x._3.contains("3"))
    proc.take(5).foreach(println)
    spark.stop()
  }
}