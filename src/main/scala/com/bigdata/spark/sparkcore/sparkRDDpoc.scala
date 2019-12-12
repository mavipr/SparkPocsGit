package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object sparkRDDpoc {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkRDDpoc").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkRDDpoc").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("sparkRDDpoc").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data ="D:\\work\\Sparkstudymaterials\\dataset\\911.csv"
    val rdd = sc.textFile(data)
    val head = rdd.first()
    val proc = rdd.map(_.split(",")).filter(x=>(x!=head)).filter(x=>x.length<=9).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
    val trans = proc.filter(x=>(x._4.contains("19525")))
    trans.collect().take(10).foreach(println)


    proc.collect.take(10).foreach(println)
    spark.stop()
  }
}