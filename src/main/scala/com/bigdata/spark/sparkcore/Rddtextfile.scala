package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Rddtextfile {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Rddtextfile").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Rddtextfile").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("Rddtextfile").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data = "file:///D:\\work\\Sparkstudymaterials\\dataset\\2008.csv"
    val output = "file:///D:\\work\\Sparkstudymaterials\\dataset\\2008out"
    val file = sc.textFile(data)
    val cfile = file.map(x=>x.replaceAll("\"",""))
    val splitrdd = cfile.map(x=>x.split(","))
    val proc= splitrdd.map(x=>(x(0),x(1),x(2),(3),x(4),x(5))).filter(x=>x._1.contains("2008"))
    proc.take(5).foreach(println)
    proc.saveAsTextFile(output)
    spark.stop()
  }
}