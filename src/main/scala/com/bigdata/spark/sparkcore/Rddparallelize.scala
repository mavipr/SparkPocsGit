package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Rddparallelize {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Rddparallelize").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Rddparallelize").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("Rddparallelize").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val num = 1 to 10 toArray
    val numrdd = sc.parallelize(num)
    val res1= numrdd.filter(x=>x>5).collect()


    val names = Array("mani","vijy","pranav")
    val namerdd = sc.parallelize(names)
    val res = namerdd.map(x=>x.toUpperCase).foreach(println)

    spark.stop()
  }
}