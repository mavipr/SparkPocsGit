package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Groupby {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Groupby").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Groupby").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("Groupby").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data = args(0)
    val output = args(1)
    val rdd = sc.textFile(data,10)
    val proc = rdd.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)
    proc.take(10).foreach(println)
    proc.coalesce(2).saveAsTextFile(output)
    spark.stop()
  }
}