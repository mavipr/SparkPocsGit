package com.bigdata.spark.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object jsonDF {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("jsonDF").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("jsonDF").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("jsonDF").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data ="file:///D:\\work\\Sparkstudymaterials\\dataset\\zips.json"
    val df = spark.read.format("json").load(data)
    df.show()
    df.printSchema()
    df.createOrReplaceTempView("jsontab")
 //process array
    val cleandf = spark.sql("select _id id,city,loc[0] longitude,loc[1] latitude,pop,state from jsontab")
    cleandf.show()
    cleandf.printSchema()
    cleandf.createOrReplaceTempView("cjsontab")
    val res = cleandf.groupBy($"city").count.orderBy($"count".desc).show()
    spark.sql("select city,count(*) cnt from cjsontab group by city order by cnt desc").show()

    spark.stop()
  }
}