package com.bigdata.spark.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import scala.io.Source
object importToRDBMS {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("importToRDBMS").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("importToRDBMS").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("importToRDBMS").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val url="jdbc:mysql://database-1.cwy7kzq805ai.ap-south-1.rds.amazonaws.com:3306/tempdb"
    val oprop = new java.util.Properties()
    oprop.setProperty("user","musername")
    oprop.setProperty("password","mpassword")
    oprop.setProperty("driver","com.mysql.cj.jdbc.Driver")

    /*val data ="file:///D:\\work\\Sparkstudymaterials\\dataset\\ca-500.csv"
    val df = spark.read.format("csv").option("header","true").load(data)
    df.write.jdbc(url,"bank",oprop)
    val df = spark.read.jdbc(url,"customer",oprop)
    df.show()
    df.write.format("csv").mode("Append")jdbc(url,"customer1",oprop)
    val query = "(select * from customer where city in ('Abbotsford','Ajax')) tmp"
    val qdf = spark.read.jdbc(url,query,oprop)
    qdf.show()
    val qfile = "D:\\work\\Sparkstudymaterials\\dataset\\sqltext.txt"
    val lines = Source.fromFile(qfile).getLines.mkString
    val df = spark.read.jdbc(url,lines,oprop)
    df.show()*/
    val query ="(select table_name from all_tables) tmp"
    val df =spark.read.jdbc(url,query,oprop)
    val tables = df.select("table_name").rdd.map(x=>x(0)).collect.toList
    tables.foreach { t =>
      val tab = t.toString()
      val df = spark.read.jdbc(url,tab,oprop)
    df.show()
    }
    spark.stop()
  }
}