package com.bigdata.spark.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object dfDSLoperation {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("dfDSLoperation").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("dfDSLoperation").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("dfDSLoperation").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data = "file:///D:\\work\\Sparkstudymaterials\\dataset\\us-500.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    df.printSchema

    df.first()
    df.take(5)
    df.head(3)
    df.select($"first_name",$"last_name",$"city",$"email",$"web").show()
    df.select($"first_name",$"last_name",$"city",$"email",$"web").where($"email".like("%hotmail%")).show()
    df.groupBy($"state").count().show()
    df.show(5,false)
    df.columns
    df.schema
    val ndf = df.withColumnRenamed("first_name","fname")
    ndf.printSchema()
    val cdf = ndf.withColumn("zip", $"zip".cast("Long"))


    spark.stop()
  }
}