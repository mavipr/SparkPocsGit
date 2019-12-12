package com.bigdata.spark.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object complxJson1 {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("complxJson1").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("complxJson1").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("complxJson1").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val data1 ="file:///D:\\work\\Sparkstudymaterials\\dataset\\911.csv"
    val df1 = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data1)
    val data2 = "file:///D:\\work\\Sparkstudymaterials\\dataset\\zipcode.csv"
    val df2 =  spark.read.format("source").option("header","true").option("inferSchema","true").load(data2)
    df1.show()
    df2.show()

    val data = "D:\\work\\Sparkstudymaterials\\dataset\\world_bank (1).json"
    val df = spark.read.format("json").load(data)
    df.printSchema()
    df.createOrReplaceTempView("tb1")
    // process struct
    val cleandf = spark.sql("select `_id`.`$oid` Identifier, theme1.Name themename,theme1.Percent thempercent from tb1").show()
    // Process array(struct)
    val proc = spark.sql("select totalamt,themecode,theme1.Name themename,theme1.Percent themePercent,tn.code themecode,tn.name themn,sn.code seccode, sn.name secname " +
      "from tb1 lateral view explode (theme_namecode) t as tn lateral view explode(sector_namecode) s as sn").show()
  //replace column names
    val cols = df.columns.map(x=>x.replace("_",""))
    val ndf = df.toDF(cols:_*)
    ndf.printSchema()
    //drop column
    val clean = ndf.drop($"url")
    clean.printSchema
    spark.stop()
  }
}