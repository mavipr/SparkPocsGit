package com.bigdata.spark.dataSet

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
case class usdata (first_name:String, last_name:String, company_name:String, address:String, city:String, county:String, state:String, zip:Int, phone1:String, phone2:String, email:String, web:String)
case class grby(state:String,cnt:Long)
object datasetapi {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("datasetapi").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("datasetapi").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("datasetapi").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data = "file:///D:\\work\\Sparkstudymaterials\\dataset\\us-500.csv"
    //creating dataset using as [case class name]
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data).as[usdata]
    df.createOrReplaceTempView("dstab")
 //dataframe
    val res = spark.sql("select * from dstab where state='NY'")
  //convert dataframe to dataset using as case class name
    val res1 = spark.sql("select * from dstab where state='NY'").as[usdata]
    val gb = spark.sql("select state,count(*) cnt from dstab group by state order by cnt desc").as[grby]
    gb.show()
    res1.show()
    spark.stop()
  }
}