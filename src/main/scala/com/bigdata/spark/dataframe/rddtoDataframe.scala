package com.bigdata.spark.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
//Add these two jars for programatically specified schema
// Import Row.
import org.apache.spark.sql.Row;

// Import Spark SQL data types
import org.apache.spark.sql.types.{StructType,StructField,StringType};



object rddtoDataframe {
 //Case class must be in out of main method
case class airline(Year:String,Month:String,DayofMonth:String,DayOfWeek:String,DepTime:String,CRSDepTime:String)

  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("toDFexample").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("toDFexample").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("toDFexample").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data = "file:///D:\\work\\Sparkstudymaterials\\dataset\\2008.csv"
    val rdd = sc.textFile(data)
    val cleanrdd = rdd.map(x=>x.replace("\"",""))
    val header = rdd.first()
   // 1#  using toDF to convert RDD to Dataframe
    /********************************************************************************/
    val df1 = cleanrdd.filter(x=>(x!=header)).map(_.split(",")).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5))).toDF("year","month","dayofmonth","dayofweek","deptime","crs")
    df1.createOrReplaceTempView("dftab")

    //2# using Case class to convert RDD to Dataframe
    /********************************************************************************/
    val df2 = cleanrdd.filter(x=>(x!=header)).map(_.split(",")).map(x=>airline(x(0),x(1),x(2),x(3),x(4),x(5))).toDF()
    df2.createOrReplaceTempView("df2tab")
   // spark.sql("select * from df2tab limit 15").show()

    //3# using programatically specified schema to convert rdd to dataframe
/********************************************************************************/
    //val schema = new StructType().add("Year","string").add("Month","String")
    val cols = "year,month,dayofmonth,dayofweek,deptime,crs"
    val schema =
      StructType(
        cols.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val rawdata = cleanrdd.filter(x=>(x!=header)).map(_.split(",")).map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5)))
        val df3 = spark.createDataFrame(rawdata,schema)
      df3.createOrReplaceTempView("dftab3")
    //spark.sql("select * from dftab3 limit 15").show()

    //4# using dataframe API to convert rdd to dataframe
    /********************************************************************************/
   val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    df.show()

    spark.stop()
  }
}