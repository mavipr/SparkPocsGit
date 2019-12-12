package com.poc

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object sparkRDD {

  case class Olympic(Name: String, Age: Int, Country: String, year: Int, date: String, event: String, Mach: Int, tot: Int, lost: Int, win: Int)

  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkRDD").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkRDD").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("sparkRDD").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val num = List("Manikandan","Vijayalakshmi","Pranav","Sai Prajan")
    val p_rdd = sc.parallelize(num)
    p_rdd.collect()
    val data = ("file:///D:\\work\\Sparkstudymaterials\\dataset\\Olympic_data.csv")
    val rdd = sc.textFile(data)
    /*to check header*/
    val header = rdd.first()
    /*to check length*/
    val len = rdd.map(_.split(",")).map(x=>x.length)

    val  regix= ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val cleanrdd = rdd.map(_.split(regix)).filter(x =>(x.length>=10)).map(x=>Olympic(x(0),x(1).toInt,x(2),x(3).toInt,x(4),x(5),x(6).toInt,x(7).toInt,x(8).toInt,x(9).toInt))
 /*ReduceByKey*/
    //val combinerdd = cleanrdd.map(x=>((x._1+","+x._2),1)).reduceByKey(_+_).sortByKey(false)
    // combinerdd.take(10).foreach(println)
    /*  .map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17))).take(10).foreach(println)*/
/*filter*/
    val medalcount = rdd.map(_.split(regix)).filter(x =>(x.length>=10)).map(x=>(x(0),x(2),x(3).toInt,x(5),x(9).toInt))
    val combinerdd = medalcount.map(x=>((x._1+", "+x._2+", "+x._3+", "+x._4),1)).reduceByKey(_+_).sortBy(_._2,false)
    combinerdd.take(20).foreach(println)
    spark.stop()
  }
}