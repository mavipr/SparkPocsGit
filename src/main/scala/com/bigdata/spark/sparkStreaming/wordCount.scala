package com.bigdata.spark.sparkStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.streaming._
object wordCount {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("wordCount").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("wordCount").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    val ssc = new StreamingContext(sc, Seconds(10))
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("ec2-13-127-28-131.ap-south-1.compute.amazonaws.com", 1235)
    //val res = lines.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((a,b)=>a+b)
    //res.print()

   // val wres = lines.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKeyAndWindow((x:Int,y:Int)=>(x+y),Minutes(3),Seconds(30))
     // wres.print()
    //val rdf = lines.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2)))
    //rdf.print()
    //rdf.saveAsTextFiles("")
    lines.foreachRDD { x =>
    val rdd = x.toString()
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val data = x.map(a=>a.split(",")).map(a=>(a(0),a(1),a(2))).toDF("Name","Age","City")
      data.show()
      //data.createOrReplaceTempView("hivtab")
      //val res = spark.sql("select * from hivtab").show()
      val url="jdbc:mysql://database-1.cwy7kzq805ai.ap-south-1.rds.amazonaws.com:3306/tempdb"
      val oprop = new java.util.Properties()
      oprop.setProperty("user","musername")
      oprop.setProperty("password","mpassword")
      oprop.setProperty("driver","com.mysql.cj.jdbc.Driver")
      data.write.jdbc(url,"stream",oprop)
    }
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
    //spark.stop()
  }
}