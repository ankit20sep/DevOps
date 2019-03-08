package com.rrb.spark.test
 
import org.apache.spark.sql.SparkSession
 
case class Action(name: String, value: Long)
object SumOfA {
  def main(args: Array[String]): Unit = {
 
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Assignment 1")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
 
    import spark.implicits._
 
    val inputDF = spark.sparkContext
      .textFile("data/BigFile.txt")
      .map(_.split("-"))
      .filter(_.contains("a"))
      .map(r => Action(r(0), r(1).trim().toInt))
      .toDF()
 
    inputDF.collect().foreach(println)
 
    inputDF.createOrReplaceTempView("input")
 
    val resultDF = spark.sql("SELECT SUM(value) FROM input")
    resultDF.collect().foreach(println)
 
    //val sum = resultDF.map(x=>)
 
  }
}






