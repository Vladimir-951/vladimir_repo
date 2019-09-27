package com.example.spark_part2_practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ClickStreamExample extends App{

  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._
  //val csFolder = args(0)

  val userEvents = spark.read.json(path = s"D:\\json\\omni_clickstream.json")
  val products = spark.read.json(path = s"D:\\json\\products.json")
  val users = spark.read.json(path = s"D:\\json\\users.json")
  //userEvents.show()
  //products.show()
  //users.show()

  userEvents
    .join(products, userEvents("url") === products("url"))
    .join(users, userEvents("swid") === concat(lit("{"), users("SWID"), lit("}")))
    .filter($"GENDER_CD" =!= "U")
    .groupBy("GENDER_CD", "category")
    .count()
    .orderBy(desc("count"))
    .show()


}
