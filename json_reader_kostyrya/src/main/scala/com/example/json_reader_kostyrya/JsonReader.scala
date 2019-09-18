package com.example.json_reader_kostyrya

import org.apache.spark.sql.SparkSession

import org.json4s._
import org.json4s.jackson.JsonMethods._



object JsonReader extends App {
    val spark: SparkSession = SparkSession.builder()
      .appName(name = "json_reader_kostyrya")
      .master(master = "local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val path_ = args(0)
    val json = sc.textFile(path_)

    implicit val formats = DefaultFormats

    case class elem(id:Option[Int], country:Option[String], points:Option[Int], price:Option[Float],
                    title:Option[String], variety:Option[String], winery:Option[String])

    json.foreach(x => println(parse(x).extract[elem]))
}
