package com.example.spark_part2_practice

import org.apache.spark.sql._
import org.apache.spark.sql.functions.lpad
import org.apache.spark.sql.functions._

object BostonCrimesMap extends App{
  val spark = SparkSession
    .builder()
    .master(master = "local[*]")
    .getOrCreate()

  import spark.implicits._

  case class Crime (
                     INCIDENT_NUMBER: String,
                     OFFENSE_CODE: String,
                     OFFENSE_CODE_GROUP: String,
                     PFFENSE_DESCRIPTION: String,
                     DISTRICT: String,
                     REPORTING_AREA: String,
                     SHOOTING: String,
                     OCCURRED_ON_DATE: String,
                     YEAR: Int,
                     MONTH: Int,
                     DAY_OF_WEEK: String,
                     HOUR: Int,
                     UCR_PART: String,
                     STREET: String,
                     Lat: Double,
                     Long: Double,
                     Location: String
                    )

  case class Crime_code (
                     CODE: String,
                     NAME: String
                   )

  val path_c = args(0)
  val path_cc = args(1)
  val path_f = args(2)

  val crime_codes = spark
    .read
    .option("header", "true")
    .option("inferShema", "true")
    .csv(path_cc)
    .withColumn("CODE", lpad($"CODE", 5, "0"))
    .withColumn("NAME", split($"Name", "-")(0))
    .groupBy($"CODE")
    .agg(collect_list($"NAME")(0) as "name")

  val crimes = spark
    .read
    .option("header", "true")
    .option("inferShema", "true")
    .csv(path_c)
    .withColumn("OFFENSE_CODE", lpad($"OFFENSE_CODE", 5, "0"))

  case class test_class (
                          DISTRICT: String,
                          name: String,
                          count: BigInt
                        )

  val broadcast_crime_codes = broadcast(crime_codes)

  val test = crimes
    .join(broadcast_crime_codes, $"OFFENSE_CODE" === $"CODE")
    .groupBy($"DISTRICT", $"name")
    .count()
    .orderBy($"DISTRICT".desc, $"count".desc)
    .as[test_class]
    .groupByKey(_.DISTRICT)
    .flatMapGroups{
      case (district, iter) => iter.toList.sortBy(-_.count).take(3)
    }
    .groupBy($"DISTRICT")
    .agg(concat_ws(", ", collect_list($"name")) as "name_t",
      concat_ws(", ", collect_list($"count")) as "count_t")

  val avg_values = crimes
    .groupBy($"DISTRICT")
    .agg(avg($"Lat") as "Lat", avg($"Long") as "Long")
    .withColumnRenamed("DISTRICT",  "DISTRICT_avg")

  val stats = crimes
    .groupBy($"DISTRICT", $"MONTH")
    .count()
    .groupBy($"DISTRICT")
    .agg(sum("count")  as "crimes_total",
      callUDF("percentile_approx", col("count"), lit(0.5)) as "crimes_monthly")
    .withColumnRenamed("DISTRICT",  "DISTRICT_stat")

  val broadcast_stats = broadcast(stats)
  val broadcast_avg_values = broadcast(avg_values)

  val result = test
    .join(broadcast_stats, test("DISTRICT") === broadcast_stats("DISTRICT_stat"))
    .join(broadcast_avg_values, test("DISTRICT") === broadcast_avg_values("DISTRICT_avg"))
    .toDF()
    .drop("count_t", "DISTRICT_stat", "DISTRICT_avg")
    .coalesce(1)
    .write
    .option("header", "true")
    .parquet(path_f)

}
