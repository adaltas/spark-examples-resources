package com.ferdi.spark.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object taxiCount {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("yarn")
      .appName("Best Taxi Count 2018")
      .config("spark.sql.shuffle.partitions", "16")
      .config("spark.executor.memory", "1g")
      .config("spark.executor.instances", "4")
      .config("spark.executor.cores", "2")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val SchemaTaxi  = "VendorID INT,pickup_datetime TIMESTAMP,dropoff_datetime TIMESTAMP,store_and_fwd_flag STRING,RatecodeID INT,PULocationID INT,DOLocationID INT,passenger_count INT,trip_distance FLOAT,fare_amount FLOAT,extra FLOAT,mta_tax FLOAT,tip_amount FLOAT,tolls_amount FLOAT,ehail_fee FLOAT,improvement_surcharge FLOAT,total_amount FLOAT,payment_type INT,trip_type INT,congestion_surcharge FLOAT"

    val driver_df = spark.read
      .option("header", "true")
      .schema(SchemaTaxi)
      .csv(args(0))
      .select("VendorID")
      .repartition(16, $"VendorID")

    val littleDf = Seq(
      (1, "The First Company"),
      (2, "The second company"))
      .toDF("VendorID", "VendorName")

    val detailledDfBroad = driver_df.joinWith(broadcast(littleDf), driver_df("VendorID")===littleDf("VendorID")).toDF("VendorID", "VendorInfo")
    detailledDfBroad.groupBy($"VendorInfo.VendorName").count().sort($"count".desc).show()

  }

}