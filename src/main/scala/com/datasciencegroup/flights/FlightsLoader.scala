package com.datasciencegroup.flights


import org.apache.spark.sql.{SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.functions._


/**
 * Created by Christian Straight on 9/17/2017.
 */


object FlightsLoader {

  def main(args: Array[String]): Unit = {

    val flightsCsv = "file:///home/cls/dse/data/flight/flights_from_pg.csv"
    val keyspace_dstx = "datastx"
    val conf = new SparkConf().setMaster("local[4]").setAppName("SparkFlight")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    println("****************************")
    println("FlightsLoader running")
    println("****************************")

    // 1. Write a program to load the source data from the flights_from_pg.csv file into the flights table.

    val flightsRdd = sc.textFile(flightsCsv).map(line => line.split(",").map(_.trim)).
      map(row => Flight(row(0).toInt, row(1).toInt, row(2).toInt, cleanDate(row(3)), row(4).toInt,
        row(5), row(6).toInt, row(7).toInt, row(8), row(9), row(10),
        row(11), row(12), row(13), mungeDate(row(3), row(14)),
        mungeDate(row(3), row(15)), row(16).toInt,
        row(17).toInt, row(18).toInt))


    val flights_Df = flightsRdd.toDF()

    flights_Df.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "flights", "keyspace" -> keyspace_dstx,
      "spark.cassandra.output.consistency.level" -> "LOCAL_ONE")).save()

    val flights = spark.sql("SELECT * FROM datastx.flights").cache()

    println()
    println(flights.count + " loaded into " + keyspace_dstx + ".flights")
    println()

    // 2. Create and populate a Cassandra table designed to list all flights leaving a particular airport,
    // sorted by time.

    val flightsByOriginDf = flights.toDF() // create key of origin, dep_time, carrier

    flightsByOriginDf.write.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "flights_by_origin", "keyspace" -> keyspace_dstx,
        "spark.cassandra.output.consistency.level" -> "LOCAL_ONE")).save()

    println()
    println(flightsByOriginDf.count() + " loaded into " + keyspace_dstx + ".flights_by_origin")
    println()

    // 3. Create and populate a Cassandra table designed to provide the carrier, origin, and destination airport for
    // a flight based on 10 minute buckets of air_time.

    val flightsByTimeBinDf = flights.withColumn("timebin", binTime($"air_time_mins"))

    flightsByTimeBinDf.write.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "flights_by_timebin", "keyspace" -> keyspace_dstx,
        "spark.cassandra.output.consistency.level" -> "LOCAL_ONE")).save()

    println()
    println(flightsByOriginDf.count() + " loaded into " + keyspace_dstx + ".flights_by_timebin")
    println()
    println("stopping.....")
    println()

    sc.stop()

  }


  val binTime = udf { (i: Int) => (Math.ceil(i / 10.0).toInt * 10) }

  def cleanDate(date: String): java.sql.Date = {
    java.sql.Date.valueOf(date.replace("/", "-"))
  }

  def mungeDate(fl_date: String, hhmm: String): java.sql.Timestamp = {

    val cleanDate = fl_date.replace("/", "-")
    val hhmmLength = hhmm.length()

    if (hhmmLength == 3) {
      java.sql.Timestamp.valueOf(cleanDate + " " + "0" + hhmm.slice(0, 1) + ":"
        + hhmm.slice(1, 3) + ":00.000000000")
    }
    else if (hhmmLength == 4) {
      java.sql.Timestamp.valueOf(cleanDate + " " + hhmm.slice(0, 2) + ":"
        + hhmm.slice(2, 4) + ":00.000000000")
    }
    else {
      java.sql.Timestamp.valueOf("1970-01-01 00:00:00.000000000") // badly formed input string or doesn't exist
    }
  }

  case class Flight(id: Int, // 0
                    year: Int, // 1
                    day_of_month: Int, // 2
                    fl_date: java.sql.Date, // 3
                    airline_id: Int, // 4
                    carrier: String, // 5
                    fl_num: Int, // 6
                    origin_airport_id: Int, // 7
                    origin: String, // 8
                    origin_city_name: String, // 9
                    origin_state_abr: String, // 10
                    dest: String, // 11
                    dest_city_name: String, // 12
                    dest_state_abr: String, // 13
                    dep_time: java.sql.Timestamp, // 14
                    arr_time: java.sql.Timestamp, // 15
                    actual_elapsed_time_mins: Int, // 16
                    air_time_mins: Int, // 17
                    distance: Int // 18

                     )

  /*

  // 1. Write a program to load the source data from the flights_from_pg.csv file into the flights table.

  CREATE TABLE datastx.flights (
          id INT,
          year INT,
          day_of_month INT,
          fl_date DATE,
          airline_id INT,
          carrier VARCHAR,
          fl_num INT,
          Origin_airport_id INT,
          origin VARCHAR,
          origin_city_name VARCHAR,
          origin_state_abr VARCHAR,
          dest VARCHAR,
          dest_city_name VARCHAR,
          dest_state_abr VARCHAR,
          dep_time TIMESTAMP,
          arr_time TIMESTAMP,
          actual_elapsed_time_mins INT,
          air_time_mins INT,
          distance INT,
          PRIMARY KEY ((id),carrier));

  // 2. Create and populate a Cassandra table designed to list all flights leaving a particular airport, sorted by time.

       -- TRUNCATE TABLE datastx.flights_by_origin;
       -- DROP TABLE datastx.flights_by_origin;
       CREATE TABLE IF NOT EXISTS datastx.flights_by_origin(
          id INT,
          year INT,
          day_of_month INT,
          fl_date DATE,
          airline_id INT,
          carrier VARCHAR,
          fl_num INT,
          Origin_airport_id INT,
          origin VARCHAR,
          origin_city_name VARCHAR,
          origin_state_abr VARCHAR,
          dest VARCHAR,
          dest_city_name VARCHAR,
          dest_state_abr VARCHAR,
          dep_time TIMESTAMP,
          arr_time TIMESTAMP,
          actual_elapsed_time_mins INT,
          air_time_mins INT,
          distance INT,
          PRIMARY KEY ((id),origin, dep_time)
          );

  // 3. Create and populate a Cassandra table designed to provide the carrier, origin, and destination airport for a
  //    flight based on 10 minute buckets of air_time.

       -- TRUNCATE TABLE datastx.flights_by_timebin;
       -- DROP TABLE datastx.flights_by_timebin;
       CREATE TABLE IF NOT EXISTS datastx.flights_by_timebin(
          timebin INT,
          id INT,
          year INT,
          day_of_month INT,
          fl_date DATE,
          airline_id INT,
          carrier VARCHAR,
          fl_num INT,
          Origin_airport_id INT,
          origin VARCHAR,
          origin_city_name VARCHAR,
          origin_state_abr VARCHAR,
          dest VARCHAR,
          dest_city_name VARCHAR,
          dest_state_abr VARCHAR,
          dep_time TIMESTAMP,
          arr_time TIMESTAMP,
          actual_elapsed_time_mins INT,
          air_time_mins INT,
          distance INT,
          PRIMARY KEY ((id),timebin, carrier, origin, dest)
          );



   */

}
