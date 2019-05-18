import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


object Airlines {

  def readDF(path: String, format: String, sqlContext: SQLContext) : DataFrame = {
    val df = sqlContext.read
      .format(format)
      .option("header", "true")
      .load(path)
    df
  }

  def getDfSummary(df: DataFrame) = {
    val aircraftSummary = df.describe()
    println(aircraftSummary.show())
  }

  def simplifyAircraftDF(df: DataFrame) : DataFrame = {
    val header = df.first()
    var f_df = df.filter(row => row != header)
    val aircraftColToRemove = Seq("status", "seatconfiguration", "notes")

    f_df = f_df.select(f_df.columns.filter(c => !aircraftColToRemove.contains(c)).map(c => new Column(c)): _*)
    f_df
  }

  def joinDFsBroadcast(airlines:DataFrame, aircraft:DataFrame, aircraftType: DataFrame, sc: SparkContext) : DataFrame = {
    val dfAsAirlines = airlines.as("airlines")
    val dfAsAircraft = aircraft.as("aircraft")
    val dfAsAircraftType = aircraftType.as("aircraftType")

    val joinType = "left_outer"

    val joinedAircraftType = dfAsAircraft.join(dfAsAircraftType,
      col("aircraft.typecode") === col("aircraftType.Designator"),
      joinType)

    val dfAsJoinedAircraft = joinedAircraftType.as("joinedAircraft")
    println("Joined aircrafts with aircraft types")
    println(joinedAircraftType.show())
    val broadcastJoinedAircraft = sc.broadcast(joinedAircraftType)

    println(s"Count of partitions in dfAsJoinedAircraft: ${dfAsJoinedAircraft.rdd.getNumPartitions}")
    println(s"Count of partitions in dfAsAirlines: ${dfAsAirlines.rdd.getNumPartitions}")

    val joinedAirlinesAircraft = dfAsAirlines.join(broadcastJoinedAircraft.value,
      broadcastJoinedAircraft.value("icao24") === col("airlines.icao24"),
      joinType)
      .drop(col("joinedAircraft.icao24"))

    println(joinedAirlinesAircraft.explain())
    println("Joined stream airplanes with joined aircraft with aircraft types")
    println(joinedAirlinesAircraft.show())

    println(s"Count of partitions in joined df: ${joinedAirlinesAircraft.rdd.getNumPartitions}")
    val outputDf = joinedAirlinesAircraft.coalesce(5)
    println(s"Count of partitions after coalesce: ${outputDf.rdd.getNumPartitions}")

    outputDf
  }

  def joinDFs(airlines:DataFrame, aircraft:DataFrame, aircraftType: DataFrame) : DataFrame = {
    val dfAsAirlines = airlines.as("airlines")
    val dfAsAircraft = aircraft.as("aircraft")
    val dfAsAircraftType = aircraftType.as("aircraftType")

    val joinType = "left_outer"

    val joinedAircraftType = dfAsAircraft.join(dfAsAircraftType,
      col("aircraft.typecode") === col("aircraftType.Designator"),
      joinType)

    val dfAsJoinedAircraft = joinedAircraftType.as("joinedAircraft")
    println("Joined aircrafts with aircraft types")
    println(joinedAircraftType.show())

    println(s"Count of partitions in dfAsJoinedAircraft: ${dfAsJoinedAircraft.rdd.getNumPartitions}")
    println(s"Count of partitions in dfAsAirlines: ${dfAsAirlines.rdd.getNumPartitions}")

    val joinedAirlinesAircraft = dfAsAirlines.join(dfAsJoinedAircraft,
      col("joinedAircraft.icao24") === col("airlines.icao24"),
      joinType)
      .drop(col("joinedAircraft.icao24"))

    println(joinedAirlinesAircraft.explain())
    println("Joined stream airplanes with joined aircraft with aircraft types")
    println(joinedAirlinesAircraft.show())

    println(s"Count of partitions in joined df: ${joinedAirlinesAircraft.rdd.getNumPartitions}")
    val outputDf = joinedAirlinesAircraft.coalesce(5)
    println(s"Count of partitions after coalesce: ${outputDf.rdd.getNumPartitions}")

    outputDf
  }

  /**
    * Task A
    * Get number of partitions for 1-hour DataFrame
    */
  def taskA(airlines: DataFrame) = {
    println("Result of task A:")
    println(s"Count of partitions${airlines.rdd.getNumPartitions}")
  }

  /**
    * Task B
    * Calculate average latitude and minimum longitude for each origin _country
    */
  def taskB(airlines: DataFrame)= {
    println("Result of task B:")
    println(airlines
      .groupBy("origin_country")
      .agg(round(avg("latitude"), 3).alias("Average latitude"),
           round(mean("longitude"), 3).alias("Average longitude"))
      .show())
  }

  /**
    * Task C
    * Get the max speed ever seen for the last 4 hours
    */
  def taskC(airlines: DataFrame)= {
    println("Result of task C:")
    println(airlines
          .filter("velocity > 200")
          .agg(max("velocity"))
          .head())
  }

  /**
    * Task D
    * Get top 10 airplanes with max average speed for the last 4 hours (round the result)
    */
  def taskD(airlines: DataFrame)= {
    println("Result of task D:")
    println(airlines
      .filter("velocity is not null")
      .groupBy("icao24")
      .agg(round(mean("velocity")).alias("velocity"))
      .orderBy(desc_nulls_last("velocity"))
      .show())
  }

  /**
    * Task E
    * Show distinct airplanes where origin_country = ‘Germany’ and it was on ground
    * at least one time during last 4 hours.
    */
  def taskE(airlines: DataFrame)= {
    println("Result of task E:")
    val df = airlines
      .filter("origin_country = 'Germany'")
      .groupBy("icao24")
      .agg(min("on_ground").alias("on_ground"))
      .filter("on_ground = True")

    println(s"Count: ${df.count()}")
    println(df.show())
  }

  /**
    * Task F
    * Show top 10 origin_country with the highest number of
    * unique airplanes in air for the last day
    */
  def taskF(airlines: DataFrame)= {
    println("Result of task F:")
    val dfOnAir = airlines.filter("on_ground = False")
    val df = dfOnAir.select("icao24", "origin_country").distinct().groupBy("origin_country").count().sort(desc("count"))
    println(df.show())
  }

  /**
    * Task G
    * Show top 10 longest (by time) completed flights for the last day
    */
  def taskG(airlines: DataFrame) = {
    println("Result of task G:")
    val w = Window.partitionBy("icao24").orderBy(desc("time_position"))
    val toMinutesUDF = udf[Option[Double], Double](secondToMinutesUDF)
    val flightDf = airlines
      .withColumn("prev__on_ground", lag("on_ground", 1, "False")
        .over(w))
      .filter(col("on_ground") =!= col("prev__on_ground"))
      .groupBy(col("icao24"))
      .agg((first("time_position") - last("time_position")).alias("flight_time"), first("origin_country").alias("origin_country"))
      .filter(col("flight_time").isNotNull)
      .orderBy(desc("flight_time"))
      .select(col("icao24"), toMinutesUDF(col("flight_time")).as("flight_time, min"), col("origin_country"))
      .limit(20)
    println(flightDf.show(20))
  }

  /**
    * Task H
    * Get the average geo_altitude value for each origin_country
    * (round the result to 3 decimal places and rename column)
    */
  def taskH(airlines: DataFrame)= {
    println(airlines
      .groupBy("origin_country")
      .agg(round(avg("geo_altitude"), 3).alias("Average altitude"))
      .show())
  }

  def roundAvoid(value: Double, places: Int): Double = {
    val scale = Math.pow(10, places)
    (value * scale).round / scale
  }

  def secondToMinutesUDF(sec: Double): Option[Double] = {
    val sec_val = Option(sec).getOrElse(return None)
    Some(roundAvoid(sec_val  / 60, 2))
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").setAppName("OpenSkyReader")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val folders = List.range(20, 23).map(i => s"F:\\Python\\Scala\\data\\data_${i}h__25_04_2019")

    val hoursAirlinesDF = sqlContext.read
      .format("csv")
      .option("header", "true")
      .csv(folders: _*)
    println(hoursAirlinesDF.show())
    println(s"Count of airlines: ${hoursAirlinesDF.count()}")

    val smpHoursAirlinesDF = hoursAirlinesDF.sample(withReplacement = true,0.1, seed = 1)
    

//    taskF(smpHoursAirlinesDF)
//    taskG(smpHoursAirlinesDF)

    /** Joining DataFrames **/

//    var aircraftDF = readDF("D:\\Python\\Work projects\\parsing_linkedin\\collect_data\\aircraftDatabase.csv",
//      "csv", sqlContext)
//    val aircraftTypesDF = readDF("D:\\Python\\Work projects\\parsing_linkedin\\collect_data\\aircraftTypes.csv",
//      "csv", sqlContext)
//
//    aircraftDF = simplifyAircraftDF(aircraftDF)
//
//    println(aircraftDF.show())
//    println(aircraftTypesDF.show())
//    println(s"Count of aircraft DB: ${aircraftDF.count()}")
//
//    val joinDF = joinDFs(smpHoursAirlinesDF, aircraftDF, aircraftTypesDF)
//
//    joinDFsBroadcast(smpHoursAirlinesDF, aircraftDF, aircraftTypesDF, sc)
//
//    joinDF.write.format("csv").mode("overwrite").option("sep", "\t")
//      .save("/tmp/output_files.csv")
//
//    joinDF.write.format("parquet").mode("overwrite")
//      .save("/tmp/output_files.parquet")

    scala.io.StdIn.readLine()
  }
}
