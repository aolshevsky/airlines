import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
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

  def joinDFs(airlines:DataFrame, aircraft:DataFrame, aircraftType: DataFrame) : DataFrame = {
    val dfAsAirlines = airlines.as("airlines")
    val dfAsAircraft = aircraft.as("aircraft")
    val dfAsAircraftType = aircraftType.as("aircraftType")

    val joinType = "left_semi"

    val joinedAircraftType = dfAsAircraft.join(dfAsAircraftType,
      col("aircraft.typecode") === col("aircraftType.Designator"),
      joinType)

    val dfAsJoinedAircraft = joinedAircraftType.as("joinedAircraft")
    println("Joined aircrafts with aircraft types")
    println(joinedAircraftType.show())

    println(s"Count of partitions in df_asJoinedAircrafts: ${dfAsJoinedAircraft.rdd.getNumPartitions}")
    println(s"Count of partitions in df_asHoursAirlines: ${dfAsJoinedAircraft.rdd.getNumPartitions}")

    val joinedAirlinesAircraft = dfAsAirlines.join(dfAsJoinedAircraft,
      col("joinedAircraft.icao24") === col("airlines.icao24"),
      "left_outer")
      .drop(col("joinedAircraft.icao24"))

    println(joinedAirlinesAircraft.explain())
    println("Joined stream airplanes with joined aircraft with aircraft types")
    println(joinedAirlinesAircraft.orderBy("time_position").show())

    println(s"Count of partitions in joined df: ${joinedAirlinesAircraft.rdd.getNumPartitions}")
    val outputDf = joinedAirlinesAircraft.coalesce(5)
    println(s"Count of partitions after coalesce: ${outputDf.rdd.getNumPartitions}")

    outputDf
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").setAppName("OpenSkyReader")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val folders = List.tabulate(4)(i => s"D:\\Training\\Opensky\\data\\data_0${i}h__20_04_2019")

    var aircraftDF = readDF("D:\\Python\\Work projects\\parsing_linkedin\\collect_data\\aircraftDatabase.csv",
      "csv", sqlContext)
    val aircraftTypesDF = readDF("D:\\Python\\Work projects\\parsing_linkedin\\collect_data\\aircraftTypes.csv",
      "csv", sqlContext)

    aircraftDF = simplifyAircraftDF(aircraftDF)

    println(aircraftDF.show())
    println(aircraftTypesDF.show())
    println(s"Count of aircraft DB: ${aircraftDF.count()}")

    val hoursAirlinesDF = sqlContext.read
      .format("csv")
      .option("header", "true")
      .csv(folders: _*)
    println(hoursAirlinesDF.show())
    println(s"Count of airlines: ${hoursAirlinesDF.count()}")

    val smpHoursAirlinesDF = hoursAirlinesDF.sample(withReplacement = true,0.1, seed = 1)

    joinDFs(smpHoursAirlinesDF, aircraftDF, aircraftTypesDF)
  }
}
