import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


object Airlines {

  def readDF(path: String, format: String, sqlContext: SQLContext) : DataFrame = {
    val df = sqlContext.read
      .format(format)
      .option("header", "true")
      .load(path)
    df
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").setAppName("OpenSkyReader")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val folders = List.tabulate(4)(i => s"D:\\Training\\Opensky\\data\\data_0${i}h__20_04_2019")

    var aircraftsDF = readDF("D:\\Python\\Work projects\\parsing_linkedin\\collect_data\\aircraftDatabase.csv",
      "csv", sqlContext)
    val aircraftsTypesDF = readDF("D:\\Python\\Work projects\\parsing_linkedin\\collect_data\\aircraftTypes.csv",
      "csv", sqlContext)

    val header = aircraftsDF.first()
    aircraftsDF = aircraftsDF.filter(row => row != header)

    println(aircraftsDF.show())
    println(aircraftsTypesDF.show())

    println(s"Count of aircraft DB: ${aircraftsDF.count()}")


  }
}
