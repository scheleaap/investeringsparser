package util

import org.apache.spark.sql._

object Spark {

  def getOrCreate: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Investeringsparser")
      .getOrCreate()

}
