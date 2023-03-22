package airbus.com

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

trait ReaderAndParserExample {

  def extractDataFrameFromFile(sparkSession: SparkSession, filePath: String): DataFrame = {

    //TODO: Replace rootId for your XML root identifier	
    sparkSession.read
      .format("xml")
      .option("rowTag", "rootId")
      .load(filePath)
  }

  def flattenFileToCreateList(df: DataFrame): DataFrame = {
    //TODO: Adapt this code to your specific XML file structure
    df
      .withColumn("COLUMN1", explode_outer(col("column1")))
      .withColumn("COLUMN2", explode(col("column2")))
      .select(
        col("COLUMN1._field1").alias("field1"),
        col("COLUMN2._field2").alias("field2"),
      ).filter("field1 != 'X'")
  }
}