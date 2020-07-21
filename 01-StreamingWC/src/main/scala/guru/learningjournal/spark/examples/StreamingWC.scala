package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamingWC extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Streaming Word Count")
      .getOrCreate()

    val linesDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    linesDF.printSchema()

    //val wordsDF = linesDF.select(explode(split(col("value"), " ")).as("word"))
    val wordsDF = linesDF.select(expr("explode(split(value,' ')) as word"))
    val countsDF = wordsDF.groupBy("word").count()

    val wordCountQuery = countsDF.writeStream
      .format("console")
      .outputMode("complete")
      .option("checkpointLocation", "chk-point-dir")
      .start()

    wordCountQuery.awaitTermination()

  }

}
