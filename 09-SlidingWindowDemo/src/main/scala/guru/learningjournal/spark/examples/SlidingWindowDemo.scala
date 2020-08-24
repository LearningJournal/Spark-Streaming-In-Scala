package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object SlidingWindowDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Sliding Window Demo")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    val invoiceSchema = StructType(List(
      StructField("CreatedTime", StringType),
      StructField("Reading", DoubleType)
    ))

    val kafkaSourceDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sensor")
      .option("startingOffsets", "earliest")
      .load()

    val valueDF = kafkaSourceDF.select(col("key").cast("string").alias("SensorID"),
      from_json(col("value").cast("string"), invoiceSchema).alias("value"))

    val sensorDF = valueDF.select("SensorID", "value.*")
      .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))


    val aggDF = sensorDF
      .withWatermark("CreatedTime", "30 minute")
      .groupBy(col("SensorID"),
        window(col("CreatedTime"), "15 minute", "5 minute"))
      .agg(max("Reading").alias("MaxReading"))

    val outputDF = aggDF.select("SensorID", "window.start", "window.end", "MaxReading")
    //outputDF.show()

    val windowQuery = outputDF.writeStream
      .format("console")
      .outputMode("update")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("Counting Invoices")
    windowQuery.awaitTermination()

  }

}
