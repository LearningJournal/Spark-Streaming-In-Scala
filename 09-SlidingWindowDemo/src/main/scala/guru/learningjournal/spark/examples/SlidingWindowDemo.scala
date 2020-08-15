package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, to_timestamp, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object SlidingWindowDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Tumbling Window Demo")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

    val invoiceSchema = StructType(List(
      StructField("InvoiceNumber", StringType),
      StructField("CreatedTime", StringType),
      StructField("StoreID", StringType),
      StructField("TotalAmount", DoubleType)
    ))

    val kafkaSourceDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "invoices")
      .option("startingOffsets", "earliest")
      .load()

    val valueDF = kafkaSourceDF.select(from_json(col("value").cast("string"), invoiceSchema).alias("value"))

    //valueDF.printSchema()
    //valueDF.show(false)

    val invoiceDF = valueDF.select("value.*")
      .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))

    val countDF = invoiceDF.groupBy(col("StoreID"),
      window(col("CreatedTime"), "5 minute", "1 minute"))
      .count()

    //countDF.printSchema()
    //countDF.show(false)

    val outputDF = countDF.select("StoreID", "window.start", "window.end", "count")

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
