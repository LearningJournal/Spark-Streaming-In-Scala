package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object TumblingWindowDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Tumbling Window Demo")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()

    val stockSchema = StructType(List(
      StructField("CreatedTime", StringType),
      StructField("Type", StringType),
      StructField("Amount", IntegerType),
      StructField("BrokerCode", StringType)
    ))

    val kafkaSourceDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "trades")
      .option("startingOffsets", "earliest")
      .load()

    val valueDF = kafkaSourceDF.select(from_json(col("value").cast("string"), stockSchema).alias("value"))

    val tradeDF = valueDF.select("value.*")
      .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("Buy", expr("case when Type == 'BUY' then Amount else 0 end"))
      .withColumn("Sell", expr("case when Type == 'SELL' then Amount else 0 end"))

    val windowAggDF = tradeDF
      .groupBy( // col("BrokerCode"),
        window(col("CreatedTime"), "15 minute"))
      .agg(sum("Buy").alias("TotalBuy"),
        sum("Sell").alias("TotalSell"))


    val outputDF = windowAggDF.select("window.start", "window.end", "TotalBuy", "TotalSell")

    /*
    val runningTotalWindow = Window.orderBy("end")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val finalOutputDF = outputDF
      .withColumn("RTotalBuy", sum("TotalBuy").over(runningTotalWindow))
      .withColumn("RTotalSell", sum("TotalSell").over(runningTotalWindow))
      .withColumn("NetValue", expr("RTotalBuy - RTotalSell"))

    finalOutputDF.show(false)
    */

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
