package guru.learningjournal.spark.examples

import java.nio.file.{Files, Paths}

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.{col, expr, round, struct, sum, to_json}

object KafkaAvroSourceDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Kafka Avro Sink Demo")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

    val kafkaSourceDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "invoice-items")
      .option("startingOffsets", "earliest")
      .load()

    val avroSchema = new String(Files.readAllBytes(Paths.get("schema/invoice-items")))
    val valueDF = kafkaSourceDF.select(from_avro(col("value"), avroSchema).alias("value"))
    val rewardsDF = valueDF.filter("value.CustomerType == 'PRIME'")
      .groupBy("value.CustomerCardNo")
      .agg(sum("value.TotalValue").alias("TotalPurchase"),
        sum(expr("value.TotalValue * 0.2").cast("integer")).alias("AggregatedRewards"))

    val kafkaTargetDF = rewardsDF.select(expr("CustomerCardNo as key"),
      to_json(struct("TotalPurchase", "AggregatedRewards")).alias("value"))

    //kafkaTargetDF.show(false)

    val rewardsWriterQuery = kafkaTargetDF
      .writeStream
      .queryName("Rewards Writer")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "customer-rewards")
      .outputMode("update")
      .option("checkpointLocation", "chk-point-dir")
      .start()

    rewardsWriterQuery.awaitTermination()

  }
}
