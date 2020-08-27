package guru.learningjournal.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, from_json, to_timestamp}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamTableJoinDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Stream Table Join Demo")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
      .config("spark.sql.catalog.lh", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .getOrCreate()

    val loginSchema = StructType(List(
      StructField("created_time", StringType),
      StructField("login_id", StringType)
    ))

    val kafkaSourceDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "logins")
      .option("startingOffsets", "earliest")
      .load()

    val valueDF = kafkaSourceDF.select(from_json(col("value").cast("string"), loginSchema).alias("value"))
    val loginDF = valueDF.select("value.*")
      .withColumn("created_time", to_timestamp(col("created_time"), "yyyy-MM-dd HH:mm:ss"))

    val userDF = spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "spark_db")
      .option("table", "users")
      .load()

    val joinExpr = loginDF.col("login_id") === userDF.col("login_id")
    val joinType = "inner"

    val joinedDF = loginDF.join(userDF, joinExpr, joinType)
      .drop(loginDF.col("login_id"))

    val outputDF = joinedDF.select(col("login_id"), col("user_name"),
      col("created_time").alias("last_login"))

    val outputQuery = outputDF.writeStream
      .foreachBatch(writeToCassandra _)
      .outputMode("update")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()


    logger.info("Waiting for Query")
    outputQuery.awaitTermination()

  }

  def writeToCassandra(outputDF: DataFrame, batchID: Long): Unit = {
    outputDF.write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "spark_db")
      .option("table", "users")
      .mode("append")
      .save()

    outputDF.show()
  }
}
