import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, to_timestamp}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object StreamTableJoinDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Stream Table Join Demo")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()

    val loginSchema = StructType(List(
      StructField("CreatedTime", StringType),
      StructField("LoginID", StringType)
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
      .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))

    val rawUserDF = spark.read
      .format("json")
      .option("path", "static-data/")
      .load()
      .cache()

    val userDF = rawUserDF
      .withColumn("LastLogin", to_timestamp(col("LastLogin"), "yyyy-MM-dd HH:mm:ss"))

    val joinExpr = loginDF.col("LoginID") === userDF.col("LoginID")
    val joinType = "inner"

    val joinedDF = loginDF.join(userDF, joinExpr, joinType)
      .drop(loginDF.col("LoginID"))

    val outputDF = joinedDF.select(col("LoginID"),
      col("UserName"),
      col("CreatedTime").alias("LastLogin"))

    val outputQuery = outputDF.writeStream
      .format("console")
      .outputMode("update")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("Waiting for Query")
    outputQuery.awaitTermination()

  }
}
