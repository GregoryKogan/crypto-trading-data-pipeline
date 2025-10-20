import os
import time
import logging
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, window, sum, first, last, max, min
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
    DecimalType,
)

KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_trades")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_data")
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/opt/spark/checkpoint")
POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main() -> None:
    spark = (
        SparkSession.builder.appName("CryptoAnalytics")
        .master(SPARK_MASTER_URL)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logging.info("Spark Session created successfully.")

    schema = StructType(
        [
            StructField("trade_id", LongType(), True),
            StructField("symbol", StringType(), True),
            StructField("price", StringType(), True),
            StructField("quantity", StringType(), True),
            StructField("trade_time", LongType(), True),
            StructField("is_buyer_maker", BooleanType(), True),
        ]
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER_URL)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    logging.info("Kafka stream reader configured.")

    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING)")
        .withColumn("data", from_json(col("value"), schema))
        .select("data.*")
    )

    transformed_df = (
        parsed_df.withColumn("price", col("price").cast(DecimalType(20, 8)))
        .withColumn("quantity", col("quantity").cast(DecimalType(20, 8)))
        .withColumn("trade_time", (col("trade_time") / 1000).cast("timestamp"))
        .withColumn("trade_value", col("price") * col("quantity"))
    )

    aggregated_df = (
        transformed_df.withWatermark("trade_time", "2 minutes")
        .groupBy(window(col("trade_time"), "1 minute"), col("symbol"))
        .agg(
            sum("quantity").alias("total_volume"),
            sum("trade_value").alias("total_trade_value"),
            first("price").alias("open_price"),
            last("price").alias("close_price"),
            max("price").alias("high_price"),
            min("price").alias("low_price"),
        )
        .withColumn("vwap", col("total_trade_value") / col("total_volume"))
    )

    output_df = aggregated_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("symbol"),
        col("open_price"),
        col("high_price"),
        col("low_price"),
        col("close_price"),
        col("total_volume"),
        col("vwap"),
    )

    query = (
        output_df.writeStream.outputMode("update")
        .foreachBatch(write_to_postgres)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="60 seconds")
        .start()
    )

    logging.info("Streaming query started. Awaiting termination...")
    query.awaitTermination()
    logging.info("Streaming query terminated successfully.")


def write_to_postgres(df: DataFrame, epoch_id: int) -> None:
    if df.isEmpty():
        logging.info(f"Batch {epoch_id}: No data to write.")
        return

    logging.info(f"Writing batch {epoch_id} of {df.count()} rows to Postgres.")

    try:
        staging_table = f"temp_batch_{epoch_id}_{int(time.time())}"

        (
            df.write.format("jdbc")
            .option("url", POSTGRES_JDBC_URL)
            .option("dbtable", staging_table)
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )

        with psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        ) as conn:
            with conn.cursor() as cur:
                upsert_sql = f"""
                INSERT INTO trades_1min_agg (
                    window_start, window_end, symbol, open_price, high_price, 
                    low_price, close_price, total_volume, vwap
                )
                SELECT 
                    window_start, window_end, symbol, open_price, high_price, 
                    low_price, close_price, total_volume, vwap
                FROM {staging_table}
                ON CONFLICT (symbol, window_start) DO UPDATE SET
                    window_end = EXCLUDED.window_end,
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    total_volume = EXCLUDED.total_volume,
                    vwap = EXCLUDED.vwap;

                DROP TABLE {staging_table};
                """
                cur.execute(upsert_sql)
                conn.commit()

        logging.info(f"Batch {epoch_id} written to Postgres successfully.")

    except Exception as e:
        logging.error(f"Error writing batch {epoch_id} to PostgreSQL: {e}")
        raise


if __name__ == "__main__":
    main()
