"""Kafka -> Bronze. Raw JSON stored untouched (contract: lakehouse/schemas/bronze_*.md).
Usage: bronze_ingest.py <clics_vle|evaluations>
"""
import sys
from pyspark.sql import functions as F
from common import get_spark, BRONZE

topic = sys.argv[1]
spark = get_spark(f"bronze_{topic}")

stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")  # internal Docker address
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .load()
)

bronze = stream.select(
    F.col("value").cast("string").alias("raw_value"),
    F.col("topic").alias("kafka_topic"),
    F.col("partition").alias("kafka_partition"),
    F.col("offset").alias("kafka_offset"),
    F.col("timestamp").alias("kafka_timestamp"),
    F.current_timestamp().alias("ingested_at"),
)

query = (
    bronze.writeStream.format("parquet")
    .option("path", f"{BRONZE}/bronze_{topic}")
    .option("checkpointLocation", f"{BRONZE}/_checkpoints/bronze_{topic}")
    .trigger(availableNow=True)  # process everything available, then stop
    .start()
)
query.awaitTermination()

print("BRONZE rows:", spark.read.parquet(f"{BRONZE}/bronze_{topic}").count())
