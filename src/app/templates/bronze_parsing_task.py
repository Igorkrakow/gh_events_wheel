from pyspark.sql.functions import input_file_name, current_timestamp, col, get_json_object, parse_json
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import app.config as conf

def run():
    spark = SparkSession.builder.getOrCreate()
    raw_path = conf.BASE_PATH
    checkpoint = f"{conf.CHECK_POINTS_PATH}gh_bronze"
    table_name = f"{conf.SCHEMA_PATH}.github_events_bronze"

    def process_micro_batch(micro_batch_df, batch_id):
        if bool(micro_batch_df.head(1)):
            micro_batch_df.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(table_name)
            print(f"[Batch {batch_id}] Successfully committed to Delta log.")

    schema = StructType([
        StructField("id", LongType(), True),
        StructField("type", StringType(), True),
        StructField("actor", VariantType(), True),
        StructField("repo", VariantType(), True),
        StructField("payload", VariantType(), True),
        StructField("org", VariantType(), True),
        StructField("ts_created", TimestampType(), True),
        StructField("ingested_at", TimestampType(), True),
        StructField("source_file", StringType(), True)
    ])

    df_raw_stream = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "text")
            .load(raw_path)
    )

    df_raw_parsed = (
        df_raw_stream.select(
            get_json_object(col("value"), "$.id").cast('bigint').alias("id"),
            get_json_object(col("value"), "$.type").alias("type"),
            parse_json(get_json_object(col("value"), "$.actor")).alias("actor"),
            parse_json(get_json_object(col("value"), "$.repo")).alias("repo"),
            parse_json(get_json_object(col("value"), "$.payload")).alias("payload"),
            parse_json(get_json_object(col("value"), "$.org")).alias("org"),
            get_json_object(col("value"), "$.created_at").cast('timestamp').alias("ts_created")
        )
            .withColumn('ingested_at', current_timestamp())
            .withColumn('source_file', input_file_name())
    )

    q = (df_raw_parsed.writeStream
         .foreachBatch(process_micro_batch)
         .option("checkpointLocation", checkpoint)
         .trigger(availableNow=True)
         .start()
         )