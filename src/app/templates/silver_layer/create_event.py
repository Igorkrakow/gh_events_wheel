from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import app.config as conf
def run():
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 32)
    source_table = f'{conf.SCHEMA_PATH}.github_events_bronze'
    checkpoint = f'{conf.CHECK_POINTS_PATH}create_event'
    table = f'{conf.SCHEMA_PATH}.gh_events_create_silver'
    spark.conf.set("spark.sql.shuffle.partitions", 32)
    df = (
        spark.readStream
        .table(source_table)
    )
    silver_create_event_df = (df
        .filter(F.col("type") == "CreateEvent")
        .select(
        F.col("id").alias("id"),
        F.col("ts_created").alias("event_time"),
        F.expr("actor:id").cast("bigint").alias("user_id"),
        F.expr("actor:login").alias("user_login"),
        F.expr("org:id").cast("bigint").alias("org_id"),
        F.expr("org:login").alias("org_name"),
        F.expr("repo:id").cast("bigint").alias("repo_id"),
        F.expr("repo:name").alias("repo_name"),

        F.expr("payload:ref").cast("string").alias("ref_name"),
        F.expr("payload:ref_type").cast("string").alias("ref_type"),
        F.expr("payload:master_branch").cast("string").alias("base_branch"),
        F.expr("payload:pusher_type").cast("string").alias("pusher_type")
    )
    )

    silver_create_event_clean_df = silver_create_event_df.dropDuplicates(["id"])

    query = (
        silver_create_event_clean_df.writeStream
            .trigger(availableNow=True)
            .option("checkpointLocation", checkpoint)
            .toTable(table)
    )