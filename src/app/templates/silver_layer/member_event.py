from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import app.config as conf
def run():
    spark = SparkSession.builder.getOrCreate()
    source_table = f'{conf.SCHEMA_PATH}.github_events_bronze'
    checkpoint = f'{conf.CHECK_POINTS_PATH}member_event'
    table =f'{conf.SCHEMA_PATH}.gh_events_member_silver'
    spark.conf.set("spark.sql.shuffle.partitions", 32)
    df = (
        spark.readStream
        .table(source_table)
    )

    silver_member_event_df = (df
        .filter(F.col("type") == "MemberEvent")
        .select(
        F.col("id").alias("id"),
        F.col("ts_created").alias("event_time"),
        F.expr("actor:id").cast("bigint").alias("user_id"),
        F.expr("actor:login").alias("user_login"),
        F.expr("org:id").cast("bigint").alias("org_id"),
        F.expr("org:login").alias("org_name"),
        F.expr("repo:id").cast("bigint").alias("repo_id"),
        F.expr("repo:name").alias("repo_name"),

        F.expr("payload:action").alias("action"),
        F.expr("payload:member.id").cast("bigint").alias("member_id"),
        F.expr("payload:member.login").alias("member_login"),
        F.expr("payload:member.type").alias("member_type")
    )
    )

    silver_member_event_clean_df = silver_member_event_df.dropDuplicates(["id"])

    query = (
        silver_member_event_clean_df.writeStream
            .trigger(availableNow=True)
            .option("checkpointLocation", checkpoint)
            .toTable(table)
    )