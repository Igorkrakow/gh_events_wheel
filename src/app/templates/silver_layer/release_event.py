from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import app.config as conf
def run():
    spark = SparkSession.builder.getOrCreate()
    source_table = f'{conf.SCHEMA_PATH}.github_events_bronze'
    checkpoint = f'{conf.CHECK_POINTS_PATH}release_event'
    table = f'{conf.SCHEMA_PATH}.gh_events_release_silver'
    spark.conf.set("spark.sql.shuffle.partitions", 32)
    df = (
        spark.readStream
        .table(source_table)
    )

    silver_release_event_df = (df
        .filter(F.col("type") == "ReleaseEvent")
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
        F.expr("payload:release.id").cast("bigint").alias("release_id"),
        F.expr("payload:release.tag_name").alias("tag_name"),
        F.expr("payload:release.name").alias("release_name"),
        F.expr("payload:release.target_commitish").alias("target_branch"),

        F.expr("payload:release.published_at").cast("timestamp").alias("published_at"),
        F.expr("payload:release.prerelease").cast("boolean").alias("is_prerelease")
    )
    )

    silver_release_event_clean_df = silver_release_event_df.dropDuplicates(["id"])

    query = (
        silver_release_event_clean_df.writeStream
            .trigger(availableNow=True)
            .option("checkpointLocation", checkpoint)
            .toTable(table)
    )