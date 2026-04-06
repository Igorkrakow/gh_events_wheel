from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import app.config as conf
def run():
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 32)
    source_table = f'{conf.SCHEMA_PATH}.github_events_bronze'
    checkpoint = f'{conf.CHECK_POINTS_PATH}pull_request_event'
    table =f'{conf.SCHEMA_PATH}.gh_events_pr_silver'
    spark.conf.set("spark.sql.shuffle.partitions", 32)
    df = (
        spark.readStream
        .table(source_table)
    )

    silver_pr_event_df = (df
        .filter(F.col("type") == "PullRequestEvent")
        .select(
        F.col("id").alias("id"),
        F.col("ts_created").alias("event_time"),
        F.expr("actor:id").cast("bigint").alias("user_id"),
        F.expr("actor:login").alias("user_login"),
        F.expr("org:id").cast("bigint").alias("org_id"),
        F.expr("org:login").alias("org_name"),
        F.expr("repo:id").cast("bigint").alias("repo_id"),
        F.expr("repo:name").alias("repo_name"),

        F.expr("payload:action").cast("string").alias("action"),
        F.expr("payload:number").cast("int").alias("pr_number"),

        F.expr("payload:pull_request.id").cast("bigint").alias("pr_id"),

        F.expr("payload:pull_request.head.ref").cast("string").alias("source_branch"),
        F.expr("payload:pull_request.head.sha").cast("string").alias("commit_sha"),

        F.expr("payload:pull_request.base.ref").cast("string").alias("target_branch"),
        F.expr("payload:pull_request.base.sha").cast("string").alias("target_base_sha")
    )
    )

    silver_pr_event_clean_df = silver_pr_event_df.dropDuplicates(["id"])

    query = (
        silver_pr_event_clean_df.writeStream
            .trigger(availableNow=True)
            .option("checkpointLocation", checkpoint)
            .toTable(table)
    )
