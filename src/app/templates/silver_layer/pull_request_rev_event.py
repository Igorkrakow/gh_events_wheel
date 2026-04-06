from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import app.config as conf
def run():
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 32)
    source_table = f'{conf.SCHEMA_PATH}.github_events_bronze'
    checkpoint = f'{conf.CHECK_POINTS_PATH}pull_request_review_event'
    table =f'{conf.SCHEMA_PATH}.gh_events_pr_review_silver'
    spark.conf.set("spark.sql.shuffle.partitions", 32)
    df = (
        spark.readStream
        .table(source_table)
    )

    silver_pr_review_event_df = (df
        .filter(F.col("type") == "PullRequestReviewEvent")
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
        F.expr("payload:review.id").cast("bigint").alias("review_id"),
        F.expr("payload:review.state").alias("review_state"),
        F.expr("payload:review.submitted_at").cast("timestamp").alias("submitted_at"),

        F.expr("payload:pull_request.id").cast("bigint").alias("pr_id"),
        F.expr("payload:review.commit_id").alias("commit_sha")
    )
    )

    silver_pr_review_event_clean_df = silver_pr_review_event_df.dropDuplicates(["id"])

    query = (
        silver_pr_review_event_clean_df.writeStream
            .trigger(availableNow=True)
            .option("checkpointLocation", checkpoint)
            .toTable(table)
    )