from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import app.config as conf
def run():
    spark = SparkSession.builder.getOrCreate()
    source_table = f'{conf.SCHEMA_PATH}.github_events_bronze'
    checkpoint = f'{conf.CHECK_POINTS_PATH}issues_event'
    table = f'{conf.SCHEMA_PATH}.gh_events_issues_silver'
    spark.conf.set("spark.sql.shuffle.partitions", 32)
    df = (
        spark.readStream
        .table(source_table)
    )

    silver_issues_event_df = (df
        .filter(F.col("type") == "IssuesEvent")
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
        F.expr("payload:issue:id").cast("long").alias("issue_id"),
        F.expr("payload:issue:number").cast("int").alias("issue_number"),
        F.expr("payload:issue:title").cast("string").alias("title"),

        F.expr("payload:issue:created_at").cast("timestamp").alias("issue_created_at"),
        F.expr("payload:issue:closed_at").cast("timestamp").alias("issue_closed_at"),
        F.expr("payload:issue:state").cast("string").alias("issue_state"),
        F.expr("payload:issue:comments").cast("int").alias("comments_count")

    )
    )

    silver_issues_event_clean_df = silver_issues_event_df.dropDuplicates(["id"])

    query = (
        silver_issues_event_clean_df.writeStream
            .trigger(availableNow=True)
            .option("checkpointLocation", checkpoint)
            .toTable(table)
    )