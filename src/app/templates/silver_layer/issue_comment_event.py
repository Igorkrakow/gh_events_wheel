from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import app.config as conf
def run():
    spark = SparkSession.builder.getOrCreate()
    source_table = f'{conf.SCHEMA_PATH}.github_events_bronze'
    checkpoint = f'{conf.CHECK_POINTS_PATH}issue_comment_event'
    table = f'{conf.SCHEMA_PATH}.gh_events_issue_comment_silver'
    spark.conf.set("spark.sql.shuffle.partitions", 32)
    df = (
        spark.readStream
        .table(source_table)
    )
    silver_issue_comment_df = (df
        .filter(F.col("type") == "IssueCommentEvent")
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
        F.expr("payload:comment.id").cast("bigint").alias("comment_id"),
        F.expr("payload:comment.body").alias("comment_text"),

        F.expr("payload:issue.id").cast("bigint").alias("issue_id"),
        F.expr("payload:issue.number").cast("int").alias("issue_number"),
        F.expr("payload:issue.state").alias("issue_state")
    )
    )
    silver_issue_comment_clean_df = silver_issue_comment_df.dropDuplicates(["id"])
    query = (
        silver_issue_comment_clean_df.writeStream
            .trigger(availableNow=True)
            .option("checkpointLocation", checkpoint)
            .toTable(table)
    )