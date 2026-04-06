from pyspark.sql.functions import col, lit, to_date, count, when
from pyspark.sql import SparkSession
import app.config as conf
def run():
    spark = SparkSession.builder.getOrCreate()

    date = (
        spark.read.table("course_catalog.ihor_schema.gh_download_state")
            .select("current_date")
            .first()["current_date"]
    )

    df_pr = (
        spark.read.table("course_catalog.ihor_schema.gh_events_pr_silver")
            .select("user_id", "user_login")
            .filter(to_date(col("event_time")) == lit(date))
            .withColumn("event_type", lit("pr"))
    )

    df_pr_review = (
        spark.read.table("course_catalog.ihor_schema.gh_events_pr_review_silver")
            .select("user_id", "user_login")
            .filter(to_date(col("event_time")) == lit(date))
            .withColumn("event_type", lit("review"))
    )

    df_pr_issue = (
        spark.read.table("course_catalog.ihor_schema.gh_events_issues_silver")
            .select("user_id", "user_login")
            .filter(to_date(col("event_time")) == lit(date))
            .withColumn("event_type", lit("issue"))
    )

    df_pr_push = (
        spark.read.table("course_catalog.ihor_schema.gh_events_push_silver")
            .select("user_id", "user_login")
            .filter(to_date(col("event_time")) == lit(date))
            .withColumn("event_type", lit("push"))
    )

    df = (
        df_pr
            .unionByName(df_pr_review)
            .unionByName(df_pr_issue)
            .unionByName(df_pr_push)
    )



    result = (
        df.groupBy("user_id", "user_login")
            .agg(
            count(when(col("event_type") == "pr", True)).alias("pr_count"),
            count(when(col("event_type") == "review", True)).alias("review_count"),
            count(when(col("event_type") == "issue", True)).alias("issue_count"),
            count(when(col("event_type") == "push", True)).alias("push_count"),
        )
            .withColumn(
            "total_events",
            col("pr_count") + col("review_count") + col("issue_count") + col("push_count")
        )
            .withColumn(
            "activity_score",

            col("pr_count") * 5 +
            col("review_count") * 3 +
            col("issue_count") * 2 +
            col("push_count") * 1

        )
            .withColumn("day", lit(date))
    )
    result.write.format("delta").mode("overwrite").saveAsTable("course_catalog.ihor_schema.gold_daily_user_rating")