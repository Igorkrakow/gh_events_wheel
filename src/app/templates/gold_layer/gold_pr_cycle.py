from pyspark.sql.window import Window as w
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import app.config as conf
def run():
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.table("course_catalog.ihor_schema.gh_events_pr_silver").select("pr_id","org_name","action","event_time").filter(
        col("org_name").isNotNull())
    df_pr = spark.read.table("course_catalog.ihor_schema.gh_events_pr_review_silver").select("pr_id","org_name",lit("review").alias("action"),"event_time").filter(
        col("org_name").isNotNull())
    full_df = df_pr.unionByName(df)

    df2 = full_df.withColumn(
        "opened_time",
        min(when(col("action") == "opened", col("event_time"))).over(w.partitionBy("pr_id"))
    ).withColumn(
        "first_review_time",
        min(when(col("action") == "review", col("event_time"))).over(w.partitionBy("pr_id"))
    ).withColumn(
        "merged_time",
        min(when(col("action") == "merged", col("event_time"))).over(w.partitionBy("pr_id"))
    )



    result = df2.filter(
        col("opened_time").isNotNull() &
        col("merged_time").isNotNull() &
        col("first_review_time").isNotNull()
    ).select(
        "pr_id",
        "org_name",
        "opened_time",
        "first_review_time",
        "merged_time",
        (
            col("merged_time").cast("long") - col("opened_time").cast("long")
        ).alias("cycle_time_seconds"),
        (
            col("first_review_time").cast("long") - col("opened_time").cast("long")
        ).alias("open_to_review_time_seconds"),
        (
            col("merged_time").cast("long") - col("first_review_time").cast("long")
        ).alias("review_to_close_time_seconds")
    ).dropDuplicates(["pr_id"])

    pr_metrics = result.withColumn(
        "year", year("merged_time")
    ).withColumn(
        "week", weekofyear("merged_time")
    )



    weekly = pr_metrics.groupBy("org_name", "year", "week").agg(
        count("*").alias("pr_closed"),
        avg("cycle_time_seconds").cast("int").alias("avg_cycle"),
        avg("open_to_review_time_seconds").cast("int").alias("avg_open_to_review"),
        avg("review_to_close_time_seconds").cast("int").alias("avg_review_to_close"),
       ( (count("*") / avg("cycle_time_seconds")) * log10(count("*")) ).cast("decimal(18,3)").alias("growth_score")
    )
    weekly.write.format("delta").mode("overwrite").saveAsTable("course_catalog.ihor_schema.gold_rp_cycle")
