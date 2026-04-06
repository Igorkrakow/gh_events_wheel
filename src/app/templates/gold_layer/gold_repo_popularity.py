from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import app.config as conf
def run():
    spark = SparkSession.builder.getOrCreate()


    def update_gold_popularity():
        silver_watch = spark.read.table("course_catalog.ihor_schema.gh_events_watch_silver")

        window_spec = Window.partitionBy("report_date").orderBy(F.desc("stars_gained"))

        gold_popularity = (
            silver_watch
                .withColumn("report_date", F.date_trunc("day", F.col("event_time")).cast("date"))
                .groupBy("report_date", "repo_name", "org_name")
                .agg(F.count("id").alias("stars_gained"))
                .withColumn("rank", F.rank().over(window_spec))
        )

        gold_popularity.write.format("delta").mode("overwrite").saveAsTable(
            "course_catalog.ihor_schema.gold_repo_popularity")


    update_gold_popularity()