from pyspark.sql import SparkSession
import app.config as conf
def run():
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"""
          CREATE TABLE IF NOT EXISTS {conf.SCHEMA_PATH}.gh_download_state (
    current_date STRING,
    current_hour INT
)
USING DELTA
  
        """)
    state_df = spark.table(f"{conf.SCHEMA_PATH}.gh_download_state")

    if state_df.count() == 0:
        spark.sql(f"""
        INSERT INTO {conf.SCHEMA_PATH}.gh_download_state VALUES ('2026-03-27', 2)
        """)