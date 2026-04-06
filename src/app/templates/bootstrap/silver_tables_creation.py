from pyspark.sql import SparkSession
import app.config as conf
def run():
    spark = SparkSession.builder.getOrCreate()

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {conf.SCHEMA_PATH}.gh_events_push_silver (
            id BIGINT,
            event_time TIMESTAMP,
            user_id BIGINT,
            user_login STRING,
            org_id BIGINT,
            org_name STRING,
            repo_id BIGINT,
            repo_name STRING,
            branch STRING,
            parent_sha STRING,
            commit_sha STRING
        )
        USING DELTA
    """)

    spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {conf.SCHEMA_PATH}.gh_events_pr_silver (
 id LONG,
 event_time TIMESTAMP,
 user_id long,
 user_login STRING,
 org_id LONG,
 org_name STRING,
 repo_id LONG,
 repo_name STRING,

 action STRING,
 pr_number INT,
 pr_id LONG,

 source_branch STRING,
 commit_sha STRING,
 target_branch STRING,
 target_base_sha STRING

)
USING DELTA
PARTITIONED BY (action)
        """)

    spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {conf.SCHEMA_PATH}.gh_events_pr_review_silver (
 id LONG,
 event_time TIMESTAMP,
 user_id long,
 user_login STRING,
 org_id LONG,
 org_name STRING,
 repo_id LONG,
 repo_name STRING,
 
 action STRING,
 review_id LONG,
 review_state STRING,
 submitted_at TIMESTAMP,


 pr_id LONG,
 commit_sha STRING
)
USING DELTA
PARTITIONED BY (action)
        """)

    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {conf.SCHEMA_PATH}.gh_events_create_silver (
 id LONG,
 event_time TIMESTAMP,
 user_id long,
 user_login STRING,
 org_id LONG,
 org_name STRING,
 repo_id LONG,
 repo_name STRING,
 
 ref_name STRING,
 ref_type STRING,
 base_branch STRING,
 pusher_type STRING
)
USING DELTA
            """)

    spark.sql(f"""
CREATE TABLE IF NOT EXISTS {conf.SCHEMA_PATH}.gh_events_delete_silver (
 id LONG,
 event_time TIMESTAMP,
 user_id long,
 user_login STRING,
 org_id LONG,
 org_name STRING,
 repo_id LONG,
 repo_name STRING,
 
 ref_name STRING,
 ref_type STRING,
 pusher_type STRING
)
USING DELTA
            """)

    spark.sql(f"""
CREATE TABLE IF NOT EXISTS {conf.SCHEMA_PATH}.gh_events_issues_silver (
 id LONG,
 event_time TIMESTAMP,
 user_id long,
 user_login STRING,
 org_id LONG,
 org_name STRING,
 repo_id LONG,
 repo_name STRING,
 
 action STRING,
 issue_id LONG,
 issue_number INT,
 title string,
issue_created_at TIMESTAMP,
issue_closed_at TIMESTAMP,
issue_state STRING,
comments_count INT

)
USING DELTA
PARTITIONED BY (action)
            """)

    spark.sql(f"""
CREATE TABLE IF NOT EXISTS {conf.SCHEMA_PATH}.gh_events_issue_comment_silver (
 id LONG,
 event_time TIMESTAMP,
 user_id long,
 user_login STRING,
 org_id LONG,
 org_name STRING,
 repo_id LONG,
 repo_name STRING,
 
 action STRING,
comment_id LONG,
comment_text string,
 issue_id LONG,
 issue_number INT,
issue_state STRING

)
USING DELTA
PARTITIONED BY (action)
            """)

    spark.sql(f"""
CREATE TABLE IF NOT EXISTS {conf.SCHEMA_PATH}.gh_events_watch_silver (
 id LONG,
 event_time TIMESTAMP,
 user_id long,
 user_login STRING,
 org_id LONG,
 org_name STRING,
 repo_id LONG,
 repo_name STRING,
 
action STRING
)
USING DELTA
PARTITIONED BY (action)
            """)

    spark.sql(f"""
CREATE TABLE IF NOT EXISTS {conf.SCHEMA_PATH}.gh_events_member_silver (
 id LONG,
 event_time TIMESTAMP,
 user_id long,
 user_login STRING,
 org_id LONG,
 org_name STRING,
 repo_id LONG,
 repo_name STRING,

action STRING,
member_id LONG,
member_login STRING,
member_type STRING


)
USING DELTA
PARTITIONED BY (action)
            """)

    spark.sql(f"""
CREATE TABLE IF NOT EXISTS {conf.SCHEMA_PATH}.gh_events_release_silver (
 id LONG,
 event_time TIMESTAMP,
 user_id long,
 user_login STRING,
 org_id LONG,
 org_name STRING,
 repo_id LONG,
 repo_name STRING,

action STRING,
release_id LONG,
tag_name STRING,
release_name STRING,
target_branch STRING,
published_at TIMESTAMP,
is_prerelease BOOLEAN


)
USING DELTA
PARTITIONED BY (action)
            """)