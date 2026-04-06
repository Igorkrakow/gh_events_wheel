import sys

from app.templates.daily_ingestion_task import run as daily_run
from app.templates.bronze_parsing_task import run as bronze_parsing

from app.templates.silver_layer.create_event import run as create_event
from app.templates.silver_layer.delete_event import run as delete_event
from app.templates.silver_layer.issue_comment_event import run as issue_comment_event
from app.templates.silver_layer.issues_event import run as issues_event
from app.templates.silver_layer.member_event import run as member_event
from app.templates.silver_layer.pull_request_event import run as pull_request_event
from app.templates.silver_layer.pull_request_rev_event import run as pull_request_rev_event
from app.templates.silver_layer.push_event import run as push_event
from app.templates.silver_layer.release_event import run as release_event
from app.templates.silver_layer.watch_event import run as watch_event

from app.templates.gold_layer.gold_daily_user_rating import run as gold_daily_user_rating
from app.templates.gold_layer.gold_pr_cycle import run as gold_pr_cycle
from app.templates.gold_layer.gold_repo_popularity import run as gold_repo_popularity

from app.templates.bootstrap.configuration_tables_creation import run as configuration_tables_creation
from app.templates.bootstrap.silver_tables_creation import run as silver_tables_creation

JOBS = {
    "daily-injection": daily_run,
    "bronze_parsing": bronze_parsing,

    "create_event": create_event,
    "delete_event": delete_event,
    "issue_comment_event": issue_comment_event,
    "issues_event": issues_event,
    "member_event": member_event,
    "pull_request_event": pull_request_event,
    "pull_request_rev_event": pull_request_rev_event,
    "push_event": push_event,
    "release_event": release_event,
    "watch_event": watch_event,

    "gold_daily_user_rating": gold_daily_user_rating,
    "gold_pr_cycle": gold_pr_cycle,
    "gold_repo_popularity": gold_repo_popularity,

    "configuration_tables_creation": configuration_tables_creation,
    "silver_tables_creation": silver_tables_creation

}


def main():
    job_name = sys.argv[1]
    if job_name not in JOBS:
        raise ValueError(f"Unknown job: {job_name}")
    JOBS[job_name]()


if __name__ == "__main__":
    main()