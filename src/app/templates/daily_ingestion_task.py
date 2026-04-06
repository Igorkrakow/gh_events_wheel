import requests
from datetime import datetime, timedelta
from pathlib import Path
from pyspark.sql import SparkSession
import app.config as conf

def run():
    spark = SparkSession.builder.getOrCreate()
    BASE_PATH = conf.BASE_PATH

    state = spark.table(f"{conf.SCHEMA_PATH}.gh_download_state").collect()[0]
    current_date = state["current_date"]
    current_hour = state["current_hour"]

    day_dt = datetime.strptime(current_date, "%Y-%m-%d")

    day_str = day_dt.strftime("%Y-%m-%d")
    day_folder = Path(f"{BASE_PATH}/{day_str}")
    day_folder.mkdir(parents=True, exist_ok=True)

    missing_files = []

    for i in range(2):
        hour = current_hour + i

        filename = f"{day_str}-{hour}.json.gz"
        file_path = day_folder / filename

        if file_path.exists():
            print(f"skip: {filename}")
            continue

        url = f"https://data.gharchive.org/{filename}"

        try:
            r = requests.get(url, timeout=60)

            if r.status_code == 200:
                with open(file_path, "wb") as f:
                    f.write(r.content)

                print(f"downloaded: {filename}")
            else:
                print(f"ERROR {r.status_code}: {filename}")
                missing_files.append(filename)

        except Exception as e:
            print(f"ERROR downloading {filename}: {e}")
            missing_files.append(filename)

    new_hour = current_hour + 2
    new_date = day_dt

    if new_hour > 23:
        new_hour = 0
        new_date = day_dt + timedelta(days=1)

    new_date_str = new_date.strftime("%Y-%m-%d")

    spark.sql(f"DELETE FROM {conf.SCHEMA_PATH}.gh_download_state")

    spark.sql(f"""
    INSERT INTO {conf.SCHEMA_PATH}.gh_download_state VALUES ('{new_date_str}', {new_hour})
    """)

    if missing_files:
        raise Exception(f"missing files: {missing_files}")

    print(f"Done. Next start from {new_date_str} hour {new_hour}")