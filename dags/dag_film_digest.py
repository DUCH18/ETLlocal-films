from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from pathlib import Path
import os

from src.extract_data_films import extract_film_data, urls


with DAG(
    dag_id="dag_film_digest",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    @task
    def extract_film_data_task(urls):
        data = extract_film_data(urls)
        output_dir = Path("/tmp/film_data")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "film_data.json"
        with open(output_file, "w") as f:
            f.write(data)
        return str(output_file)
