from dotenv import load_dotenv

from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime
from pathlib import Path
import os

from src.extract_data_films import extract_film_data
from dotenv import load_dotenv


env_folder_path = Path('.') / 'config' / '.env'
load_dotenv(dotenv_path=env_folder_path)
API_KEY = os.getenv('API_KEY')


@dag(
    dag_id="dag_film_digest",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)

def dag_ingest():
    @task
    def extract():
        

        base_url = 'https://api.themoviedb.org/3/discover/movie'
        url_params = f'?api_key={API_KEY}&language=en-US&sort_by=popularity.desc'

        url = f"{base_url}{url_params}&page=1"
        # request the first five pages to gather roughly 100 movies
        pages = range(1, 6)
        urls = [f"{base_url}{url_params}&page={p}" for p in pages]
        extract_film_data(urls)

    @task
    def load():
        pass

    @task
    def transform():
        pass

    extract() >> load() >> transform()
    
dag_ingest()


   
