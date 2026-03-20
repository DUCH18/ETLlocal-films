
from airflow.decorators import task, dag
from datetime import datetime, timedelta
from airflow.models import Variable
from pathlib import Path
import os
import pandas as pd

from src.extract_db_env import extract_db_envs
from src.extract_data_films import extract_film_data
from src.extract_data_genres import extract_genre_data
from src.extract_data_person import extract_person_data
from src.load_data_films import load_data
from src.transform_data_films import transform_films
from src.transform_data_person import transform_person

from dotenv import load_dotenv


MAX_PAGES = 500

env_folder_path = Path('.') / 'config' / '.env'
load_dotenv(dotenv_path=env_folder_path)

API_KEY = os.getenv('API_KEY')

genre_url = f"https://api.themoviedb.org/3/genre/movie/list?api_key={API_KEY}"

@dag(
    dag_id="dag_film_digest",
    default_args = {
        'owner' : 'airflow',
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    },
    start_date= datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) 
def dag_ingest():

    @task
    def extract_db_variables():
        current_page_films, current_page_persons = extract_db_envs()
        Variable.set("current_page_films", current_page_films)
        Variable.set("current_page_persons", current_page_persons)

    @task
    def extract_genres():
        return extract_genre_data(genre_url)
    
    @task
    def extract_persons():
        current_page = int(Variable.get("current_page_persons", default_var=1))
        persons_url = f"https://api.themoviedb.org/3/person/popular?api_key={API_KEY}&language=en-US&page={current_page}"
        
        return extract_person_data(persons_url)
    @task
    def extract_films():
        current_page = int(Variable.get("current_page_films", default_var=1))
        films_url = f"https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}&language=en-US&page={current_page}"
        
        return extract_film_data(films_url)

    @task()
    def transform_person_task():
        df_1, df_2 = transform_person()
        df_1.to_parquet("/opt/airflow/data/processed_silver/persons.parquet", index=False)
        df_2.to_parquet("/opt/airflow/data/processed_silver/persons_films.parquet", index=False)

    @task
    def transform_films_task():
        df = transform_films()
        df.to_parquet("/opt/airflow/data/processed_silver/films.parquet", index=False)
    
    
    
    @task
    def load():
        films = pd.read_parquet("/opt/airflow/data/processed_silver/films.parquet")
        genres = pd.read_parquet("/opt/airflow/data/processed_silver/genres.parquet")
        persons = pd.read_parquet("/opt/airflow/data/processed_silver/persons.parquet")
        current_page_films = int(Variable.get("current_page_films", default_var=1))
        current_page_persons = int(Variable.get("current_page_films", default_var=1))
        


        load_data(films, genres, persons, current_page_films, current_page_persons)
    


    extract_db_variables() >> [extract_genres(),  extract_films(),  extract_persons()] >> transform_person_task() >> transform_films_task() >> load()
    
    
dag_ingest()


   
