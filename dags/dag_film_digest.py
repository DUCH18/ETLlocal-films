
from airflow.decorators import task, dag
from datetime import datetime, timedelta

from pathlib import Path
import os
from airflow.models import Variable

from src.extract_data_films import extract_film_data
from src.extract_data_genres import extract_genre_data
from src.transform_data_films import transform_films

from dotenv import load_dotenv


MAX_PAGES = 500

env_folder_path = Path('.') / 'config' / '.env'
load_dotenv(dotenv_path=env_folder_path)
API_KEY = os.getenv('API_KEY')


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
    def extract_genres():
        genre_url = f"https://api.themoviedb.org/3/genre/movie/list?api_key={API_KEY}"
        return extract_genre_data(genre_url)

    @task
    def extract_films():
        current_page = int(Variable.get("current_page", default_var=1))
        url = f"https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}&language=en-US&page={current_page}
        
        return extract_film_data(url)

    @task
    def transform(genres):
        return transform_films(genres, films)
     
    
    @task
    def load():
        # ...
        Variable.set("current_page", current_page + 1) if current_page <= MAX_PAGES else Variable.set("current_page", 1)
    


    

    extract_films() >> genres = extract_genres() >> transform(genres) >> load() 
    
dag_ingest()


   
