import requests
import json
from pathlib import Path
import os
from dotenv import load_dotenv


env_folder_path = Path('.') / 'config' / '.env'

load_dotenv(dotenv_path=env_folder_path)
API_KEY = os.getenv('API_KEY')

base_url = 'https://api.themoviedb.org/3/discover/movie'
url_params = f'?api_key={API_KEY}&language=en-US&sort_by=popularity.desc'

url = f"{base_url}{url_params}&page=1"
# request the first five pages to gather roughly 100 movies
pages = range(1, 6)
urls = [f"{base_url}{url_params}&page={p}" for p in pages]

def extract_film_data(urls) -> list:
    if isinstance(urls, str):
        urls = [urls]

    all_movies = []

    for u in urls:
        response = requests.get(u)
        if response.status_code != 200:
            print(f"Erro de requisição para {u}")
            continue
        data = response.json() or {}
        page_results = data.get('results', [])
        all_movies.extend(page_results)

    if not all_movies:
        print("Dados vazios")
        return []

    raw_data_path = "../ETLlocal-films/data/raw/latest_films.json"
    with open(raw_data_path, 'w') as f:
        json.dump(all_movies, f, indent=4)

    print(f"Arquivo json cru de ultimos filmes em {raw_data_path}")
    return all_movies



# extract_film_data(urls)