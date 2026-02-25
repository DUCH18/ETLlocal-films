import requests
import json
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv('API_KEY')
url =  f'https://api.themoviedb.org/3/movie/11?api_key={API_KEY}'

def extract_film_data(url:str) -> list:
    response = requests.get(url)

    if response.status_code != 200:
        print("Erro de requisição")
        return []
        
    data = response.json()

    if not data:
        print("Dados vazios")
        return []

    raw_data_path = "../ETLlocal-films/data/raw_latest_films.json"

    f = open(raw_data_path, 'w')
    json.dump(data, f, indent=4)
    f.close()

    print(f"Arquivo json cru de ultimos filmes em {raw_data_path}")

    
    return data

extract_film_data(url)