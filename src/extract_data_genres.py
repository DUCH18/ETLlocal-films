import requests
import json
from pathlib import Path
import os



def extract_genre_data(url:str):
    
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Erro de requisição para {url}")
        return []
        
    data = response.json()
    

    if not data:
        print("Dados vazios")
        return []

    # raw_data_path = "../data/raw/latest_films.json"
    raw_data_path = Path('.').parent.parent / 'data' / 'raw' / 'genres.json'
    with open(raw_data_path, 'w') as f:
        json.dump(data, f, indent=4)

    print(f"Arquivo json cru de generos de filmes em {raw_data_path}")
    return data



# extract_film_data(urls)