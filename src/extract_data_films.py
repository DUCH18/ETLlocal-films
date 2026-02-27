import requests
import json
from pathlib import Path
import os



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

    # raw_data_path = "../data/raw/latest_films.json"
    raw_data_path = Path('.').parent.parent / 'data' / 'raw' / 'latest_films.json'
    with open(raw_data_path, 'w') as f:
        json.dump(all_movies, f, indent=4)

    print(f"Arquivo json cru de ultimos filmes em {raw_data_path}")
    return all_movies



# extract_film_data(urls)