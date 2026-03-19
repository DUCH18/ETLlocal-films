import logging

import requests
import json
from pathlib import Path
import pandas as pd



def extract_genre_data(url:str):
    
    response = requests.get(url)
    if response.status_code != 200:
        logging.error(f"Erro de requisição para {url}: {response.status_code}")
        
        return []
        
    data = response.json()
    

    if not data:
        logging.warning(f"Dados vazios para {url}")
  
        return []

    # raw_data_path = "../data/raw/latest_films.json"
    raw_data_path = Path('.').parent.parent / 'data' / 'raw' / 'genres.json'
    with open(raw_data_path, 'w') as f:
        json.dump(data, f, indent=4)

    print(f"Arquivo json cru de generos de filmes em {raw_data_path}")
    df_genre = pd.json_normalize(data["genres"])
   
    return df_genre.to_parquet(Path('.').parent.parent / 'data' / 'processed_silver' / 'genres.parquet', index=False)

