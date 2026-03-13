import json
import pandas as pd
from pathlib import Path

cols_to_drop = [
    "adult",
    "backdrop_path",
    "original_title",
    "poster_path"
]

def create_dataframe(json_path):
    json_path = Path('__path__').parent.parent / 'data' / 'raw' / 'latest_films.json'
    with open(json_path) as f:
        data = json.load(f)
        
    df = pd.json_normalize(data["results"])
    return df

def drop_film_cols(df:pd.DataFrame)->pd:DataFrame:
    df = df.drop(columns=cols_to_drop)
    return df 

def enrich_film_genre(df:pd.DataFrame, data_genre)->pd.DataFrame:
    df_genre = pd.json_normalize(data_genre)
    
    df = df.concat([df, df_genre], axis=1)
    df.apply(lambda x : x)


    df.drop('genre_ids', axis=1)
    
    return df 

def transform_films(data_genre):
    df = create_dataframe()
    df = drop_film_cols(df)
    df = enrich_film_genre(df, data_genre)
    return df





