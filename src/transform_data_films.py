import json
import pandas as pd
from pathlib import Path

cols_to_drop = [
    "adult",
    "backdrop_path",
    "original_title",
    "poster_path",
    "genre_ids"
]
new_cols = [
    "genre_0_media_id",
    "genre_1_media_id",
    "genre_2_media_id",
]

def create_dataframe(film_data_path:str)->pd.DataFrame:
    with open(film_data_path, "r") as f:
        film_data = json.load(f)
    df = pd.json_normalize(film_data["results"])
    return df

def drop_film_cols(df:pd.DataFrame)->pd.DataFrame:
    return df.drop(columns=cols_to_drop)

def funcao_acha_id_genres(genre_ids:list):
    count = 0
    ids_encontrados = [-1, -1, -1]
    
    for genre in genre_ids:
        if count > 2:
            break
       
        ids_encontrados[count] = genre
        count += 1

    return tuple(ids_encontrados)

def flatten_film_genre(df:pd.DataFrame)->pd.DataFrame:

    result = df.apply(
        lambda row: funcao_acha_id_genres(row["genre_ids"]),
        axis=1,
        result_type='expand' 
    )
    df[new_cols] = result
   
    return df 

def transform_films()->pd.DataFrame:
    data_films_path = Path('.').parent.parent / 'data' / 'raw' / 'films.json'
    data_films_persons_path = Path('.').parent.parent / 'data' / 'processed_silver' / 'persons_films.parquet'

    df = create_dataframe(data_films_path)
    df_from_persons = pd.read_parquet(data_films_persons_path)

    
    df = pd.concat([df, df_from_persons], axis=0)
    df = flatten_film_genre(df)
    df = drop_film_cols(df)

    return df
    
    





