import json
import pandas as pd
from pathlib import Path


cols_to_drop = [
    "adult",
    "original_name",
    "profile_path",
    "known_for"
]
new_cols = [
    "known_for_0_media_id",
    "known_for_1_media_id",
    "known_for_2_media_id",
]
cols_to_rename = {
    "known_for_department": "department"
}

def create_dataframe(data_person_path:Path)->pd.DataFrame:
    
    with open(data_person_path, 'r') as f:
        data_person = json.load(f)

    df = pd.json_normalize(data_person["results"])
    return df


def drop_columns_person(df:pd.DataFrame)->pd.DataFrame:
    return df.drop(columns=cols_to_drop)


def rename_cols(df:pd.DataFrame)->pd.DataFrame:
    return df.rename(columns=cols_to_rename)



# def flatten_known_for_films(df_persons:pd.DataFrame, total_films:list)->pd.DataFrame:
#     df_persons["known_for_0_movie_id"] = df_persons.apply(lambda row_person : funcao_acha_id_filme(row_person["known_for"], 0, total_films), axis=1)
#     df_persons["known_for_1_movie_id"] = df_persons.apply(lambda row_person : funcao_acha_id_filme(row_person["known_for"], 1, total_films), axis=1)
#     df_persons["known_for_2_movie_id"] = df_persons.apply(lambda row_person : funcao_acha_id_filme(row_person["known_for"], 2, total_films), axis=1)

# def funcao_acha_id_filme(films_json_person:object, i:int, total_films:list):
#     count = 0
#     id_encontrado = -1
#     for media in films_json_person:
#         if media["media_type"] == "movie" :
#             if count == i:        
#                 id_encontrado = media["id"] 
#                 total_films.append(media)
#                 break

#             count += 1
#     return int(id_encontrado)

def funcao_acha_id_filme2(films_json_person:object, total_films:list):
    count = 0
    ids_encontrados = [-1, -1, -1]
    for media in films_json_person:
        if media["media_type"] == "movie" :
            if count > 2:
                break
            total_films.append(media)
            ids_encontrados[count] = int(media["id"] )
            count += 1

    return tuple(ids_encontrados)

def flatten_known_for_films2(df_persons:pd.DataFrame, total_films:list)->pd.DataFrame:
    result = df_persons.apply(
        lambda row_person : funcao_acha_id_filme2(row_person["known_for"], total_films), 
            axis=1,
            result_type='expand'
        )
    df_persons[new_cols] = result



def transform_person():
    total_films = []
    data_person_path = Path('.').parent.parent / 'data' / 'raw' / 'persons.json'
    df_persons = create_dataframe(data_person_path)

    flatten_known_for_films2(df_persons, total_films)
    df_persons = drop_columns_person(df_persons)
    df_persons = rename_cols(df_persons)

    

    
    parquet_path = Path('.').parent.parent / 'data' / 'processed_silver' / 'persons.parquet'
    # if the directory does not exist, create it, if it already exists, do nothing. Append the new file to the existing directory, if file exists, append data to existing csv.
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df_persons.to_parquet(parquet_path, index=False)
    
    parquet_path = Path('.').parent.parent / 'data' / 'processed_silver' / 'persons_films.parquet'
    df_persons_films = pd.DataFrame(total_films)
    df_persons_films.to_parquet(parquet_path, index=False)

    return df_persons, df_persons_films






