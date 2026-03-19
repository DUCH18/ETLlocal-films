import os
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd
import logging



env_folder_path = Path('.') / 'config' / '.env'
load_dotenv(dotenv_path=env_folder_path)

database = os.getenv('database')
user = os.getenv("user")
pswd = os.getenv("password")

host = 'host.docker.internal'
host = '172.26.80.136'
port = "5432"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_engine_():
    logging.info(f"Creating database engine... on {host}:{database}")

    try:
        engine = create_engine(
            f"postgresql://{user}:{pswd}@{host}:{port}/{database}"
        )
        
    except Exception as e:
        logging.error(f"Error occurred while connecting to the database: {e}")
        raise e
    return engine


# def print_all_tables(engine):
    
#     print("Tabelas : qtde de registros inseridos")
#     f_check = df_films.read_sql_query("SELECT * FROM films;", engine)
#     g_check = df_genre.read_sql_query("SELECT * FROM genres;", engine)
#     p_check = df_from_persons.read_sql_query("SELECT * FROM person;", engine)
#     print(f"films : {len(f_check)} registro(s) inseridos")
#     print(f"genres : {len(g_check)} registro(s) inseridos")
#     print(f"person : {len(p_check)} registro(s) inseridos")


def load_data(df_films, df_genre, df_from_persons, current_page_films, current_page_persons):
    engine = create_engine_()

    current_page_films += 1 if current_page_films < 500 else 1
    current_page_persons += 1 if current_page_persons < 500 else 1
    

    df_env_vars = pd.Dataframe({"current_page_films": current_page_films, "current_page_persons": current_page_persons}, index=[0])

    df_env_vars.to_sql('db_env', con=engine, if_exists='replace', index=False)
    df_films.to_sql('films', con=engine, if_exists='append', index=False)  
    df_genre.to_sql('genres', con=engine, if_exists='replace', index=False)  
    df_from_persons.to_sql('person', con=engine, if_exists='append', index=False)  
    
    logging.info("Registros inseridos com sucesso nas tabelas: films, genres, person")

    print("Tabelas : qtde de registros inseridos")
    f_check = pd.read_sql("SELECT * FROM films;", engine)
    g_check = pd.read_sql("SELECT * FROM genres;", engine)
    p_check = pd.read_sql("SELECT * FROM person;", engine)
    env_check = pd.read_sql("SELECT * FROM db_env;", engine)
    print(f"films : {len(f_check)} registro(s) inseridos")
    print(f"genres : {len(g_check)} registro(s) inseridos")
    print(f"person : {len(p_check)} registro(s) inseridos")
    print(f"db_env : {env_check.iloc[0]['current_page_films']} e {env_check.iloc[0]['current_page_persons']} proximas paginas")
 
    

    engine.close()

