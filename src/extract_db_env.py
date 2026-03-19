from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd
import logging
import os
from pathlib import Path


env_folder_path = Path('.') / 'config' / '.env'
load_dotenv(dotenv_path=env_folder_path)

database = os.getenv('database')
user = os.getenv("user")
pswd = os.getenv("password")

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

def extract_db_envs():
    engine = create_engine_()

    with engine.connect() as connection:

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS db_env (
                current_page_films INTEGER DEFAULT 1,
                current_page_persons INTEGER DEFAULT 1
            )
        """))
        



    df = pd.read_sql(text("SELECT current_page_films, current_page_persons FROM db_env"), con=engine)

    if df.empty:
        logging.info("db_env table is empty. Inserting initial page 1.")
        with engine.connect() as connection:
            connection.execute(text("INSERT INTO db_env (current_page_films, current_page_persons) VALUES (1, 1)"))
        current_page_films = 1
        current_page_persons = 1
    else:
        current_page_films = df['current_page_films'].iloc[0]
        current_page_persons = df['current_page_persons'].iloc[0]
        logging.info(f"Current pages extracted from db_env: Films={current_page_films}, Persons={current_page_persons}")

    return current_page_films, current_page_persons