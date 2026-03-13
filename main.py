from src.extract_data_films import extract_film_data
from src.extract_data_genres import extract_genre_data
from dotenv import load_dotenv
import os
from pathlib import Path
import requests
import json

env_folder_path = Path('.') / 'config' / '.env'
load_dotenv(dotenv_path=env_folder_path)
API_KEY = os.getenv('API_KEY')



def main():
    url = f"https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}&language=en-US"
    data = extract_film_data(url)
    print(data)



if __name__ == "__main__":
    main()
