import json
import pandas as pd
from pathlib import Path



def create_dataframe(json_path):

    with open(json_path) as f:
        data = json.load(f)
        
    df = pd.json_normalize(data)
    print(df)
    return df


def transform_film_df(data: pd.DataFrame) -> pd.DataFrame:

    pass


path = Path('__path__').parent.parent / 'data' / 'raw' / 'latest_films.json' 
create_dataframe(path)