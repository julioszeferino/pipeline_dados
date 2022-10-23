import yaml
import os
from typing import Dict
import sqlalchemy as sa
import psycopg2
import pandas as pd
from typing import List

PATH = os.path.dirname(os.path.abspath(__name__))

def load_yaml() -> Dict[str, Dict[str, str]]:

    config_path: str = PATH + "/conf/config.yaml"

    with open(config_path) as conf_file:
        config = yaml.safe_load(conf_file)

    return config


def load_database(host: str, user: str, password: str, database: str, models: List[str]) -> None:
    
    conn = sa.create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:5432/{database}")
    
    for model in models:

        print('Inserindo dados na tabela: {}'.format(model))
        df: pd.DataFrame = pd.read_csv(f"{PATH}/data/processed/{model}.csv")
        df.to_sql(model, conn, if_exists="append", index=False)

        print('Dados inseridos com sucesso na tabela: {}'.format(model))

    conn.dispose()


