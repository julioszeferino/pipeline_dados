import pandas as pd
from apps.utils.helpers import PATH
import unidecode 
from typing import List, Dict, Tuple
from datetime import datetime

class EtlFact:

    def __init__(self) -> None:
        self.__processeddir = PATH + "/data/processed"
        self.__df_pnad: DataFrame = pd.read_csv(PATH + "/data/raw/pnad.csv", sep=",")

    def _etl_pnad(self) -> Tuple[pd.DataFrame]:

        df_pnad = pd.read_csv("data/raw/pnad.csv", sep=",")
        dim_cores = pd.read_csv(PATH + "/data/processed/dim_cores.csv", sep=",")
        dim_estados = pd.read_csv(PATH + "/data/processed/dim_estados.csv", sep=",")
        dim_regioes = pd.read_csv(PATH + "/data/processed/dim_regioes.csv", sep=",")

        # buscando cores
        df = df_pnad.merge(dim_cores, left_on='cor', right_on='cor', how='left')
        # busca estados
        df = df.merge(dim_estados, left_on='uf', right_on='nome', how='left')
        # cria sk_tempo
        f = lambda x: int(str(x['trimestre'])+str(x['ano']))
        df['sk_tempo'] = df.apply(f, axis=1)

        # tratando as colunas
        df['graduacao'].replace({
            'SIM':1,
            'NAO': pd.NA,
            'NAN': pd.NA}, inplace=True)

        df['ocup'].replace({
            'PESSOAS OCUPADAS':1,
            'PESSOAS DESOCUPADAS': pd.NA,
            'NAN': pd.NA}, inplace=True)

        # criando a fato_pnads
        mask = ['sk_tempo', 'id_cor', 'id']
        fato_pnad = df.groupby(mask, as_index=False).agg(
            {
            '_id':'count',
            'graduacao': 'sum',
            'ocup': 'sum',
            'renda': 'mean',
            'horastrab': 'mean'})

        # tratando nulos
        fato_pnad.fillna(0, inplace=True)

        # renomeando colunas
        fato_pnad.rename(columns={
        'id_cor': 'sk_cor',
        'id': 'sk_estado',
        '_id': 'qtd_pessoas',
        'graduacao': 'qtd_graduacao',
        'ocup': 'qtd_ocupados',
        'renda': 'renda_media',
        'horastrab': 'horastrab_media'}, inplace=True)

        fato_pnad['id'] = fato_pnad.index + 1

        fato_pnads: pd.DataFrame = fato_pnad

        return fato_pnads

    def run(self) -> None:
        
        print('Recuperando os dados das dimensoes..')
        print('Iniciando o ETL das fatos..')
        fato_pnads = self._etl_pnad()

        print('Salvando os dados..')
        fato_pnads.to_csv(PATH + "/data/processed/fato_pnads.csv", index=False)
                


       