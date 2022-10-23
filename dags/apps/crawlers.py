# importando as bibliotecas
import json 
from pymongo import MongoClient
import requests
from requests.models import Response
from typing import List, Dict, Optional
import pandas as pd
from pandas import DataFrame
from apps.utils.helpers import load_yaml, PATH

# parametrizacao das variaveis
_CONFIG: dict = load_yaml()
_CONN_PNAD: Dict[str, str] = _CONFIG['pnad_conn']
_CONN_IBGE: Dict[str, str] = _CONFIG['ibge_conn']

# parametrizacao das conexoes
_MONGO_URI: str = f"mongodb+srv://{_CONN_PNAD['user']}:{_CONN_PNAD['passwd']}@{_CONN_PNAD['server']}"
_MONGO_DB: str = _CONN_PNAD['db']
_MONGO_COLLECTION: str = _CONN_PNAD['collection']
_API_IBGE: str = _CONN_IBGE['api']


class Crawler:

    def __init__(self) -> None:
        self.__path_data: str = PATH + "/data/raw"
    
    def _get_regiao(self, estados: dict) -> List[dict]:
        """
        Funcao que realiza o parse dos dados de regiao dos estados brasileiros
        do arquivo JSON requisitado da API do IBGE

        :param estados: dicionario com os dados das regioes dos estados brasileiros
        """
        for linha in estados:

            linha['regiao_id'] = linha['regiao']['id']
            linha['regiao_sigla'] = linha['regiao']['sigla']
            linha['regiao_nome'] = linha['regiao']['nome']
            linha.pop('regiao')

        return estados


    def get_data_pnad(self) -> DataFrame:
        """
        Metodo responsavel por fazer a requisicao dos dados da PNAD em um servidor MongoDB

        :return: objeto DataFrame com os dados da PNAD
        """
        try:
            client = MongoClient(_MONGO_URI)

            db = client[_MONGO_DB]
            collection = db[_MONGO_COLLECTION]

            df: DataFrame = pd.DataFrame(list(collection.find()))

            if df.shape[0] > 0:
                return df
            else:
                print("Nao ha dados na base de dados")
                return None

        except Exception as e:
            print(f"Nao foi possivel conectar ao banco de dados: \n{e}")
            return None


    def get_data_ibge(self) -> DataFrame:
        """
        Metodo que requisita dados da API de regioes do IBGE.

        :return: objeto DataFrame com os dados da API do IBGE
        """
        try:
            response: Response = requests.get(_API_IBGE)
            response: List[dict] = json.loads(response.content)

            regioes: List[dict] = self._get_regiao(response)

            df: DataFrame = pd.DataFrame(regioes)

            if df.shape[0] > 0:
                return df
            else:
                print("Nao ha dados na base de dados")
                return None

        except Exception as e:
            print(f"Nao foi possivel conectar a API do IBGE: \n{e}")
            return None


    def main(self) -> None:
        """
        Funcao que executa o crawler de dados da PNAD e da API do IBGE
        e salva os dados na pasta data/raw.
        """
        print('Requisitando os dados da PNAD..')
        df_pnad: Optional[pd.DataFrame] = self.get_data_pnad()

        print('Requisitando os dados do IBGE..')
        df_ibge: Optional[pd.DataFrame] = self.get_data_ibge()

        print('Salvando os dados em arquivos CSV..')
        df_pnad.to_csv(self.__path_data + f"/pnad.csv", index=False)  
        df_ibge.to_csv(self.__path_data + f"/ibge.csv", index=False)
        

        print('Dados salvos com sucesso..')
        print('Requisicoes concluidas com sucesso..')




