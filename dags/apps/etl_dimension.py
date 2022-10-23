from pandas import DataFrame
import pandas as pd
from apps.utils.helpers import PATH
import unidecode 
from typing import List, Dict, Tuple
from datetime import datetime

class EtlDimension:

    def __init__(self) -> None:
        self.__processeddir = PATH + "/data/processed"
        self.__df_pnad: DataFrame = pd.read_csv(PATH + "/data/raw/pnad.csv", sep=",")
        self.__df_ibge: DataFrame = pd.read_csv(PATH + "/data/raw/ibge.csv", sep=",")

    @property
    def df_pnad_filter(self) -> DataFrame:
        """
        Metodo responsavel por filtrar os dados da PNAD

        :return: objeto DataFrame com os dados da PNAD filtrados
        """
        df_filter: DataFrame = self.__df_pnad.loc[
            (self.__df_pnad.sexo == 'Mulher') & 
            (self.__df_pnad.idade >= 20) & 
            (self.__df_pnad.idade <= 40)
            ]

        return df_filter
       

    def _limpa_txt(self) -> Tuple[DataFrame, DataFrame]:

        f = lambda x: unidecode.unidecode(str(x).strip().upper())

        df1 = self.df_pnad_filter.copy()
        df2 = self.__df_ibge.copy()

        f = lambda x: unidecode.unidecode(str(x).strip().upper())

        df1['uf'] = df1['uf'].apply(f)
        df1['cor'] = df1['cor'].apply(f)
        df1['sexo'] = df1['sexo'].apply(f)
        df1['graduacao'] = df1['graduacao'].apply(f)
        df1['trab'] = df1['trab'].apply(f)
        df1['ocup'] = df1['ocup'].apply(f)
        df2['nome'] = df2['nome'].apply(f)

        # criando a coluna com a data de hoje
        df1['data_criacao'] = datetime.now().date()
        df2['data_criacao'] = datetime.now().date()

        return df1, df2


    def _etl_regiao(self, df: DataFrame) -> DataFrame:

        dim_regioes = df[['regiao_id','regiao_nome', 'data_criacao']]
        dim_regioes = dim_regioes.drop_duplicates(subset=['regiao_nome'])

        return dim_regioes

    def _etl_estado(self, df: DataFrame) -> DataFrame:
        
        dim_estados = df[['id','sigla','nome','regiao_id', 'data_criacao']]
        dim_estados = dim_estados.drop_duplicates(subset=['nome'])

        return dim_estados


    def _etl_cor(self, df:DataFrame) -> DataFrame:

        dim_cores = df[['cor', 'data_criacao']]
        dim_cores = dim_cores.drop_duplicates(subset=['cor'])
        dim_cores['id_cor'] = dim_cores.index + 1

        return dim_cores

    def _etl_sexo(self, df:DataFrame) -> DataFrame:

        dim_sexos = df[['sexo', 'data_criacao']]
        dim_sexos = dim_sexos.drop_duplicates(subset=['sexo'])
        dim_sexos['id_sexo'] = dim_sexos.index + 1

        return dim_sexos

    def _etl_calendario(self) -> DataFrame:
        '''
        Funcao que cria a dimensao de calendario.
        :returns: dataframe com a dimensao de calendario
        '''

        # criando a dimensao de calendario
        dim_calendario: DataFrame = pd.DataFrame()
        dim_calendario['data'] = pd.date_range(start='1/1/2000', end='12/31/2030')

        # criando as colunas de ano, mes 
        dim_calendario['ano'] = dim_calendario['data'].dt.year
        dim_calendario['mes'] = dim_calendario['data'].dt.month

        # criando a coluna de trimestre
        f = lambda x: 1 if x <= 3 else 2 if x <= 6 else 3 if x <= 9 else 4
        dim_calendario['trimestre'] = dim_calendario['mes'].apply(f)

        # criando a coluna de id
        f = lambda x: int(str(x['trimestre'])+str(x['ano']))
        dim_calendario['id_calendario'] = dim_calendario.apply(f, axis=1)

        # removendo a coluna de mes
        dim_calendario = dim_calendario.drop(columns=['mes', 'data'])

        # removendo duplicadas
        dim_calendario = dim_calendario.drop_duplicates(subset=['id_calendario'])

        return dim_calendario


    def run(self):
        """
        Metodo responsavel por executar o processo de ETL
        """
        print('Recuperando os dados da camada raw..')
        print('Iniciando o ETL das dimensoes..')
        print('Limpando colunas')
        df_pnad, df_ibge = self._limpa_txt()
        
        print('Criando a dimensao de Regioes..')
        dim_regioes = self._etl_regiao(df_ibge)

        print('Criando a dimensao de Estados..')
        dim_estados = self._etl_estado(df_ibge)

        print('Criando a dimensao Cor..')
        dim_cores = self._etl_cor(df_pnad)

        print('Criando a dimensao de Tempo..')
        dim_tempo = self._etl_calendario()

        print('Salvando os dados..')
        dim_regioes.to_csv(PATH + '/data/processed/dim_regioes.csv', index=False)
        dim_estados.to_csv(PATH + '/data/processed/dim_estados.csv', index=False)
        dim_cores.to_csv(PATH + '/data/processed/dim_cores.csv', index=False)
        dim_tempo.to_csv(PATH + '/data/processed/dim_tempo.csv', index=False)
        df_pnad.to_csv(PATH + '/data/raw/pnad.csv', index=False)

        print('ETL das dimensoes finalizado com sucesso!')

    
