from airflow.decorators import task, dag

from datetime import datetime, timedelta
from apps.crawlers import Crawler
from apps.etl_dimension import EtlDimension
from apps.etl_fact import EtlFact
from apps.utils.helpers import load_yaml, load_database
from typing import List
import boto3

_CONFIG: dict = load_yaml()


@task.python
def execute_crawler() -> None:

    _Crawler: Crawler = Crawler()
    
    _Crawler.main()


@task.python
def load_s3(file_name: str) -> None:

    global _CONFIG

    key: str = _CONFIG['s3']['AWS_KEY']
    secret: str = _CONFIG['s3']['AWS_SECRET_KEY']
    bucket: str = _CONFIG['s3']['BUCKET']

    _s3_client = boto3.client(
        's3',
        aws_access_key_id=key,
        aws_secret_access_key=secret
    )

    _s3_client.upload_file(
        f'/opt/airflow/data/raw/{file_name}.csv',
        bucket,
        f'{file_name}.csv'
    )

    _s3_client.close()

@task.python
def etl() -> None:

    _EtlDimension: EtlDimension = EtlDimension()
    _EtlDimension.run()

    _EtlFact: EtlFact = EtlFact()
    _EtlFact.run()

@task.python
def load_rds() -> None:

    global _CONFIG

    models: List[str] = [
        'dim_cores', 
        'dim_regioes',
        'dim_estados', 
        'dim_tempo', 
        'fato_pnads']

   
    load_database(
        host=_CONFIG['rds']['host'],
        user=_CONFIG['rds']['user'],
        password=_CONFIG['rds']['passwd'],
        database=_CONFIG['rds']['db'],
        models=models
    )

    print(f'Concluido!')


@dag(
    dag_id="dag_etl_pnad",
    description="Pipeline de dados de cargas em Datalake e Datawarehouse dos dados do PNAD", 
    start_date=datetime(2022, 9, 17),
    schedule_interval=None,
    tags=["meu teste", "teste"])
def dag_etl_pnad():

    execute_crawler() >> [load_s3(file_name="pnad"), load_s3(file_name="ibge")] >> etl() >> load_rds()


dag_etl_pnad = dag_etl_pnad()

    







