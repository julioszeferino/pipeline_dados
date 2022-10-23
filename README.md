# Criacao de um Pipeline de Dados

> Projeto pratico em modo de desafio com a criacao de um pipeline de dados completo desenvolvido como atividade avaliativa final do Bootcamp Engenheiro de Dados da XPEducacao.

O pipeline foi criado a partir dos requisitos propostos pelo [desafio](docs/media/desafio_xp.pdf).

O projeto extrai dados da **API** de regioes do IBGE e de um cluster **MongoDB** com os dados da PNAD 2020

Os dados em sua camada raw sao colocados em um bucket da **AWS S3** que servira como **Data Lake**.

Por fim, os dados sao filtrados e processados de acordo com as regras de negocio, modelados em formato dimensional e inseridos em um servidor **PostgreSQL** criado no **AWS RDS** e que servira como **Data Warehouse**.  

## **Ambiente de Desenvolvimento**

[Docker 20.10.17](https://www.docker.com/)   

## **Arquitetura de Dados**
![arquitetura](docs/media/arquitetura.png)

## **Como Executar este Projeto**

1. Crie uma instancia no `AWS EC2` ou outro provedor da sua escolha, realize o download deste repositorio e execute o docker-compose para realizar o build dos containers:  
```bash
docker-compose up -d
```  
2. Criar um bucket no `AWS S3` com o nome *data-lake-pnad*.  
3. Crie um servidor de banco de dados `PostgreSQL` utilizando a `AWS RDS`. Alem disso, crie o database `dw_pnad`.  
4. Adicione as credenciais de acesso da API e do cluster MongoDB no arquivo `./conf/config.yaml`.  
5. Adicione as credenciais da aws para acesso ao S3 e os dados de acesso ao banco de dados no arquivo `./conf/config.yaml`.  
6. Crie as tabelas no banco de dados executando os comandos do arquivo `databases.sql`.  
7. Executar o pipeline de dados no `Apache Airflow`.  

## **Referencias**

A documentacao completa do projeto esta disponivel neste link: https://julioszeferino.github.io/pipeline_dados/ 

## Histórico de Atualizações

* 0.0.1
    * Projeto Inicial

## Direitos de Uso
A ideia deste repositório é treinar os conceitos de Pipelines de Dados e compartilhar conhecimento. Dessa forma, você pode replicar e utilizar o conteúdo deste repositório sem nenhuma restrição desde que forneça uma atribuição de volta e não me responsabilize por quaisquer reclamações, danos ou responsabilidades.  

Exigido | Permitido |Proibido
:---: | :---: | :---:
Aviso de licença e direitos autorais | Uso comercial | Responsabilidade Assegurada
 || Modificação ||
 || Distribuição || 
 || Sublicenciamento ||

## Meta

Julio Zeferino - [@Linkedin](https://www.linkedin.com/in/julioszeferino/) - julioszeferino@gmail.com
[https://github.com/julioszeferino] 
