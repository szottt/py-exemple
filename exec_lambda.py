import os
import subprocess
import s3fs
import boto3, json
from argparse import ArgumentParser
from datetime import date, datetime, timedelta
from pytz import timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

def extract(soluxDetalhamentoNota):
    
    lambda_client = boto3.client('lambda', region_name='us-east-1')
    
    response = lambda_client.invoke(
      FunctionName='ecd-eng-solux-det-nota',
      Payload=json.dumps(soluxDetalhamentoNota),
    )

def criar_sessao_spark():

    spark = SparkSession \
        .builder \
        .getOrCreate()
        
    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

    return spark

def parquetSolux(spark,file,path_raw,startDate):
    
    multiline_df = spark.read.option("multiline", "true").json(file)
    
    df_file_Tudo = multiline_df.withColumn('DATA_ATUALIZACAO', F.lit(startDate))
    
    print(df_file_Tudo.show())
    
    df_file_Tudo.write\
        .mode('overwrite')\
        .partitionBy('DATA_ATUALIZACAO')\
        .parquet(path_raw)
    

def main():

    parser = ArgumentParser()
    
    parser.add_argument("--env",
        choices=['dev', 'hlg', 'prd'],
        default = 'dev',
        help="Ambiente do pipeline")  
    
    parser.add_argument("--data_atualizacao", 
        type=lambda s: timezone("America/Sao_Paulo").localize(datetime.strptime(s, '%d/%m/%Y')).date(),
        default = date.today() - timedelta(days = 1) ,
        help="Data de execucao do pipeline")
    
    parser.add_argument("--arquivo_config", 
        type=str, 
        help="Arquivo de configuracao")

    args = parser.parse_args()

    fs = s3fs.S3FileSystem()
    
    startDate = args.data_atualizacao - timedelta(days=1)
	
    endDate = args.data_atualizacao
    
    print(startDate)
    print(endDate)
    
    with fs.open(args.arquivo_config.format(args.env), 'r') as f:
        config = json.load(f)
            
    path_file = config['RAW_SOLUX_DETALHAMENTO_OUTPUT'].format(startDate.strftime('%Y%m%d'))
    path_raw = config['RAW_SOLUX_DETALHAMENTO']
    file = 's3://customer-intelligences/{}'.format(path_file)
   
    
    soluxDetalhamentoNota = {
                        "StartDate": "{}".format(startDate),
                        "EndDate": "{}".format(endDate),
                        "pasta_s3_file":"{}".format(path_file)
                     }

    spark = criar_sessao_spark()
    
    extract(soluxDetalhamentoNota)
    
    print('passou extract')
    
    parquetSolux(spark,file,path_raw,startDate)
    
if __name__ == '__main__':
    main()