import os
import subprocess
import s3fs
import boto3
import json
from argparse import ArgumentParser
from datetime import date, datetime, timedelta
from pytz import timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


def extract(appsflyer):

    lambda_client = boto3.client('lambda', region_name='us-east-1')

    response = lambda_client.invoke(
        FunctionName='ecd-eng-aflyer-org-events',
        Payload=json.dumps(appsflyer),
    )


def criar_sessao_spark():

    spark = SparkSession \
        .builder \
        .getOrCreate()

    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic'),
    spark.conf.set('spark.sql.shuffle.partitions', 10000)

    return spark


def main():

    parser = ArgumentParser()

    parser.add_argument("--env",
                        choices=['dev', 'hlg', 'prd'],
                        default='dev',
                        help="Ambiente do pipeline")

    parser.add_argument("--data_atualizacao",
                        type=lambda s: timezone(
                            "America/Sao_Paulo").localize(datetime.strptime(s, '%d/%m/%Y')).date(),
                        default=date.today() - timedelta(days=1),
                        help="Data de execucao do pipeline")

    parser.add_argument("--arquivo_config",
                        type=str,
                        help="Arquivo de configuracao")

    args = parser.parse_args()

    fs = s3fs.S3FileSystem()

    startDate = args.data_atualizacao - timedelta(days=1)

    endDate = args.data_atualizacao - timedelta(days=1)

    with fs.open(args.arquivo_config.format(args.env), 'r') as f:
        config = json.load(f)

    path_file = config[''].format(startDate.strftime('%Y%m%d'))
    path_raw = config[''].format(args.env)
    bucket = path_raw.split('/')[2]

    api = "https://"

    api_url = api.format(startDate.strftime('%Y-%m-%d'),
                         endDate.strftime('%Y-%m-%d'))

    appsflyer = {
        "file_path": "{}".format(path_file),
        "api_url": "{}".format(api_url),
        "bucket": "{}".format(bucket),
    }

    spark = criar_sessao_spark()

    if not fs.exists(path_file):
        extract(appsflyer)
    else:
        raise FileNotFoundError("Falha na extração com Lambda: ")

    print('passou extract')

    schema = StructType([
        StructField("Is_LAT", StringType(), True),
    ])

    df_file = spark.read \
        .option("header", "true") \
        .option("sep", ",") \
        .option("escape", '\"') \
        .csv("s3://-" + args.env + "/"+path_file, schema=schema)\
        .withColumn('data_atualizacao', F.lit(startDate))

    print(df_file.show())

    df_file.write\
        .mode('overwrite')\
        .partitionBy('data_atualizacao')\
        .parquet(path_raw)


if __name__ == '__main__':
    main()
