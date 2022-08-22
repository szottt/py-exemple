import urllib.request
import boto3
import json
import urllib3


def lambda_handler(event, context):

    # VAR
    caminho_do_arquivo_local = ""
    nome_do_arquivo_local = ""

    nome_do_s3 = event['bucket']
    pasta_s3_file = event['file_path']
    url_api = event['api_url']
    with urllib.request.urlopen(url_api) as url:
        s = url.read().decode('utf-8')
        s = str(s).replace("b'A", 'A')
       # print(s)
    with open(caminho_do_arquivo_local, "w") as txtfile:
        print("{}".format(s), file=txtfile)
    s3 = boto3.client('s3')
    with open(caminho_do_arquivo_local, "rb") as f:
        s3.upload_fileobj(f, nome_do_s3, pasta_s3_file)
