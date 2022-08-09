from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
from airflow.models import Variable

bucket_orig = Variable.get("bucket_orig") #isso é uma variavel dentro do Airflow
bucket_dest = Variable.get("bucket_dest") #isso é uma variavel dentro do Airflow
orig_name_file = Variable.get("orig_name_file") #isso é uma variavel dentro do Airflow
dest_name_file = Variable.get("dest_name_file2") #isso é uma variavel dentro do Airflow

def MoveFileBoto():
    
    s3 = boto3.resource('s3')
    copy_source = {
        'Bucket': f'{bucket_orig}',
        'Key': f'{orig_name_file}'
    }
    s3.meta.client.copy(copy_source, f'{bucket_dest}', f'{dest_name_file}')

with DAG(dag_id="move_file2_dag",
        start_date=datetime(2022,3,1,2,0,0),
        schedule_interval=timedelta(days=1,hours=3,minutes=30),
        dagrun_timeout=timedelta(seconds=5),         
        catchup=False) as dag:
    
    task1 = PythonOperator(
            task_id="move_file",
            python_callable=MoveFileBoto)

    
task1