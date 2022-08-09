from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
from airflow.models import Variable
from datetime import datetime
import time


def executar_emr(steps, emr_client, cluster_id):
    action = emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=steps)
    return action


def main():
    cluster_id = 'j-1IEGGW7IQ8XQO'
    env = 'dev'
    arquivo_config = 's3://customer-intelligences/{}/jobs/config/config.json'.format(env)
    data_atualizacao = '08/01/2022'
    
    solux_trusted = [
        {
            'Name': 'Solux-Trusted',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', 
                            '--deploy-mode', 'cluster',
                            's3://customer-intelligences/{}/jobs/BI03DJ11.py'.format(env),
                            '--env', env,
                            '--data_atualizacao',data_atualizacao,
                            '--arquivo_config', arquivo_config]   
            }     
        }
    ]
    
    
    emr_client = boto3.client('emr', region_name = 'us-east-1')

    solux_trusted = executar_emr(solux_trusted, emr_client, cluster_id)



with DAG(dag_id="emr_exec_2_dag",
         start_date=datetime(2022,1,1),
         schedule_interval=None,
         catchup=False) as dag:
    
    task1 = PythonOperator(
            task_id="emr_exec",
            python_callable=main)

    
task1