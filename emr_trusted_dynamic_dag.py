from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
import json
import boto3

arquivo_config = '{{params.arquivo_config}}'
# env = Variable.get("env")  # isso é uma variavel dentro do Airflow
env = 'prd'
data_atualizacao = datetime.now() - timedelta(hours=3)
data_atualizacao = data_atualizacao.strftime('%d/%m/%Y')
cluster_id = Variable.get("cluster_id")

bucket = f'customer-intelligence-{env}'
arqs_trusted = 'data/archive/variaveis_airflow/arq_trusted.json'

s3 = boto3.resource('s3')
obj = s3.Object(bucket, arqs_trusted)
arqs_movimentacao = json.load(obj.get()['Body'])


def create_dag_Mongo(dag_id,
                     schedule,
                     dag_number,
                     default_args,
                     tags,
                     catchup,
                     params
                     ):

    steps_exec_raw = [
        {
            'Name': f'{dag_id}_{data_atualizacao}',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit',
                         '--deploy-mode', 'cluster',
                         '--packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1',
                         f's3://customer-intelligence-{env}/jobs/{dag_number}.py',
                         '--env', env,
                         '--env_mongo', env,
                         '--data_atualizacao', data_atualizacao,
                         '--arquivo_config', arquivo_config
                         ]
            }
        }]

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args,
              start_date=datetime(2022, 5, 15, 20),
              tags=tags,
              catchup=catchup,
              params=params,
              )

    with dag:

        exec_raw = EmrAddStepsOperator(
            task_id='exec_raw',
            job_flow_id=cluster_id,
            aws_conn_id='aws_default',
            steps=steps_exec_raw,
        )

        monitora_steps_exec_raw = EmrStepSensor(
            task_id='monitora_import_amigavel',
            job_flow_id=cluster_id,
            step_id="{{ task_instance.xcom_pull(task_ids='exec_raw', key='return_value')[0] }}",
            aws_conn_id='aws_default',
        )

        exec_raw >> monitora_steps_exec_raw

    return dag


def create_dag(dag_id,
               schedule,
               dag_number,
               default_args,
               tags,
               catchup,
               params
               ):

    steps_exec_raw = [
        {
            'Name': f'{dag_id}_{data_atualizacao}',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit',
                         '--deploy-mode', 'cluster',
                         f's3://customer-intelligence-{env}/jobs/{dag_number}.py',
                         '--env', env,
                         '--data_atualizacao', data_atualizacao,
                         '--arquivo_config', arquivo_config
                         ]
            }
        }]

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args,
              start_date=datetime(2022, 5, 15, 20),
              tags=tags,
              catchup=catchup,
              params=params,
              )

    with dag:

        exec_raw = EmrAddStepsOperator(
            task_id='exec_raw',
            job_flow_id=cluster_id,
            aws_conn_id='aws_default',
            steps=steps_exec_raw,
        )

        monitora_steps_exec_raw = EmrStepSensor(
            task_id='monitora_import_amigavel',
            job_flow_id=cluster_id,
            step_id="{{ task_instance.xcom_pull(task_ids='exec_raw', key='return_value')[0] }}",
            aws_conn_id='aws_default',
        )

        exec_raw >> monitora_steps_exec_raw

    return dag


default_args = {
    'owner': 'Dynamic',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

for arq in arqs_movimentacao:
    dag_id = f'{arqs_movimentacao[str(arq)][0]}_{arq}_trusted'
    schedule = f"{arqs_movimentacao[str(arq)][1]}"
    dag_number = arqs_movimentacao[str(arq)][0]
    tags = ['DYNAMIC', 'TRUSTED', 'PRD']
    catchup = False
    params = {
        "arquivo_config": f"s3://customer-intelligence-{env}/config/config.json"}

    if arqs_movimentacao[str(arq)][2] == "Mongo":
        globals()[dag_id] = create_dag_Mongo(dag_id,
                                             schedule,
                                             dag_number,
                                             default_args,
                                             tags,
                                             catchup,
                                             params
                                             )
    else:
        globals()[dag_id] = create_dag(dag_id,
                                       schedule,
                                       dag_number,
                                       default_args,
                                       tags,
                                       catchup,
                                       params
                                       )
