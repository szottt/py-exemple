import boto3
from datetime import datetime
import time

import os
os.chdir(r'C:\Users\4895860\OneDrive\Onedrive - GPA\Scripts\Modelos')

import config_pipeline


from solux_trusted import solux_trusted

def executar_emr(steps, emr_client, cluster_id):
    action = emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=steps)
    return action

def acompanhar_emr(action, emr_client, cluster_id, sleeptime = 5):
    step_id = action['StepIds']
    i = 0
    for s in step_id:
        step_state = emr_client.describe_step(ClusterId = cluster_id, StepId = s)

        print("Step", i + 1, "de", len(step_id), ":", step_state['Step']['Name'], "(", step_state['Step']['Id'], ')')

        step_status = step_state['Step']['Status']['State']

        while step_status not in ['FAILED', 'CANCELLED', 'INTERRUPTED', 'COMPLETED']:
            print(datetime.now().strftime('%d/%m/%Y %H:%M:%S'), '-', step_status, "(",i + 1, "/", len(step_id), ")")
            time.sleep(sleeptime)
            step_state = emr_client.describe_step(ClusterId = cluster_id, StepId = s)
            step_status = step_state['Step']['Status']['State']

        print(datetime.now().strftime('%d/%m/%Y %H:%M:%S'), '-', step_status, "(",i + 1, "/", len(step_id), ")")

        if step_status == 'FAILED':
            print(step_state['Step']['Status']['FailureDetails'])

        if step_status != 'CANCELLED':
            tempo_decorrido = step_state['Step']['Status']['Timeline']['EndDateTime'] - step_state['Step']['Status']['Timeline']['StartDateTime']
            print('Tempo decorrido:', tempo_decorrido)

        print('')
        i+=1

    return 0

emr_client = boto3.client('emr', region_name = 'us-east-1')

solux_trusted = executar_emr(solux_trusted, emr_client, config_pipeline.cluster_id)
acompanhar_emr(solux_trusted, emr_client, config_pipeline.cluster_id)

#cluster_id = emr_client.run_job_flow(**cluster_config)