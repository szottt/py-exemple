import boto3
from datetime import datetime, timedelta, date

client = boto3.client('datapipeline')


response = client.activate_pipeline(
    pipelineId='df-0250176Y120MJ51CSTJ',
    parameterValues=[
        {
            'id': 'df-0250176Y120MJ51CSTJ',
            'stringValue': 'trusted_reproc'
        },
    ],
    startTimestamp=datetime(2022, 1, 1)
)