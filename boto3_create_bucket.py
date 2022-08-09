import boto3
s3 = boto3.resource('s3')

s3.meta.client.upload_file(r'C:\Users\4895860\Documents\tempo.sh', 'customer-intelligences', 'tmp/teste/teste/boto3_create_bucket.py')
