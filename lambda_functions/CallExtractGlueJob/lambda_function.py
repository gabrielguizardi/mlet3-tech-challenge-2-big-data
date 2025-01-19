import json
import boto3

def lambda_handler(event, context):
    glue = boto3.client('glue')
    try:
        response = glue.start_job_run(JobName='extract')
        
        job_run_id = response['JobRunId']
        print(f"Started Glue job 'extract' with JobRunId: {job_run_id}")
        
        return {
            'statusCode': 200,
            'body': f"Started Glue job 'extract' with JobRunId: {job_run_id}"
        }
    except Exception as e:
        print(f"Error starting Glue job: {e}")
        return {
            'statusCode': 500,
            'body': f"Error starting Glue job: {e}"
        }
