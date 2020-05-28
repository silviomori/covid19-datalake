import logging

import boto3
from airflow.hooks.S3_hook import S3Hook


def check_data_exists(bucket, prefix, file):
    logging.info('checking whether data exists in s3')
    source_s3 = S3Hook(aws_conn_id='aws_default')

    if not source_s3.check_for_bucket(bucket):
        raise Exception('Bucket not found:', bucket)

    if not source_s3.check_for_prefix(bucket, prefix, "/"):
        raise Exception('Prefix not found:', prefix)

    if not source_s3.check_for_key(prefix+'/'+file, bucket):
        raise Exception('File not found:', file)

    return f'File found: bucket: {bucket}, prefix: {prefix}, file: {file}'


def stop_airflow_containers(cluster):
    ecs = boto3.client('ecs')
    task_list = ecs.list_tasks(cluster=cluster)
    for task_arn in task_list['taskArns']:
        print('stopping task:', task_arn)
        ecs.stop_task(cluster=cluster, task=task_arn)
