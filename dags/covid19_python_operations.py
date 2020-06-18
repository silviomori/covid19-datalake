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



def copy_us_data_file(bucket_origin, prefix_origin, bucket_dest, key_dest):
    """Copy source data file to a local bucket.

    Since the name of the file which contains US data changes very
    often, it may cause error in runtime. Thus, the file should be
    copied to a local bucket to avoid incurring errors.
    This function expects only one JSON file in the given origin.

    args:
    bucket_origin (str): name of the bucket which holds the source file
    prefix_origin (str): prefix in the bucket where the source file is
    bucket_dest (str): name of the bucket to store the file
    key_dest (str): prefix/name of the file in the destination bucket
    """
    logging.info('Coping US data file ' \
                 f'FROM: {bucket_origin}/{prefix_origin}/*.json ' \
                 f'TO: {bucket_dest}/{key_dest}')

    s3_hook = S3Hook(aws_conn_id='aws_default')

    # Get the object for the data source
    s3_obj = s3_hook.get_wildcard_key(prefix_origin+'/*.json', bucket_origin,
                                      delimiter='/')

    # Copy data file into s3 bucket
    s3_hook.load_file_obj(s3_obj.get()['Body'],
                          key=key_dest,
                          bucket_name=bucket_dest,
                          replace=True)

    logging.info('Data copy finished.')


def stop_airflow_containers(cluster):
    ecs = boto3.client('ecs')
    task_list = ecs.list_tasks(cluster=cluster)
    for task_arn in task_list['taskArns']:
        print('stopping task:', task_arn)
        ecs.stop_task(cluster=cluster, task=task_arn)
