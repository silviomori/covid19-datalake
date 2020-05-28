FROM puckel/docker-airflow
MAINTAINER Silvio Mori Neto <netomori@gtmail.com>

ADD dags/* /usr/local/airflow/dags/

RUN pip install boto3 apache-airflow-backport-providers-amazon