FROM puckel/docker-airflow
MAINTAINER Silvio Mori Neto <netomori@gtmail.com>

ADD . /usr/local/airflow/

RUN pip install boto3 apache-airflow-backport-providers-amazon