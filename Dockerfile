FROM puckel/docker-airflow
MAINTAINER Silvio Mori Neto <netomori@gtmail.com>

ADD dags/* /usr/local/airflow/dags/
ADD plugins/operators/* /usr/local/airflow/plugins/operators/

RUN pip install boto3