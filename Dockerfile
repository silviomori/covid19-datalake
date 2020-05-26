FROM puckel/docker-airflow
MAINTAINER Silvio Mori Neto <netomori@gtmail.com>
ADD dags/* /usr/local/airflow/dags/
