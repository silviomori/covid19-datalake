import boto3
from airflow.operators.python_operator import PythonOperator

class TerminationOperator(PythonOperator):
    """Operator to stop all the tasks running in the ECS cluster"""

    def __init__(self, task_id, dag):
        super(TerminationOperator, self).__init__(task_id=task_id, dag=dag, python_callable=self.termination)
    
    def termination(self):
        ecs = boto3.client('ecs')
        task_list = ecs.list_tasks(cluster='covid-cluster')
        for task_arn in task_list['taskArns']:
            print('stopping task:', task_arn)
            ecs.stop_task(cluster='covid-cluster', task=task_arn)

