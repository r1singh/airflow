from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'unixuser'
}

with DAG(
    dag_id='executing_bash_files',
    description = 'dag with multiple bash files',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
	template_searchpath='/home/unixuser/airflow/dags/bash_scripts'
) as dag:

	taskA = BashOperator(
	    task_id = 'TaskA',
	    bash_command = 'TaskA.sh'
	)
	taskB=BashOperator(
		task_id='TaskB',
		bash_command = 'TaskB.sh'
    )
	taskC=BashOperator(
		task_id='TaskC',
		bash_command='TaskC.sh'
    )
	taskD=BashOperator(
		task_id='TaskD',
		bash_command='TaskD.sh'
    )
	taskE=BashOperator(
		task_id='TaskE',
		bash_command='TaskE.sh'
    )
	taskF=BashOperator(
		task_id='TaskF',
		bash_command='TaskF.sh'
    )
	taskG=BashOperator(
		task_id='TaskG',
		bash_command='TaskG.sh'
    )

taskA
taskB
taskE
taskC
taskF
taskD 
taskG