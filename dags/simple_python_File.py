from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'unixuser'
}

with DAG(
    dag_id='hello_world',
    description = 'Our first "Hello World" DAG!',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily'
) as dag:

	task1 = BashOperator(
	    task_id = 'hello_world_task',
	    bash_command = 'echo Hello World using "with"!'
	)
	task2=BashOperator(
		task_id='helow_from_task_2',
		bash_command = 'echo hellow from task 2'
    )
	task3=BashOperator(
		task_id='helo_from_task_3',
		bash_command='ls -l'
    )
	task4=BashOperator(
		task_id='helo_from_task4',
		bash_command='echo task 4'
    )

task1 >> [task2, task3]
task4 << [task2,task3]