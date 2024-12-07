from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.models import Variable

default_args = {
    'owner' : 'unixuser'
}

def printthis(name):
	print('this is first code from python block {name}'.format(name=name))
	return 100
def printthis2(name, city,ti):
	print('this is first code from python block {name} - {city}'.format(name=name,city=city))
	xcomvalue=ti.xcom_pull(task_ids='Task1')
	return xcomvalue
def printthis3(ti):
	multi = Variable.get("Multi") 
	value=(multi * ti.xcom_pull(task_ids='Task2'))
	return value

with DAG(
    dag_id='Python_file',
    description = 'Our first "Hello World" DAG!',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily'
) as dag:

	task1 = PythonOperator(
	    task_id = 'Task1',
	    python_callable = printthis,
		op_kwargs={'name' : 'Rahul Choudhary'}
	)
	task2 = PythonOperator(
	    task_id = 'Task2',
	    python_callable = printthis2,
		op_kwargs={'name' : 'Rahul Choudhary' , 'city' : 'Kalka'}
	)
	task3 = PythonOperator(
	    task_id = 'Task3',
	    python_callable = printthis3
	)

task1 >> task2 >> task3