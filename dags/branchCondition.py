from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator,BranchPythonOperator
from random import choice

def result_set():
    return choice([True,False])
def branch_check(ti):
    result= ti.xcom_pull(task_ids='result_id')
    if result:
        return 'drive'
    else:
        return 'notdrive'
def candrive():
    return 'Yes, you can drive, you have valid driving license'
def notdrive():
    return 'No, you dont have any valid lisence'
default_arg={'owner':'unixuser'}
with DAG(
    dag_id='Branching_Condition',
    description='This will check and clear the branching',
    schedule_interval='@once',
    start_date= days_ago(1),
    default_args=default_arg
) as dag:
    result=PythonOperator(
        task_id='result_id',
        python_callable=result_set
    )
    branch=BranchPythonOperator(
        task_id='branch',
        python_callable=branch_check
    )
    drive=PythonOperator(
        task_id='drive',
        python_callable=candrive
    )
    notdrive=PythonOperator(
        task_id='not_drive',
        python_callable= notdrive
    )
result >> branch >> [drive,notdrive]