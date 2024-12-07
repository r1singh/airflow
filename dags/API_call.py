from datetime import timedelta,datetime 
from airflow.operators.python import PythonOperator
from  airflow.providers.http.operators.http import SimpleHttpOperator
from airflow import DAG
from airflow.utils.dates import days_ago

default_args={'owner' : 'uixuser'}

with DAG(
    dag_id='api_call',
    description ='The dag is created to call an api through simple http operator',
    schedule_interval = '@daily',
    catchup =True,
    start_date =days_ago(1)
) as dag:
    """function is called to test the API connection which is configured in airflow web ui"""
    callapi=SimpleHttpOperator(
        task_id='simple_API_Call',
        method='Get',
        http_conn_id='API_conn_id',
        headers={'content':'application/josn'},
        dag=dag
    )
callapi






