from datetime import timedelta,datetime  
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.dates import days_ago

import pandas as pd

def readfun():
    df=pd.read_csv('/home/unixuser/airflow/datasets/insurance.csv')
    print()
    return df.to_json()

def remove_null(**kwargs):
    ti=kwargs['ti']
    df=pd.read_json(ti.xcom_pull(task_ids='Read_call'))
    df=df.dropna()
    print(df)
    return df.to_json()

def output_file(**kwargs):
    ti=kwargs['ti']
    df=pd.read_json(ti.xcom_pull(task_ids='Remove_null'))
    smokerGroup= df.groupby('smoker').agg(
        {'age': 'mean',
         'bmi':'mean',
         'charges': 'mean'
         }
    ).reset_index()

    print(smokerGroup)
    smokerGroup.to_csv('/home/unixuser/airflow/dags/output/smokerGroup.csv', index=False)
def region_data(**kwargs):
    ti=kwargs['ti']
    df=pd.read_json(ti.xcom_pull(task_ids='Remove_null'))
    region=df.groupby('region').agg({
        'age' : 'mean',
        'bmi' : 'mean',
        'charges' : 'mean'
    }).reset_index()
    print(region)
    region.to_csv('/home/unixuser/airflow/dags/output/region.csv', index=False)

with DAG(
    dag_id='Data_transform',
    description='Read from CSV file',
    start_date=days_ago(1),
    schedule_interval='@Daily'
) as dag :
    
    task1=PythonOperator(
        task_id='Read_call',
        python_callable = readfun
    )
    task2=PythonOperator(
        task_id='Remove_null',
        python_callable= remove_null
    )
    task3=PythonOperator(
        task_id='Smoker_group',
        python_callable= output_file
    )
    task4 =PythonOperator(
        task_id='region',
        python_callable=region_data
    )

    task1>> task2 >> [task3,task4]

