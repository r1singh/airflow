from datetime import timedelta,datetime
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow import DAG
import requests as req
import pandas as pd
import sqlite3 

deafult_args={'owner' : 'unixuser'}

def API_call(ti):
    preoffset=Variable.get('preoffset')
    url="https://api.eia.gov/v2/electricity/rto/daily-region-data/data/?frequency=daily&data[0]=value&facets[respondent][]=AEC&facets[type][]=D&facets[type][]=DF&facets[type][]=NG&facets[type][]=TI&sort[0][column]=period&sort[0][direction]=desc&offset="+str(preoffset)+"&length=1000&api_key=WJCdtpf7cqm6q9JGNnEFrvVEqO8Vbf4CkiCNghy4"
    response=req.get(url).json()
    df=pd.DataFrame(response['response']['data'])
    df= df.rename(columns={'period': "Date_Time", 'respondent':'Organization',	'respondent-name':'Name',	'type':'Group_',	'type-name':'Group_Type',	'timezone':'TimeZone',	'timezone-description':'timezone_description',	'value':'Electricity_Consumed',	'value-units':'Unit'})
    return df.to_json()
def update_offset():
    updatevalue=int(Variable.get('preoffset')) + 100
    Variable.set('preoffset', updatevalue)
def saveCSV(ti):
    data=ti.xcom_pull(task_ids='API_CALL')
    df=pd.read_json(data)
    df['value']=pd.to_numeric(df['Electricity_Consumed'])
    df['period']=pd.to_datetime(df['Date_Time'])
    df.to_csv("/home/unixuser/airflow/dags/output/EAI.CSV", index=False, mode='a',header=True)
def update_database():
    df=pd.read_csv("/home/unixuser/airflow/dags/output/EAI.CSV")
    conn=sqlite3.connect('my_sqlite.db')
    instance=conn.cursor()
    instance.execute(r'''
                     CREATE TABLE IF NOT EXISTS EAI(
                     Date_Time timestamp,
                     Organization varchar(10),
                     Name varchar(15),
                     Group_ varchar(15),
                     Group_Type varchar(15),
                     TimeZone varchar(15),
                     timezone_description varchar(15),
                     Electricity_Consumed integer,
                     Unit varchar(15)
                     );''')
    
    df.to_sql('EAI',conn,if_exists='replace')
    conn.commit()
    conn.close()


with DAG(
    dag_id='ETL_Call_Dag',
    description = 'this dag will collect data from api and convert it to csv file and from csv data is being pushed to sql.',
    start_date=days_ago(0),
    schedule_interval="@daily"
) as dag:
    
    callAPI= PythonOperator(
        task_id='API_CALL',
        python_callable=API_call
    )
    CSVfile=PythonOperator(
        task_id='CSV_SAVE',
        python_callable=saveCSV

    )
    offset=PythonOperator(
        task_id='offset_call',
        python_callable= update_offset
    )
    createtable=PythonOperator(
        task_id='create_table',
        python_callable=update_database
    )
callAPI>>CSVfile>>offset >>createtable
