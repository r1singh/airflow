from airflow import DAG

from airflow.operators.sqlite_operator import SqliteOperator

from datetime import date, datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
        'owner' : 'unixuser'
    }

with DAG(
        dag_id = 'executing_sql_pipeline',
        description = 'Pipeline using SQL operators',
        default_args = default_args,
        start_date = days_ago(1),
        schedule_interval = '@once',
        tags = ['pipeline', 'sql']
    ) as dag:
        create_table = SqliteOperator(
            task_id = 'create_table',
            sql = r"""
                CREATE TABLE IF NOT EXISTS users (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR(50) NOT NULL,
                        age INTEGER NOT NULL,
                        city varchar(30),
                        is_active BOOLEAN DEFAULT true,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """,
            sqlite_conn_id = 'sqlite_conn_id',
            dag = dag,
        )

        insert_values1 = SqliteOperator(
                task_id='Inert_values_1',
                sql = r""" 
                       insert into users(name,age,is_active) values ('julle',23,true),
                       ('jay',26,false);""",
                sqlite_conn_id='sqlite_conn_id',
                dag=dag
        )
        insert_value2=SqliteOperator(
                task_id='Insert_values2',
                sql=r""" insert into users(name, age) values ('alan',35);""",
                sqlite_conn_id='sqlite_conn_id',
                dag=dag
        )
        delete_records=SqliteOperator(
                task_id='Delete_Records',
                sql="""delete from users where is_active=0;""",
                sqlite_conn_id='sqlite_conn_id',
                dag=dag
        )
        update_records=SqliteOperator(
                task_id='Update_record',
                sql="""update users set city='kalka';""",
                sqlite_conn_id='sqlite_conn_id',
                dag=dag
        )
        display_record=SqliteOperator(
                task_id='Display_Record',
                sql=""" select * from users;""",
                sqlite_conn_id='sqlite_conn_id',
                do_xcom_push=True,
                dag=dag
        )

create_table >> [insert_values1,insert_value2] >> delete_records >> update_records >> display_record


