from airflow import DAG
from datetime import datetime
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator

with DAG(
    dag_id = 'day_two_task_number_two',
    schedule=None,
    start_date=datetime(2022, 10, 21),
    catchup=False
) as dag:
    
     create_table_in_db_task = PostgresOperator(
        task_id = 'create_table_in_db',
        sql = ('CREATE TABLE IF NOT EXISTS gender_name_prediction ' +
        '(' +
            'input JSONB, ' +
            'details JSONB, ' +
            'result_found BOOLEAN, ' +
            'first_name TEXT, ' +
            'probability NUMERIC, ' +
            'gender TEXT, ' +
            'timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP' +
        ')'),
        postgres_conn_id='pg_conn_id', 
        autocommit=True,
        dag=dag
    )