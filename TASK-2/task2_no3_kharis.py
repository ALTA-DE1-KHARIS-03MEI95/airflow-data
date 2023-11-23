from airflow import DAG
from datetime import datetime
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
import json

with DAG(
    dag_id="day_two_task_number_three",
    schedule=None,
    start_date=datetime(2022, 10, 21),
    catchup=False,
) as dag:
    create_table_in_db_task = PostgresOperator(
        task_id="create_table_in_db",
        sql=(
            "CREATE TABLE IF NOT EXISTS gender_name_prediction "
            + "("
            + "input JSONB, "
            + "details JSONB, "
            + "result_found BOOLEAN, "
            + "first_name TEXT, "
            + "probability NUMERIC, "
            + "gender TEXT, "
            + "timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP "
            + ")"
        ),
        postgres_conn_id="pg_conn_id",
        autocommit=True,
        dag=dag,
    )

    def loadDataToPostgres():
        pg_hook = PostgresHook(postgres_conn_id="pg_conn_id").get_conn()
        curr = pg_hook.cursor()

        prediction_result = [
            {
                "input": {"first_name": "sandra", "country": "US"},
                "details": {
                    "credits_used": 1,
                    "duration": "13ms",
                    "samples": 9273,
                    "country": "US",
                    "first_name_sanitized": "sandra",
                },
                "result_found": True,
                "first_name": "Sandra",
                "probability": 0.98,
                "gender": "female",
            }
        ]

        for result in prediction_result:
            sql = "INSERT INTO gender_name_prediction (input, details, result_found, first_name, probability, gender) VALUES (%s, %s, %s, %s, %s, %s)"
            params = (
                json.dumps(result["input"]),
                json.dumps(result["details"]),
                result["result_found"],
                result["first_name"],
                result["probability"],
                result["gender"],
            )
            curr.execute(sql, params)
        pg_hook.commit()


    load_data_to_db_task = PythonOperator(
        task_id="load_data_to_db", python_callable=loadDataToPostgres, dag=dag
    )

    create_table_in_db_task >> load_data_to_db_task
