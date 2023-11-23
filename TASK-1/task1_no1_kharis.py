from datetime import datetime # mengimport modul datetime dari library datetime
from airflow import DAG # mengimport class DAG dari library airflow
from airflow.operators.python_operator import PythonOperator # mengimport class PythonOperator dari modul airflow.operators.python_operator. Class ini digunakan untuk menjalankan fungsi Python sebagai bagian dari DAG

'''
Mendefinisikan fungsi print_task1 yang nantinya mencetak string 'Task number 1: Create DAG run in every 5 hours'
'''
def print_task1():
    return 'Task number 1: Create DAG run in every 5 hours'

dag = DAG( 
        'task_number_one', # membuat DAG dengan nama task_number_one
        description='task number 1', # dengan deskripsi task number 1
        schedule_interval='0 */5 * * * *', # DAG akan dijalankan setiap 5 jam sekali
        start_date=datetime(2022, 10, 21), # Yang dimulai dari tanggal 21 oktober 2022
        catchup=False
    )

operator_task_1 = PythonOperator(  # membuat instance operator PythonOperator
    task_id='task_1', # dengan task_id task_1
    python_callable=print_task1, # fungsi yang dipanggil print_task1
    dag=dag
)

operator_task_1


