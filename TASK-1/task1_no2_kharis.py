from datetime import datetime # mengimport modul datetime dari library datetime
from airflow import DAG # mengimport class DAG dari library airflow
from airflow.decorators import task # mengimport class PythonOperator dari modul airflow.operators.python_operator. Class ini digunakan untuk menjalankan fungsi Python sebagai bagian dari DAG

with DAG(
    'task_number_two', # membuat DAG dengan nama task_number_two
    description='Task number 2',  # dengan deskripsi task number 2
    schedule_interval=None, # DAG akan dijalankan, tanpa interval
    start_date=datetime(2022, 10, 21), # Yang dimulai dari tanggal 21 oktober 2022
    catchup=False
) as dag: # DAG disimpan dalam variable dag
    # ti = task instance
    @task # dekorator. dekorator task digunakan untuk mendefinisikan tugas dalam DAG
    def push_var_from_task_a(ti=None): # mendefinisikan fungsi, yang menerima satu argumen
        ti.xcom_push(key='task_two', value='define a new task that push a variable to xcom') # fungsi ini mem-push varible xcom dengan key task_two dan value 'define a new task that push a variable to xcom'
    
    @task
    def get_var_from_task_a(ti=None): # mendefinisikan fungsi, yang menerima satu argumen
        task_two = ti.xcom_pull(task_ids='push_var_from_task_a', key='task_two') # fungsi ini mem-pull variable dari xcom dengan key task_two, 
        print(f'print task_two variable from xcom: {task_two}') # dan mencetak variable tersebut

    push_var_from_task_a() >> get_var_from_task_a() # mendefinisikan urutan task dalam DAG
