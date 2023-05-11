from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from vertica_python import connect

default_args = {
    'owner': 'anisimovp',
    'start_date': datetime(2023, 5, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def execute_sql():
    # Чтение SQL-запроса из файла
    with open('/lessons/load_data_global_metrics.sql', 'r') as file:
        sql_query = file.read()
    
    # Подключение к Vertica
    vertica_conn = {
        'host': '51.250.75.20',
        'port': '5433',
        'user': 'ANISIMOVP95YANDEXRU',
        'password': 'ORggzQNxMpoi6fk',
        'database': 'dwh',
        'autocommit': True
    }
    conn = connect(**vertica_conn)
    
    # Выполнение SQL-запроса
    cur = conn.cursor()
    cur.execute(sql_query)
    cur.close()
    conn.commit()
    conn.close()


dag = DAG(
    'execute_sql_and_load_data',
    default_args=default_args,
    description='DAG to execute SQL query and load data into Vertica',
    schedule_interval='@daily',
)

execute_sql_task = PythonOperator(
    task_id='load_data_to_cdm',
    python_callable=execute_sql,
    dag=dag,
)


execute_sql_task 


