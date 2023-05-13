from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from vertica_python import connect

default_args = {
    'owner': 'anisimovp',
    'start_date': datetime(2023, 5, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def execute_sql():
    # Чтение SQL-запроса из файла
    with open('/lessons/dags/load_data_global_metrics.sql', 'r') as file:
        sql_query = file.read()
    
    # Подключение к Vertica и выполнение SQL-запроса
    vertica_conn = {
        'host': '51.250.75.20',
        'port': '5433',
        'user': 'ANISIMOVP95YANDEXRU',
        'password': 'ORggzQNxMpoi6fk',
        'database': 'dwh',
        'autocommit': True
    }
    
    with connect(**vertica_conn) as conn:
        cur = conn.cursor()
        cur.execute(sql_query)
        cur.close()


dag = DAG(
    'execute_sql_and_load_data',
    default_args=default_args,
    description='load_data_to_cdm',
    schedule_interval='@daily',
)

execute_sql_task = PythonOperator(
    task_id='load_data_to_cdm',
    python_callable=execute_sql,
    dag=dag,
)

# Добавьте фиктивный оператор в качестве последней задачи, чтобы отметить конец DAG
end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

# Определение зависимости задач
execute_sql_task >> end_task
