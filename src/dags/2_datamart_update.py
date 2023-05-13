from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.vertica_hook import VerticaHook

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

    # Получение соединения с Vertica из Airflow connections
    vertica_conn_id = 'vertica_conn'  # Идентификатор подключения для Vertica в Airflow UI
    vertica_hook = VerticaHook(vertica_conn_id)
    vertica_conn = vertica_hook.get_conn()
    
    # Выполнение SQL-запроса в Vertica
    with vertica_conn.cursor() as cur:
        cur.execute(sql_query)


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
