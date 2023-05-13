from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.vertica_hook import VerticaHook
import logging

def load_currency_data():
    # Получаем соединения из подключений Airflow
    postgres_conn_id = 'postgres_conn'  # Идентификатор подключения для PostgreSQL в Airflow UI
    vertica_conn_id = 'vertica_conn'  # Идентификатор подключения для Vertica в Airflow UI

    # Создаем PostgreSQL hook
    postgres_hook = PostgresHook(postgres_conn_id)
    postgres_conn = postgres_hook.get_conn()

    # Создаем Vertica hook
    vertica_hook = VerticaHook(vertica_conn_id)
    vertica_conn = vertica_hook.get_conn()

    # Определяем таблицу для выгрузки
    table = {
        'source_table': 'public.currencies',
        'target_table': 'ANISIMOVP95YANDEXRU__STAGING.currency',
        'columns': ['currency_code', 'currency_code_with', 'currency_with_div', 'date_update']
    }
    
    try:
        # Устанавливаем соединение с PostgreSQL
        with psycopg2.connect(**postgres_conn) as postgres_conn:
            # Устанавливаем соединение с Vertica
            with vertica_python.connect(**vertica_conn) as vertica_conn:
                # Выполняем запрос для выборки данных из PostgreSQL
                with postgres_conn.cursor() as postgres_cursor:
                    source_table = table['source_table']
                    columns = table['columns']
                    date_column = columns[-1]  # Последний столбец считаем столбцом с датой
                    postgres_cursor.execute(f"SELECT {', '.join(columns)} FROM {source_table} WHERE {date_column} >= '2022-10-01' AND {date_column} < '2022-11-01'")
                    data = postgres_cursor.fetchall()
                
                # Выполняем запрос для вставки данных в Vertica
                with vertica_conn.cursor() as vertica_cursor:
                    target_table = table['target_table']
                    insert_query = f'INSERT INTO {target_table} ({", ".join(columns)}) VALUES ({", ".join(["%s"] * len(columns))})'
                    vertica_cursor.executemany(insert_query, data)
                    vertica_conn.commit()
    
        logging.info("Data loaded from currency table successfully!")
    
    except (psycopg2.Error, vertica_python.errors.Error) as e:
        logging.error(f"Error occurred while loading data from currency table: {str(e)}")


def load_currency_data():
    # Получаем соединения из подключений Airflow
    postgres_conn_id = 'postgres_conn'  # Идентификатор подключения для PostgreSQL в Airflow UI
    vertica_conn_id = 'vertica_conn'  # Идентификатор подключения для Vertica в Airflow UI

    # Создаем PostgreSQL hook
    postgres_hook = PostgresHook(postgres_conn_id)
    postgres_conn = postgres_hook.get_conn()

    # Создаем Vertica hook
    vertica_hook = VerticaHook(vertica_conn_id)
    vertica_conn = vertica_hook.get_conn()
    
        # Определяем таблицу для выгрузки
    table = {
        'source_table': 'public.transactions',
        'target_table': 'ANISIMOVP95YANDEXRU__STAGING.transactions',
        'columns': ['operation_id', 'account_number_from', 'account_number_to', 'currency_code', 'country', 'status', 'transaction_type', 'amount', 'transaction_dt']
    }
    
    try:
        # Устанавливаем соединение с PostgreSQL
        with psycopg2.connect(**postgres_conn) as postgres_conn:
            # Устанавливаем соединение с Vertica
            with vertica_python.connect(**vertica_conn) as vertica_conn:
                # Выполняем запрос для выборки данных из PostgreSQL
                with postgres_conn.cursor() as postgres_cursor:
                    source_table = table['source_table']
                    columns = table['columns']
                    date_column = columns[-1]  # Последний столбец считаем столбцом с датой
                    postgres_cursor.execute(f"SELECT {', '.join(columns)} FROM {source_table} WHERE {date_column} >= '2022-10-01' AND {date_column} < '2022-11-01'")
                    data = postgres_cursor.fetchall()
                
                # Выполняем запрос для вставки данных в Vertica
                with vertica_conn.cursor() as vertica_cursor:
                    target_table = table['target_table']
                    insert_query = f'INSERT INTO {target_table} ({", ".join(columns)}) VALUES ({", ".join(["%s"] * len(columns))})'
                    vertica_cursor.executemany(insert_query, data)
                    vertica_conn.commit()
    
        logging.info("Data loaded from transactions table successfully!")
    
    except (psycopg2.Error, vertica_python.errors.Error) as e:
        logging.error(f"Error occurred while loading data from transactions table: {str(e)}")


# Определяем параметры DAG
dag = DAG(
    'postgres_to_vertica',
    description='Загрузка данных из PostgreSQL в Vertica',
    schedule_interval='0 0 * * *',
    start_date=datetime(2023, 5, 10),
    catchup=False
)

# Определяем операторы PythonOperator
load_currency_data_operator = PythonOperator(
    task_id='load_currency',
    python_callable=load_currency_data,
    dag=dag
)

load_transactions_data_operator = PythonOperator(
    task_id='load_transactions',
    python_callable=load_transactions_data,
    dag=dag
)

# Определяем зависимости задач
load_currency_data_operator >> load_transactions_data_operator
