from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
import vertica_python

def load_data():
    # Создаем соединения с базами данных
    postgres_conn = {
        'host': 'rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net',
        'port': '6432',
        'user': 'student',
        'password': 'de_student_112022',
        'database': 'db1'
    }
        
    vertica_conn = {
        'host': '51.250.75.20',
        'port': '5433',
        'user': 'ANISIMOVP95YANDEXRU',
        'password': 'ORggzQNxMpoi6fk',
        'database': 'dwh',
        'autocommit': True
    }

    # Определяем список таблиц для выгрузки
    tables = [
        {
            'source_table': 'public.currencies',
            'target_table': 'ANISIMOVP95YANDEXRU__STAGING.currency',
            'columns': ['currency_code', 'currency_code_with', 'currency_with_div', 'date_update']
        },
        {
            'source_table': 'public.transactions',
            'target_table': 'ANISIMOVP95YANDEXRU__STAGING.transactions',
            'columns': ['operation_id', 'account_number_from', 'account_number_to', 'currency_code', 'country', 'status', 'transaction_type', 'amount', 'transaction_dt']
        }
    ]
    postgres_conn = psycopg2.connect(**postgres_conn)
    vertica_conn = vertica_python.connect(**vertica_conn)
    
    # Загрузка данных из PostgreSQL в Vertica
    for table in tables:
        source_table = table['source_table']
        target_table = table['target_table']
        columns = table['columns']
        date_column = columns[-1]  # Последний столбец считаем столбцом с датой
        
        # Устанавливаем соединение с PostgreSQL и выполняем запрос для выборки данных за октябрь 2022 года
        postgres_cursor = postgres_conn.cursor()
        postgres_cursor.execute(f"SELECT {', '.join(columns)} FROM {source_table} WHERE {date_column} >= '2022-10-01' AND {date_column} < '2022-11-01'")
        data = postgres_cursor.fetchall()
        
        # Устанавливаем соединение с Vertica и выполняем запрос для вставки данных
        vertica_cursor = vertica_conn.cursor()
        insert_query = f'INSERT INTO {target_table} ({", ".join(columns)}) VALUES ({", ".join(["%s"] * len(columns))})'
        vertica_cursor.executemany(insert_query, data)
        vertica_conn.commit()
        
        # Закрываем соединения
        postgres_cursor.close()
     
        vertica_cursor.close()
        
    postgres_conn.close()
    vertica_conn.close()


# Определяем параметры DAG
dag = DAG(
    'postgres_to_vertica',
    description='Загрузка данных из PostgreSQL в Vertica',
    schedule_interval='0 0 * * *',
    start_date=datetime(2023, 5, 10),
    catchup=False
)

# Определяем оператор PythonOperator
load_data_operator = PythonOperator(
    task_id='load_stg',
    python_callable=load_data,
    dag=dag
)

# Определяем зависимости задач
load_data_operator
