from datetime import datetime, timedelta
import csv
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.hooks.base_hook import BaseHook
import snowflake.connector

def load_data_to_snowflake():
    snowflake_conn_id = "snowflake_conn"
    conn = BaseHook.get_connection(snowflake_conn_id)

    connection = snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson.get('host'),
        warehouse=conn.extra_dejson.get('warehouse'),
        database=conn.schema
    )

    cursor = connection.cursor()
#Функция для преобразования полей csv в JSON и затем VARIANT. Преобразовывать пришлось выполнять построчно в цикле.
#Разбитие по батчам и преобразование и загрзка количество строк > 1 за 1 цикл возможно триал учеткой не поддерживается
#Изучу еще этот вопрос, т.к. в документации пока ответ не нашел почему загрузка завершалась ошибкой
    with open('/opt/airflow/logs/dataset.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            json_data = json.dumps(row)
            sql = f"INSERT INTO my_database.stage.ratings_all (all_data) SELECT TO_VARIANT(PARSE_JSON(%s));"
            cursor.execute(sql, (json_data,))
    connection.commit()
    connection.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2023, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='load_ratings',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
#На первом шаге удаляю ранее загруженные данные. В датасете содержатся рейтинги по годам, начиная с 1997. Врятли информация
#может меняться и историчность тут не нужна
    delete_from_stage = SnowflakeOperator(
        task_id="delete_from_stage",
        sql = '''
              TRUNCATE TABLE stage.ratings_all;
              ''',
        snowflake_conn_id="snowflake_conn",
    )
    load_csv_to_snowflake = PythonOperator(
        task_id="load_csv_to_snowflake",
        python_callable=load_data_to_snowflake,
    )
#перенос данных внутри базы решил сделать хранимыми процедурами. Прикрепляю отдельными файлами
    load_to_core = SnowflakeOperator(
        task_id="load_to_core",
        sql = '''
              CALL my_database.core.data_load_ratings();
              ''',
        snowflake_conn_id="snowflake_conn",
    )
    take_max_ratings = SnowflakeOperator(
        task_id="take_max_ratings",
        sql = '''
              CALL my_database.anlz.data_load_ratings_max();
              ''',
        snowflake_conn_id="snowflake_conn",
    )

delete_from_stage >> load_csv_to_snowflake >> load_to_core >> take_max_ratings
