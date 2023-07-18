from datetime import datetime, timedelta
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.dummy import DummyOperator
from airflow.decorators import task, dag
from airflow.providers.common.sql.sensors.sql import SqlSensor

default_args = {
    'start_date': datetime(2023, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    dag_id='load_insurance_statistics',
    default_args=default_args,
    schedule_interval=None,
    catchup=True
)
def load_insurance_statistics():
    start_load = DummyOperator(
        task_id="start_load",
    )
    load_insurance_to_core = SnowflakeOperator(
        task_id="load_insurance_to_core",
        sql='''
            CALL core.data_load_insurance();
            ''',
        snowflake_conn_id="snowflake_conn"
    )
    tables = ['anlz.smokers_people', 'anlz.obesity_people', 'anlz.ins_pays']

    check_statistics_load = DummyOperator(
        task_id="check_statistics_load"
    )
    finish_load = DummyOperator(
        task_id="finish_load",
    )

    start_load >> load_insurance_to_core

    for table in tables:
        load_statistic_to_table = SnowflakeOperator(
            task_id=f"load_statistic_to_{table}",
            sql=f"CALL anlz.data_load_statistics('{table}');",
            snowflake_conn_id="snowflake_conn"
        )
#проверку на полноту таблиц сделал в виде сенсора. Если таблицы пустые даг упадет с ошибкой
        table_sensor = SqlSensor(
            task_id=f"check_{table}_not_empty",
            conn_id="snowflake_conn",
            sql=f"SELECT COUNT(*) FROM {table};",
            mode="poke",
            timeout=30,
            poke_interval=10
        )

        load_insurance_to_core >> load_statistic_to_table >> check_statistics_load >> table_sensor >> finish_load


dag = load_insurance_statistics()
