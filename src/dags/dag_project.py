from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import vertica_python
import pendulum
import boto3
import glob
import csv
import os
from pathlib import Path

AWS_ACCESS_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')

VERTICA_CONN = BaseHook.get_connection("VERTICA")
conn_info = {
    'host': VERTICA_CONN.host,
    'port': VERTICA_CONN.port,
    'user': VERTICA_CONN.login,
    'password': VERTICA_CONN.get_password(),
    'database': VERTICA_CONN.schema,
    'autocommit': True
}

def fetch_s3_file(bucket: str, key: str):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name = 's3',
        endpoint_url = 'https://storage.yandexcloud.net',
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket = bucket,
        Key = key,
        Filename = os.path.join(Path(__file__).parents[3], 'data', key)
    )

def executed(sql_string):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(sql_string)

@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def project6_dag():
    fetch_group_log_csv = PythonOperator(
        task_id=f'fetch_group_log_csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'group_log.csv'},
    )
    create_table_group_log = PythonOperator(
        task_id='create_table_group_log',
        python_callable=executed,
        op_kwargs={'sql_string':'CREATE TABLE IF NOT EXISTS STV2024111147__STAGING.group_log (id identity primary key, group_id int, user_id int, user_id_from int DEFAULT NULL, event varchar(10), datetime timestamp) \
                                 ORDER BY id, group_id, user_id PARTITION BY datetime::date GROUP BY calendar_hierarchy_day(datetime::date, 3, 2)'}
    )
    load_data2group_log_table = PythonOperator(
        task_id = 'load_data2group_log_table',
        python_callable=executed,
        op_kwargs={'sql_string':"COPY STV2024111147__STAGING.group_log FROM LOCAL '/data/group_log.csv' DELIMITER ',' "}
    )
    create_table_l_user_group_activity = PythonOperator(
        task_id = 'create_table_l_user_group_activity',
        python_callable=executed,
        op_kwargs={'sql_string':'CREATE TABLE IF NOT EXISTS STV2024111147__DWH.l_user_group_activity \
                   (hk_l_user_group_activity INT PRIMARY KEY, \
                    hk_user_id INT CONSTRAINT fk_l_hk_l_user_group_activity_user REFERENCES STV2024111147__DWH.h_users (hk_user_id), \
                    hk_group_id INT CONSTRAINT fk_l_hk_l_user_group_activity_group REFERENCES STV2024111147__DWH.h_groups (hk_group_id), \
                    load_dt TIMESTAMP, \
                    load_src VARCHAR(20)) \
                    order by load_dt \
                    segmented by hk_l_user_group_activity all nodes \
                    partition by load_dt::date \
                    group by calendar_hierarchy_day(load_dt::date, 3, 2)'}
    )
    insert2table_l_user_group_activity = PythonOperator(
        task_id = 'insert2table_l_user_group_activity',
        python_callable=executed,
        op_kwargs={'sql_string':"INSERT INTO STV2024111147__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src) \
                                 SELECT distinct hash(hu.hk_user_id, hg.hk_group_id) as hk_l_user_group_activity, \
                                        hu.hk_user_id as hk_user_id, \
                                        hg.hk_group_id as hk_group_id, \
                                        now() as load_dt, \
                                        's3' as load_src \
                                 FROM STV2024111147__STAGING.group_log gl \
                                 LEFT JOIN STV2024111147__DWH.h_users hu ON hu.user_id = gl.user_id \
                                 LEFT JOIN STV2024111147__DWH.h_groups hg ON hg.group_id = gl.group_id \
                                 WHERE hash(hu.hk_user_id, hg.hk_group_id) NOT IN (SELECT hk_l_user_group_activity FROM STV2024111147__DWH.l_user_group_activity)"}
    )
    create_table_s_auth_history = PythonOperator(
        task_id = 'create_table_s_auth_history',
        python_callable=executed,
        op_kwargs={'sql_string':"CREATE TABLE IF NOT EXISTS STV2024111147__DWH.s_auth_history \
                   (hk_l_user_group_activity int not null constraint fk_s_auth_history_l_user_group_activity references STV2024111147__DWH.l_user_group_activity (hk_l_user_group_activity), \
                    user_id_from INT, \
                    event VARCHAR(10), \
                    event_dt TIMESTAMP, \
                    load_dt TIMESTAMP, \
                    load_src VARCHAR(20)) \
                    order by load_dt \
                    segmented by hk_l_user_group_activity all nodes \
                    partition by load_dt::date \
                    group by calendar_hierarchy_day(load_dt::date, 3, 2)"}
    )
    insert2table_s_auth_history = PythonOperator(
        task_id = 'insert2table_s_auth_history',
        python_callable=executed,
        op_kwargs={'sql_string':'''INSERT INTO STV2024111147__DWH.s_auth_history (hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src) \
                                    SELECT luga.hk_l_user_group_activity, \
                                        gl.user_id_from, \
                                        gl.event, \
                                        gl.datetime as event_dt, \
                                        now() as load_dt, \
                                        's3' as load_src \
                                    FROM STV2024111147__STAGING.group_log gl \
                                    LEFT JOIN STV2024111147__DWH.h_groups hg ON gl.group_id = hg.group_id \
                                    LEFT JOIN STV2024111147__DWH.h_users hu ON gl.user_id = hu.user_id \
                                    LEFT JOIN STV2024111147__DWH.l_user_group_activity luga ON hg.hk_group_id = luga.hk_group_id AND hu.hk_user_id = luga.hk_user_id \
                                    WHERE hk_l_user_group_activity is not null '''}
    )
    fetch_group_log_csv >> [create_table_group_log, create_table_l_user_group_activity, create_table_s_auth_history] >> load_data2group_log_table
    load_data2group_log_table >> insert2table_l_user_group_activity >> insert2table_s_auth_history

project6_dag = project6_dag()
