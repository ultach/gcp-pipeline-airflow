from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime
import configparser

from clickhouse_client import Client
from gcp_bucket import Bucket

defualt_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 1),
    'retries': 1
}

dag = DAG("daily_stat", defualt_args=defualt_args, schedule_interval="0 5 * * *")

config = configparser.ConfigParser()
config.read('config.cfg')
host = config['db']['host']
port = config['db']['port']
password = config['db']['password']
destination_bucket = config['GCP']['destination_bucket']

def build_daily_stat():
    client = Client(host, port, password)
    with open('sql/daily_stat.sql') as sqlfile:
        today = datetime.now().strftime('%Y-%m-%d')
        outfile = today + '.csv'
        sqlfile = sqlfile.format(today=today, outfile=outfile)
        client.execute_query(sqlfile)

def sent_daily_report():
    bucket = Bucket(destination_bucket)
    today = datetime.now().strftime('%Y-%m-%d')
    source_file = today + '.csv'
    bucket.upload_blob(source_file, source_file)

t1 = PythonOperator(task_id="build_daily_stat", dag=dag, python_callable=build_daily_stat)
t2 = PythonOperator(task_id="sent_daily_report", dag=dag, python_callable=sent_daily_report)

t1 >> t2
