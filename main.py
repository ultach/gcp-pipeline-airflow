# coding=utf-8
import configparser
import logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

from clickhouse_client import Client
from sensor import Sensor
from gcp_bucket import Bucket

config = configparser.ConfigParser()
config.read('config.cfg')
host = config['db']['host']
port = config['db']['port']
password = config['db']['password']
project = config['GCP']['project']
subscription = config['GCP']['subscription']
source_bucket = config['GCP']['source_bucket']
table_destination = config['db']['table_destination']

def _main():
    client = Client(host, port, password)
    bucket = Bucket(source_bucket)
    sensor = Sensor(project,subscription, bucket, table_destination, client)
    sensor.poll_notifications()

if __name__ == "__main__":
    _main()

