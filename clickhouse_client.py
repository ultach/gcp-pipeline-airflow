import csv
import logging
import clickhouse_driver
from datetime import datetime

logger = logging.getLogger(__name__)

class Client:
    def __init__(self, host, port, password=None):
        if password is None:
            password = 'defualt'
        try:
            self.__client = clickhouse_driver.Client('localhost', password='clickhouse')
        except Exception:
            print("Cannot connect to database.")

    def to_datetime(self, datetime_str):
        return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')

    def apply_type(self, row, keys, key_type):
        for key in keys:
            row[key] =  None if row[key] in set(['NULL', '\\N', '']) else key_type(row[key])

    def validate_row(self, row):
        int_keys = ['tripduration', 'start station id', 'end station id', 'bikeid', 'gender', 'birth year']
        float_keys = ['start station latitude', 'end station latitude',
                    'start station longitude', 'end station longitude']
        datetime_keys = ['starttime', 'stoptime']
        
        try:
            for keys, func in zip([int_keys, float_keys, datetime_keys], [int, float, self.to_datetime]):
                self.apply_type(row, keys, func)
            return row
        except Exception as ex:
            logger.exception(ex)
            return None

    def row_reader(self, source_path):
        with open(source_path) as csv_data:
            for line in csv.DictReader(csv_data):
                yield self.validate_row(line)

    def insert_values(self, source_path, table_name):
        INSERT_SQL_QUERY = f"INSERT INTO {table_name} VALUES"
        SHIFT_SIZE = 100
        try:
            rows_to_insert = [line for line in self.row_reader(source_path) if line]
            for shift in range(0, len(rows_to_insert), SHIFT_SIZE):
                logger.info(f"Process: {shift}\{len(rows_to_insert)} rows uploaded.")
                self.__client.execute(INSERT_SQL_QUERY, rows_to_insert[shift : shift + SHIFT_SIZE])
            logger.info(f"All {len(rows_to_insert)} rows uploaded successfully :)")
        except Exception as ex:
            logger.exception(ex)
    
    def execute_query(self, sql_query):
        return self.__client.execute(sql_query)

