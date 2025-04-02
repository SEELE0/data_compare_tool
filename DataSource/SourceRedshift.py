import os.path

import pandas as pd
import redshift_connector
from redshift_connector import Error

from DataSource.Source_Hub import Source_Hub
import configparser


class SourceRedshift(Source_Hub):
    def __init__(self, table_name, filter_rule=None):
        # 初始化方法
        self.table_name = table_name
        self.filter = filter_rule

    @staticmethod
    def conn():
        config = configparser.ConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), 'env', 'redshift.ini'))
        payload = {
            'host': config['qa_eng']['Host'],
            'user': config['qa_eng']['User'],
            'port': config['qa_eng']['Port'],
            'database': config['qa_eng']['Database'],
            'password': config['qa_eng']['Password']
        }
        # 连接数据库
        try:
            connection = redshift_connector.connect(
                host=payload['host'],
                user=payload['user'],
                port=payload['port'],
                database=payload['database'],
                password=payload['password']
            )

            return connection
        except Error as e:
            print(f"Error while connecting : {e}")

    # 获取数据
    def get_data(self):
        with self.conn() as connection:
            try:
                if connection:
                    cursor = connection.cursor()
                    query = f"select * from {self.table_name} {self.filter}"
                    cursor.execute(query)
                    df: pd.DataFrame = cursor.fetch_dataframe()
                    # df = pd.read_sql_query(query, connection)
                    return df
            except Exception as e:
                raise Exception(f"Error while executing query: {e}")
