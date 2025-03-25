#根据配置文件 读取  数据源
# 找到对应的共有列 根据共有列生成 新的df1,df2
from pandas import DataFrame
from DataSource import *
from DataSource.Source_Hub import Source_Hub
from Config.ConfigReader import ConfigReader
from DataSource.SourceCsv import SourceCsv
import pandas as pd

class Sourcefactory:
    def create_entity(self, source_type, *args):
        """
        创建数据源对象
        :param source_type: 数据源类型
        :param args: 数据源参数
        :return: 数据源对象
        """
        # 调用工厂方法创建数据源对象
        entity = Source_Hub.create(source_type, *args)
        return entity.get_data()

    def filter_same_colname(self, df1: DataFrame, df2: DataFrame):
        # 获取两个DataFrame的列名
        columns1 = [col.lower() for col in df1.columns.tolist()]
        columns2 = [col.lower() for col in df2.columns.tolist()]

        # 找到相同的列名
        same_columns = set(columns1) & set(columns2)
        #
        df1 = df1.filter(items=same_columns)
        df2 = df2.filter(items=same_columns)

        return df1, df2

