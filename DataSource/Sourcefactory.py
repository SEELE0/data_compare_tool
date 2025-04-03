#根据配置文件 读取  数据源
# 找到对应的共有列 根据共有列生成 新的df1,df2
from pandas import DataFrame
from DataSource import *
from DataSource.Source_Hub import Source_Hub
from Config.ConfigReader import ConfigReader
from DataSource.SourceCsv import SourceCsv
from DataSource.SourceCrm_ali import SourceCrm_ali
from DataSource.SourceRedshift import SourceRedshift
import pandas as pd


class Sourcefactory:
    @staticmethod
    def create_entity(source_type, *args):
        """
        创建数据源对象
        :param source_type: 数据源类型
        :param args: 数据源参数
        :return: 数据源对象
        """
        # 调用工厂方法创建数据源对象
        entity = Source_Hub.create(source_type, *args)
        return entity.get_data()

    @staticmethod
    def filter_same_colname(df1: DataFrame, df2: DataFrame):
        # 获取两个DataFrame的列名
        columns1 = {col.lower(): col for col in df1.columns.tolist()}
        columns2 = {col.lower(): col for col in df2.columns.tolist()}

        # 找到相同的列名
        same_columns = set(columns1.keys()) & set(columns2.keys())
        #
        df1 = df1.filter(items=[columns1[col] for col in same_columns])
        df2 = df2.filter(items=[columns2[col] for col in same_columns])
        df1 = df1.rename(columns=lambda x: x.lower())
        df2 = df2.rename(columns=lambda x: x.lower())
        return df1, df2
