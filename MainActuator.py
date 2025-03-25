from pandas import DataFrame
from DataSource import *
from DataSource.Source_Hub import Source_Hub
from Config.ConfigReader import ConfigReader
from DataSource.SourceCsv import SourceCsv
from DataSource.Sourcefactory import Sourcefactory
import pandas as pd
# Factory = Source_Hub.create('Csv','./data/test.csv')

class MainActuator:
    # 找出 df1 中有但 df2 中没有的记录
    def find_missing_records(df1, df2):
        # 找出 df1 中有但 df2 中没有的记录
        comparison_df = pd.concat([df1, df2, df2]).drop_duplicates(keep=False)
        # return comparison_df[~comparison_df[key_columns].isin(df2[key_columns])]
        return comparison_df

    # 找出 df2 中有但 df1 中没有的记录
    def find_extra_records(df1, df2):
        # 找出 df2 中有但 df1 中没有的记录
        comparison_df = pd.concat([df1, df2, df1]).drop_duplicates(keep=False)
        # return comparison_df[~comparison_df[key_columns].isin(df1[key_columns])]
        return comparison_df

    # 找出 df1 和 df2 中 主键都存在 但是值不相同的记录


    def main(self):
        config_path = './Config/config.json'

        # TYPE = input("请输入配置文件类型（json/excel）:\n"
        #              "1.json\n"
        #              "2.excel\n")
        # if TYPE == '1':
        #     config_path = './Config/config.json'
        #     config_data = ConfigReader('config_path').config_data

        Datafactory = Sourcefactory()
        config_data = ConfigReader('config_path').config_data
        for config_row in config_data['sites']:
            df1, df2 = Datafactory.filter_same_colname(
                Datafactory.create_entity(config_row['UpStream_Type'],config_row['filter']),
                Datafactory.create_entity(config_row['DownStream_Type'],config_row['filter'])
            )



