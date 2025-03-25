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
    def find_missing_records(df1, df2, key_columns):
        # df_combined = pd.concat([df1, df2])
        # df_missing = df_combined.drop_duplicates(keep=False)
        # 找出 df1 中有但 df2 中没有的记录
        comparison_df = pd.concat([df1, df2, df2]).drop_duplicates(keep=False)
        # return df_missing[df_missing.isin(df1).all(axis=1)].reset_index(drop=True)
        return comparison_df[~comparison_df[key_columns].isin(df2[key_columns])]
        # return comparison_df
        # # 找出仅在df1中存在的记录
        # only_in_df1 = df1[~df1[key_col].isin(df2[key_col])]



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



