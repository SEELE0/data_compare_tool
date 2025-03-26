from DataSource import *
from DataSource.Source_Hub import Source_Hub
from Config.ConfigReader import ConfigReader
from DataSource.SourceCsv import SourceCsv
from DataSource.Sourcefactory import Sourcefactory
from MainActuator import MainActuator
import pandas as pd

if __name__ == '__main__':
    # config_path = './Config/config.json'

    TYPE = input("请输入配置文件类型（json/excel）:\n"
                 "1.json\n"
                 "2.excel\n")
    if TYPE == '1':
        config_path = './Config/config.json'
        config_type = 'json'
        # config_data = ConfigReader('config_path').config_data
    elif TYPE == '2':
        config_path = './Config/config.xlsx'
        config_type = 'excel'
    else:
        raise ValueError("请检查输入内容")

    config_reader = ConfigReader(config_path, config_type)
    config_data = config_reader.config_data

    Datafactory = Sourcefactory()
    # config_data = ConfigReader('config_path').config_data
    for config_row in config_data:
        df1, df2 = Datafactory.filter_same_colname(
            Datafactory.create_entity(config_row['UpStream_Type'], config_row['filter']).fillna('Null'),
            Datafactory.create_entity(config_row['DownStream_Type'], config_row['filter']).fillna('Null')
        )