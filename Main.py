from DataSource import *
from DataSource.Source_Hub import Source_Hub
from Config.ConfigReader import ConfigReader
from DataSource.SourceCsv import SourceCsv
from DataSource.Sourcefactory import Sourcefactory
from MainActuator import MainActuator
from PostProcessing import PostProcessing
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,  # 设置最低日志级别
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # 设置日志格式
    handlers=[
        # logging.FileHandler('app.log'),  # 将日志记录到文件中
        logging.StreamHandler()  # 将日志输出到控制台
    ]
)
logger = logging.getLogger(__name__)

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
    logger.info("读取配置文件成功")
    Datafactory = Sourcefactory()
    # config_data = ConfigReader('config_path').config_data
    for index,config_row in enumerate(config_data):
        df1, df2 = Datafactory.filter_same_colname(
            Datafactory.create_entity(config_row['UpStream_Type'],
                                      config_row['TableName_Upstream'],
                                      config_row['filter']).fillna('Null'),
            Datafactory.create_entity(config_row['DownStream_Type'],
                                      config_row['TableName_DownStream'],
                                      config_row['filter']).fillna('Null')
        )
        logger.info(f"================={index+1} - 读取数据成功=================")
        logger.info(f"当前处理数据配置为  {config_row}")
        find_different_records = MainActuator.find_different_records(df1, df2, key_columns=config_row['key'])

        # 方案一写法
        missing_records_df = MainActuator.find_missing_records(df1, df2)
        filtered_missing_df = missing_records_df[
            ~missing_records_df.set_index(config_row['key'].split(',')).index
            .isin(
                find_different_records.set_index(config_row['key'].split(',')).index
            )]

        extra_records_df = MainActuator.find_extra_records(df1, df2)
        filtered_extra_df = extra_records_df[
            ~extra_records_df.set_index(config_row['key'].split(',')).index
            .isin(
                find_different_records.set_index(config_row['key'].split(',')).index
            )]

        # # 方案二写法
        # missing_records_df = MainActuator.find_missing_records(df1, df2, key_columns=config_row['key'])
        # extra_records_df = MainActuator.find_extra_records(df1, df2, key_columns=config_row['key'])

        # 这里开始 进入后处理
        logger.info(f"================= 数据比对完成 =================")
        logger.info(f"================= 开始输出结果 =================")
        PostProcessing.process_missing_extract_records(filtered_missing_df, 'missing_records',
                                                       config_row['TableName_DownStream'])
        PostProcessing.process_missing_extract_records(filtered_extra_df, 'extra_records',
                                                       config_row['TableName_DownStream'])
        PostProcessing.process_diff_records(find_different_records, config_row['key'], config_row['TableName_Upstream'],
                                            config_row['TableName_DownStream'])
        logger.info(f"================= 结果输出完成 =================")

