from DataSource.Source_Hub import Source_Hub
import pandas as pd
import os


class SourceCsv(Source_Hub):
    # def __init__(self, source):
    #     self.source = source
    def __init__(self, file_path, filter_rule=None):
        # 初始化方法
        self.file_path = file_path
        self.filter = filter_rule

    # 获取数据
    def get_data(self):
        # # 获取当前文件的绝对路径
        # current_dir = os.path.dirname(os.path.abspath(__file__))
        # # 拼接文件名和路径
        # file_path = os.path.join(current_dir, 'data', 'test.csv')
        # 读取csv文件
        df = pd.read_csv(self.file_path, encoding='utf-8')
        return df
