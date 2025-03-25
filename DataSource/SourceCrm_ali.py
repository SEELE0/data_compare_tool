from DataSource.Source_Hub import Source_Hub
import pandas as pd
import os


class SourceCrm_ali(Source_Hub):
    def __init__(self, path):
        # 初始化方法
        self.path = path # 读取ini文件
    # 获取数据
    def get_data(self):
        print(self.path)