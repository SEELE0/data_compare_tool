# 后处理类
from MainActuator import MainActuator
import pandas as pd


class PostProcessing:
    def __init__(self, data1, data2):
        self.data1 = data1
        self.data2 = data2

    def get_data(self):
        missing_records = MainActuator.find_missing_records(self.data1, self.data2)
        missing_records['flag'] = '下游丢失'

        extra_records = MainActuator.find_extra_records(self.data1, self.data2)
        extra_records['flag'] = '上游丢失'

        # 获取 df1 和 df2 中 主键都存在 但是值不相同的记录

        result = pd.concat([missing_records, extra_records], ignore_index=True)

        return result

    def save_file(self, file_path):
        result = self.get_data()
        result.to_csv(file_path, index=False)
