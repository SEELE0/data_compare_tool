# 后处理类
from MainActuator import MainActuator
import pandas as pd


class PostProcessing:
    @staticmethod
    def process_missing_extract_records(df,type):
        if type == 'missing_records': #下游系统数据丢失
            pass
        if type == 'extra_records': #上游系统数据丢失
            pass
        # PostProcessing.save_file(file_path)

    @staticmethod
    def process_diff_records(df, key_columns):
        # 读入 df 样例 其中 key_columns = 'name'  也就是主键为 name
        # 列名后带_df1 为上游数据   _df2为下游数据

        # name   num_df1  team_df1   num_df2  team_df2
        # 0  zhou  ** 11 **        af  ** 24 **        af
        # 1  kimi   ** 2 **  ** af **   ** 0 **  ** na **

        # 预期处理输出结果样式如下
        # gap类型     Source   表名    name     num              team
        # 数据存在差异  上游   member    zhou   ** 11 **            af
        # 数据存在差异  下游   member    zhou   ** 24 **            af
        # 数据存在差异  上游   member    kimi   ** 2 **          ** af **
        # 数据存在差异  下游   member    kimi   ** 0 **          ** na **
        pass
        # PostProcessing.save_file(file_path)

    @staticmethod
    def save_file(self, file_path):
        result = self.get_data()
        result.to_csv(file_path, index=False)  ##### ⚠⚠⚠ 注意写入模式 ! 建议没个函数后都单独调用 save_file函数 及时释放内存
        # 如果性能不佳 请使用  del df   手动回收内存
