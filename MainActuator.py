from pandas import DataFrame
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
    def find_different_records(df1, df2):


