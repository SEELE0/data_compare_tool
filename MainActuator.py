from pandas import DataFrame
import pandas as pd
from collections import namedtuple


# Factory = Source_Hub.create('Csv','./data/test.csv')

class MainActuator:
    # @staticmethod
    # def find_missing_records(df1, df2,key_columns):
    #
    # @staticmethod
    # def find_extra_records(df1, df2):

    # 找出 df1 中有但 df2 中没有的记录
    @staticmethod
    def find_missing_records(df1, df2, ):
        # 找出 df1 中有但 df2 中没有的记录
        comparison_df = pd.concat([df1, df2, df2]).drop_duplicates(keep=False)
        # return comparison_df[~comparison_df[key_columns].isin(df2[key_columns])]
        return comparison_df

    # 找出 df2 中有但 df1 中没有的记录
    @staticmethod
    def find_extra_records(df1, df2):
        # 找出 df2 中有但 df1 中没有的记录
        comparison_df = pd.concat([df2, df1, df1]).drop_duplicates(keep=False)
        # return comparison_df[~comparison_df[key_columns].isin(df1[key_columns])]
        return comparison_df

    # 找出找出 df1 中有但 df2 中没有的记录 (方案2)
    # 左连接后 将 判断 右侧df2 表字段全为空的 字段 这种数据 我们可以认为是df1中 有但是df2中缺失的数据
    @staticmethod
    def find_missing_records_2(df1: DataFrame, df2: DataFrame,key_columns):
        key_columns = key_columns.split(',')
        check_columns = [col for col in df1.columns if col not in key_columns]
        check_columns = [f'{x}_df2' for x in check_columns]
        left_merge = df1.merge(df2, how='left', on=key_columns, suffixes=('_df1', '_df2'))
        # Identify rows in df1 that are missing in df2 by checking if all df2 fields are null
        missing_in_df2 = left_merge[left_merge[check_columns].isnull().all(axis=1)]
        missing_in_df2 = df1[df1['name'].isin(missing_in_df2['name'])]
        # print(missing_in_df2)
        return missing_in_df2

    # 找出 df2 中有但 df1 中没有的记录 (方案2)
    @staticmethod
    def find_extra_records_2(df1: DataFrame, df2: DataFrame,key_columns):
        key_columns = key_columns.split(',')
        check_columns = [col for col in df2.columns if col not in key_columns]
        check_columns = [f'{x}_df1' for x in check_columns]
        left_merge = df2.merge(df1, how='left', on=key_columns, suffixes=('_df2', '_df1'))
        # Identify rows in df2 that are missing in df1 by checking if all df1 fields are null
        missing_in_df1 = left_merge[left_merge[check_columns].isnull().all(axis=1)]
        missing_in_df1 = df2[df2['name'].isin(missing_in_df1['name'])]
        # print(missing_in_df1)
        return missing_in_df1

    # 找出 df1 和 df2 中 主键都存在 但是值不相同的记录  (方案一)
    # 生成
    @staticmethod
    def find_different_records(df1: DataFrame, df2: DataFrame, key_columns):
        def composite_new_df(rows: list) -> DataFrame:
            new_df_data = [row._asdict() for row in rows]
            new_df = pd.DataFrame(new_df_data)
            return new_df

        df1 = df1.rename(columns=lambda x: x.lower())
        df2 = df2.rename(columns=lambda x: x.lower())
        key_columns = key_columns.split(',')
        merge_df = df1.merge(df2, how='inner', on=key_columns, suffixes=('_df1', '_df2'))
        # key_columns | col1_df1 | col2_df1 | col3_df1 | col1_df2 | col2_df2 | col3_df2
        records = []
        for row in merge_df.itertuples(index=False):
            pram_dict = {}
            check_columns = [col for col in df1.columns if col not in key_columns]
            # print(row)
            for col in check_columns:
                col_df1 = f'{col}_df1'
                col_df2 = f'{col}_df2'
                if getattr(row, col_df1) != getattr(row, col_df2):
                    # 记录在案
                    # pram_dict[col_df1] = f'**{getattr(row, col_df1)}** | **{getattr(row, col_df2)}**'
                    pram_dict[col_df1] = f'** {getattr(row, col_df1)} **'
                    pram_dict[col_df2] = f'** {getattr(row, col_df2)} **'

                    # 字典样式
                    # {col1_df1:**adafa**,
                    # col2_df1:**adafa**,
                    # col3_df1:**adafa**,
                    # col1_df2:**adafa**,
                    # col2_df2:**adafa**,
                    # col3_df2:**adafa**}
            if bool(pram_dict):
                record_diff_row = row._replace(**pram_dict)  # 解包修改字段
                records.append(record_diff_row)
        new_df = composite_new_df(records)
        return new_df

    # # 找出两者之间数据不同的记录
    # def find_different_records(df1, df2, key_columns):
    #     df1 = df1.rename(columns=lambda x: x.lower())
    #     df2 = df2.rename(columns=lambda x: x.lower())
    #     df_merged = pd.merge(df1, df2, on=key_columns, how='inner', suffixes=('_df1', '_df2'))
    #     columns_to_check = [col for col in df1.columns if col != key_columns]
    #     # 专门用于比较两列包含NaN时的情况   这里debug 存在问题 (弃用)
    #     # def compare_columns_with_nan(col1, col2):
    #     #     both_nan = col1.isna() & col2.isna()
    #     #     equal_or_both_nan = (col1 == col2) | both_nan
    #     #     return ~equal_or_both_nan
    #     # 比较对应列
    #     rows_with_discrepancies = []
    #     for col in columns_to_check:
    #         # if col.endswith('data')|col.endswith('time')|col.endswith('tmstmp'):
    #         #     diff = df_merged[f'{col}_df1'] != df_merged[f'{col}_df2']
    #         # else:
    #         #     diff = df_merged[f'{col}_df1'].str.strip() != df_merged[f'{col}_df2'].str.strip()
    #
    #         # diff = compare_columns_with_nan(df_merged[f'{col}_df1'], df_merged[f'{col}_df2'])
    #         # diff = df_merged[f'{col}_df1'] != df_merged[f'{col}_df2']
    #         # diff = df_merged['cost_center_type_df1'].str.strip() != df_merged['cost_center_type_df2'].str.strip()
    #
    #         #可以用DataFrame 的 itertuples 方法 进行改进??
    #         if ptypes.is_string_dtype(df_merged[f'{col}_df1']):
    #             diff = df_merged[f'{col}_df1'].str.strip() != df_merged[f'{col}_df2'].str.strip()
    #         else:
    #             diff = df_merged[f'{col}_df1'] != df_merged[f'{col}_df2']
    #         rows_with_discrepancies.append(diff)
    #
    #         # rows_with_discrepancies.append(df_merged[f'{col}_df1'] != df_merged[f'{col}_df2'])
    #
    #     # 将所有比较列结果合并
    #     discrepancies = df_merged[pd.concat(rows_with_discrepancies, axis=1).any(axis=1)]
    #     discrepancies = discrepancies[[key_columns] + [f'{col}_df1' for col in columns_to_check]].copy()
    #     discrepancies.columns = [key_columns] + columns_to_check  # 从_df1列命名回到原来的列名
    #     # 去除_suffix以便输出更清晰
    #     # discrepancies = discrepancies[df1.columns]
    #     return discrepancies
