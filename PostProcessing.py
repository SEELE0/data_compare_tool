# 后处理类
import pandas as pd
import os


class PostProcessing:

    @staticmethod
    def process_missing_extract_records(df, type, table_name, key_columns):
        df = df.copy() if not df.empty else df  # 额外复制一份会不会过多占用内存
        if not df.empty:
            if type == 'missing_records':  # 下游系统数据丢失
                df.loc[:, 'gap类型'] = '下游系统数据丢失'
            elif type == 'extra_records':  # 上游系统数据丢失
                df.loc[:, 'gap类型'] = '上游系统数据丢失'
            df.loc[:, 'Source'] = ''
            df.loc[:, '表名'] = table_name
            PostProcessing.save_file(df, table_name, key_columns)
        # del df

    @staticmethod
    def process_diff_records(df, key_columns, upstream_table_name, down_table_name):
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

        df = df.copy() if not df.empty else df

        key_columns = key_columns.split(',')
        result_rows = []
        for _, row in df.iterrows():
            key_values = row[key_columns]
            df1_columns = [col for col in df.columns if col.endswith('_df1')]
            df1_values = row[df1_columns].rename(lambda x: x.replace('_df1', ''))
            df2_columns = [col for col in df.columns if col.endswith('_df2')]
            df2_values = row[df2_columns].rename(lambda x: x.replace('_df2', ''))

            # 处理上游数据
            upstream = pd.concat([key_values, df1_values])
            upstream['gap类型'] = '数据存在差异'
            upstream['Source'] = '上游'
            upstream['表名'] = upstream_table_name
            result_rows.append(upstream)

            # 处理下游数据
            downstream = pd.concat([key_values, df2_values])
            downstream['gap类型'] = '数据存在差异'
            downstream['Source'] = '下游'
            downstream['表名'] = down_table_name
            result_rows.append(downstream)

        result = pd.DataFrame(result_rows)
        PostProcessing.save_file(result, down_table_name )
        # del df
        # del result

    @staticmethod
    def save_file(df, tablename, key_columns=None):
        if key_columns is None:
            key_columns = []
        else:
            key_columns = key_columns.split(',')
        cols = df.columns.tolist()
        cols.insert(0, cols.pop(cols.index('gap类型')))
        cols.insert(1, cols.pop(cols.index('Source')))
        cols.insert(2, cols.pop(cols.index('表名')))
        index_insert = 3
        for key in key_columns:
            cols.insert(index_insert, cols.pop(cols.index(key)))
            index_insert = index_insert+1
        df = df[cols]
        if len(tablename.split("\\")) > 1:
            direct_path = os.path.join(os.path.dirname(__file__), 'Output', f'{tablename.split("\\")[-1]}.csv')
        else:
            direct_path = os.path.join(os.path.dirname(__file__), 'Output', f'{tablename}.csv')
        if not os.path.exists(direct_path):
            df.to_csv(direct_path, mode='a', index=False)
        else:
            df.to_csv(direct_path, mode='a', index=False, header=False)

        # ⚠⚠⚠ 注意写入模式 ! 建议每个函数后都单独调用 save_file函数 及时释放内存
        # del df
        # 如果性能不佳 请使用  del df   手动回收内存

        # 可能存在隐患：
        # 1. 没有在main.py中 设置每次运行主程序前先清空输出文件夹
        #   也可以获取当天日期后在创建子文件夹  例如 Output/2023-10-01/