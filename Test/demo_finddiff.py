from MainActuator import MainActuator
import pandas as pd
list1=[['zhou', 11, 'af'],['kimi',2,'af'],['vers',1,'rb'],['l2ewis',44,'benz']]
list2 = [['zhou', 24, 'af'], ['kimi', 0, 'na'], ['bot',77,'benz'],['vers', 1, 'rb']]

# df1
#      name  num  team
# 0    zhou   11    af
# 1    kimi    2    af
# 2    vers    1    rb
# 3  lewis   44  benz

# df2
#    name  num  team
# 0  zhou   24    af
# 1  kimi    0    na
# 2   bot   77  benz
# 3  vers    1    rb



df1=pd.DataFrame(list1,columns=['name','num','team'])
df2=pd.DataFrame(list2,columns=['name','num','team'])
print("#######打印原始表#######")
print(f"df1:\n{df1}")
print(f"df2:\n{df2}")

print(MainActuator.find_different_records(df1,df2,key_columns='name'))
diff_df = MainActuator.find_different_records(df1,df2,key_columns='name,team')
print(diff_df)
# print(diff_df[['name','team']])

missing_df = MainActuator.find_missing_records(df1, df2)
filtered_missing_df = missing_df[~missing_df.set_index(['name', 'team']).index.isin(diff_df.set_index(['name', 'team']).index)]
print(filtered_missing_df)

test_df = df1.merge(df2, how='left', on='name', suffixes=('_df1', '_df2'))
print(test_df)

# Identify rows in df1 that are missing in df2 by checking if all df2 fields are null
missing_in_df2 = test_df[test_df[['num_df2', 'team_df2']].isnull().all(axis=1)]
missing_in_df2 = df1[df1['name'].isin(missing_in_df2['name'])]
print(missing_in_df2)


missing_in_df2 = MainActuator.find_missing_records_2(df1, df2, key_columns='name')
print(missing_in_df2)

missing_df = MainActuator.find_missing_records(df1, df2)
diff_df = MainActuator.find_different_records(df1,df2,key_columns='name')
filtered_missing_df = missing_df[~missing_df.set_index(['name']).index.isin(diff_df.set_index(['name']).index)]
print(filtered_missing_df)