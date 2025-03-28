from MainActuator import MainActuator
import pandas as pd
list1=[['zhou', 11, 'af'],['kimi',2,'af'],['vers',1,'rb'],['l2ewis',44,'benz']]
list2 = [['zhou', 24, 'sub'], ['kimi', 0, 'na'], ['vers', 1, 'rb'],['bot',77,'benz']]
df1=pd.DataFrame(list1,columns=['name','num','team'])
df2=pd.DataFrame(list2,columns=['name','num','team'])
print(df1)
print(df2)

print(MainActuator.find_different_records(df1,df2,key_columns='name'))


print(MainActuator.find_missing_records(df1,df2))