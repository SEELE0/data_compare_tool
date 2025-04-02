from DataSource.Sourcefactory import Sourcefactory

df_test = Sourcefactory.create_entity('Crm_ali', 'Account', 'limit 10')
print(df_test)
print(df_test.columns.tolist())
