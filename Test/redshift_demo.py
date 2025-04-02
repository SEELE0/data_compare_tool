from DataSource.Sourcefactory import Sourcefactory

df_test = Sourcefactory.create_entity('redshift', 'ext_dice2cn_publish_dependent_common.cn_fact_meeting_interaction_raw', 'limit 10')
print(df_test)
print(df_test.columns.tolist())
