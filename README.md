# data_compare_tool
## 父类 DataSource
- 函数功能 将不同数据源 读成 dataframe 

#### 子类
    - DB
        - Redshift
        - crm ali
        - rds
    - CSV
    - 
    - 
 
 
## 执行器类 main/execution
    - 函数功能 读取配置文件 自动匹配 子类
    - 函数功能 将比较的两张表字段  进行比较  拿出 相同字段列表
    - 主键在 A 中有  B中没有
    - 主键在 B 中有  A中没有
    - 主键在 A B 中都有  但是字段细节不一致
 
## 后处理类
将初步比较结果进行可视化处理 (different)
