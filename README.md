# data_compare_tool
## 工厂模式
### 父类 DataSource
- 函数功能 将不同数据源 读成 dataframe 

##### 子类
    - DB
        - Redshift
        - crm ali
        - rds
    - CSV
    - 
    - 

 
### 执行器类 main/execution
    - 函数功能 读取配置文件 自动匹配 子类
    - 函数功能 将比较的两张表字段  进行比较  拿出 相同字段列表
    - 主键在 A 中有  B中没有
    - 主键在 B 中有  A中没有
    - 主键在 A B 中都有  但是字段细节不一致
 
### 后处理类
将初步比较结果进行可视化处理 (different)
打印输出两份文件，一份是每张表都生成一下的 detail表
格式如下
| gap类型 | Source | 表名 | col1 | col2 | col3 | ... | 
| :----- | :------: | :------: | :------: | :------: | :------: | -----: |
| 下游系统数据丢失 | | table1 | v1 | v2 | v3 | ...  |
| 上游系统数据丢失 | | table1 | v1 | v2 | v3 | ...  |
| 数据存在差异 | 上游 | table1 | v1 | v2 | v3 | ...  |
| 数据存在差异 | CCDH | table1 | v1 | v2 | v3 | ...  |

> 数据存在差异字段处理方式
在excel中将差异字段标记为黄色 ，字段开头和末尾加* 例如 `*timestamp*` 方便后期统计总量

一份是统计表
将所有表统计数据汇总 