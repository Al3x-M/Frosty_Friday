import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session): 
    tableName = 'snowflake.account_usage.tables'
    dataframe = session.table(tableName)

    dataframe = dataframe.filter((col('IS_TRANSIENT') == 'NO')  & (col('ROW_COUNT') > 0)).filter('"DELETED" is Null').group_by("TABLE_CATALOG").count().select_expr('"TABLE_CATALOG" as "DATABASE", "COUNT" as "TABLES"').sort(col("DATABASE").desc())
    return dataframe