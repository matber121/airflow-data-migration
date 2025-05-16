#config.py 

# config.py

SCHEMA = 'stg'
TABLE = 'map'


DB_SCHEMA = 'STG'
DB_TABLE = 'LPS_MAP'
DB_SRC_SCHEMA = 'STG'
DB_SRC_TABLE = 'LPS_MAP'

SQL_QUERIES = {
    'read_column_mapping': """select 
                                    source_column_name,
                                    target_column_name, 
                                    source_schema, 
                                    source_table, 
                                    destination_schema, 
                                    destination_table, 
                                    backup_schema, 
                                    backup_table, 
                                    natural_key
                                    from config.publication pbc join
                                    config.publisher ps on pbc.publisher_id = ps.id
                                    where 
                                    upper(source_table) = {source_table}
                                    and upper(source_schema) = {source_schema} 
                                    and READ_ACTIVE=1;""",
    'read_data_from_stg': """SELECT {col_list} FROM {stage_schema}.{stage_table} WHERE EXPORTED_AT = '9999-12-31'""",
    'drop_bkp_tbl': """DROP TABLE IF EXISTS {bkp_schmea}.{bkp_table}""",
    'create_bkp_tbl': """CREATE TABLE {bkp_schema}.{bkp_table} AS SELECT * FROM {stage_schema}.{stage_table}""",
    'delete_existing_id': """DELETE FROM {dest_schema}.{dest_table} WHERE {id_col} IN ({values})""",
    'update_rows_at_src': """UPDATE {stage_schema}.{stage_table}
                    SET exported_at = CURRENT_TIMESTAMP()
                    WHERE {col} IN ({values}})"""

}
