import json
import toml
import requests
import snowflake.connector as sf

def lambda_handler(event, context):
    #loading config data
    config = toml.load('config.toml')
    url = config['source']['url']
    account = config['snowflake']['account']
    user = config['snowflake']['user']
    password = config['snowflake']['password']
    warehouse = config['snowflake']['warehouse']
    database = config['snowflake']['database']
    schema = config['snowflake']['schema']
    table = config['snowflake']['table']
    role = config['snowflake']['role']
    stage = config['snowflake']['stage']
    
    
    #download file, and copy to temp folder on lambda
    response = requests.get(url)
    file_name = 'temp.csv'
    temp_file_path = '/tmp/temp.csv'
    with open(temp_file_path,'wb') as temp_file:
        temp_file.write(response.content)
        
    
    #establish connection to snowflake
    conn = sf.connect(user=user,password=password,account=account,warehouse=warehouse,database=database,schema=schema, role=role)
    cursor = conn.cursor()
    
    use_schema = f"use schema {schema}"
    cursor.execute(use_schema)
    
    file_format = f"CREATE or REPLACE FILE FORMAT CSV_COMMA TYPE ='CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1;"
    cursor.execute(file_format)
    
    create_stage = f"create or replace stage {stage} file_format = CSV_COMMA;"
    cursor.execute(create_stage)
    
    file_put = f"PUT 'file://{temp_file_path}' @{stage};"
    cursor.execute(file_put)
    
    list_stage = f"list @{stage}"
    cursor.execute(list_stage)
    
    copy_to = f"copy into {schema}.{table} from @{stage}/{file_name} FILE_FORMAT = CSV_COMMA ;"
    cursor.execute(copy_to)
    
    
    #close connection
    cursor.close()
    conn.close()
    