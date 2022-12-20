import datetime
import os
from datetime import timedelta

from airflow.hooks.filesystem import FSHook
from airflow.utils.context import Context
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    'owner': 'alena',
    'start_date': days_ago(1),
    'depends_on_past': False,
}

dag = DAG(
    dag_id="snowflake_connector",
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
)

query1 = [
    """create or replace stage load_stage""",
    r"""create or replace file format my_csv_format
  type = csv
  record_delimiter = '\n'
  field_delimiter = ','
  skip_header = 1
  null_if = ('NULL', 'null', 'nan')
  empty_field_as_null = true
  FIELD_OPTIONALLY_ENCLOSED_BY = '0x22'
      ENCODING = 'UTF-32'
""",
"""CREATE OR REPLACE TABLE RAW_TABLE (
   ID varchar,
    IOS_App_Id varchar,   
    Title varchar, 
    Developer_Name   varchar,
    Developer_IOS_Id varchar,  
    IOS_Store_Url varchar, 
    Seller_Official_Website varchar,   
    Age_Rating varchar,    
    Total_Average_Rating float,    
    Total_Number_of_Ratings float, 
    Average_Rating_For_Version     float,
    Number_of_Ratings_For_Version  int,
    Original_Release_Date timestamp,
    Current_Version_Release_Date timestamp,    
    Price_USD float,   
    Primary_Genre varchar, 
    All_Genres varchar,    
    Languages varchar,
    Description varchar
);
""",
"""CREATE OR REPLACE TABLE STAGE_TABLE (
   ID varchar,
    IOS_App_Id varchar,   
    Title varchar, 
    Developer_Name   varchar,
    Developer_IOS_Id varchar,  
    IOS_Store_Url varchar, 
    Seller_Official_Website varchar,   
    Age_Rating varchar,    
    Total_Average_Rating float,    
    Total_Number_of_Ratings float, 
    Average_Rating_For_Version     float,
    Number_of_Ratings_For_Version  int,
    Original_Release_Date timestamp,
    Current_Version_Release_Date timestamp,    
    Price_USD float,   
    Primary_Genre varchar, 
    All_Genres varchar,    
    Languages varchar,
    Description varchar
);
""",
"""CREATE OR REPLACE TABLE MASTER_TABLE (
   ID varchar,
    IOS_App_Id varchar,   
    Title varchar, 
    Developer_Name   varchar,
    Developer_IOS_Id varchar,  
    IOS_Store_Url varchar, 
    Seller_Official_Website varchar,   
    Age_Rating varchar,    
    Total_Average_Rating float,    
    Total_Number_of_Ratings float, 
    Average_Rating_For_Version     float,
    Number_of_Ratings_For_Version  int,
    Original_Release_Date timestamp,
    Current_Version_Release_Date timestamp,    
    Price_USD float,   
    Primary_Genre varchar, 
    All_Genres varchar,    
    Languages varchar,
    Description varchar
);
""",
    """put file:////home/alena/Data/Snowflake/data/part1.csv @load_stage
""",
    """copy into RAW_TABLE from @load_stage
    ON_ERROR = CONTINUE
    """
]
query2 = ["""CREATE OR REPLACE STREAM RAW_STREAM ON TABLE STAGE_TABLE;""",
"""CREATE OR REPLACE STREAM STAGE_STREAM ON TABLE MASTER_TABLE;"""]

query3 = ["""INSERT INTO STAGE_TABLE SELECT * FROM RAW_TABLE;""",
          """INSERT INTO MASTER_TABLE SELECT * FROM STAGE_TABLE;"""]

with dag:
    create_tables = SnowflakeOperator(
        task_id="creating_tables_load_data",
        sql=query1,
        snowflake_conn_id="snowflake_default",
    )
    create_streams = SnowflakeOperator(
        task_id="streams",
        sql=query2,
        snowflake_conn_id="snowflake_default")
    loading_data = SnowflakeOperator(
        task_id="load_data_two_tables",
        sql=query3,
        snowflake_conn_id="snowflake_default")

create_tables >> create_streams >> loading_data