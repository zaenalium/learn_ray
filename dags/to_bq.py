from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
import pendulum
import os
import glob
from dotenv import load_dotenv
import pandas_gbq
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv('.env')

credentials = service_account.Credentials.from_service_account_file(
    os.environ['BQ_CRED'])

url1 = 'https://archive.ics.uci.edu/static/public/352/online+retail.zip'
url2 = 'https://archive.ics.uci.edu/static/public/292/wholesale+customers.zip'

with DAG(
    dag_id="data_to_bq",
    start_date=pendulum.datetime(2024, 12, 25, tz="UTC"),
    schedule=None,
    catchup=False
):

    get_url1 = BashOperator(
        task_id="get_url1",
        bash_command=f"wget {url1} -O ~/airflow/online+retail.zip")
    
    unzip_data1 = BashOperator(task_id="unzip_file",
        bash_command="unzip -o ~/airflow/online+retail.zip -d  ~/airflow/online_retail")

    # The data are read and directly save to BigQuery because of the size is small
    def read_data_save_to_gbq1():
        df = pd.read_excel('~/airflow/online_retail/Online Retail.xlsx')
        df['InvoiceNo'] = df['InvoiceNo'].astype(str)
        df['StockCode'] = df['StockCode'].astype(str)
        df['Description'] = df['Description'].astype(str)

        pandas_gbq.to_gbq(df, 'tridorian_test.online_retail', credentials = credentials, if_exists = 'replace')
    
    df_to_gbq = PythonOperator(task_id="read_data_save_to_gbq1", python_callable=read_data_save_to_gbq1)


    get_url1 >> unzip_data1 >> df_to_gbq

    get_url2 = BashOperator(
        task_id="get_url2",
        bash_command=f"wget {url2} -O ~/airflow/wholesale+customers.zip")
    
    # The data are read and directly save to BigQuery because of the size is small
    def read_data_save_to_gbq2():
        df = pd.read_csv('~/airflow/wholesale+customers.zip')
        pandas_gbq.to_gbq(df, 'tridorian_test.wholesale_customers', credentials = credentials, if_exists = 'replace')
    
    df_to_gbq2 = PythonOperator(task_id="read_data_save_to_gbq2", python_callable=read_data_save_to_gbq2)
 
    get_url2 >> df_to_gbq2
    