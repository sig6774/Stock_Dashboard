import sys 
sys.path.append("/home/ubuntu/Stock_Dashboard/Streamlit/")
import transform_index
import db_info
import pandas as pd
import psycopg2
import os
import FinanceDataReader as fdr

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import os
from io import StringIO


# AWS
access_key = os.environ.get("AWS_ACCESS_KEY_ID")
secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

s3_bucket_name = "stock-data-warehouse"
s3_object_key = "Index-Data/"

parent_dir = "/home/ubuntu/index_data"

# 오늘 날짜 갱신
now = datetime.now()

today_date_str = now.strftime("%Y-%m-%d")

# 원본 Index 이름
origin_index_list = transform_index.index_list

# 전처리가 완료된 index이름
new_index_list = transform_index.transform_special_char(
    transform_index.index_list)


# db 정보
db_config = {
    'host': db_info.host,
    'database': db_info.database,
    'user': db_info.user,
    'password': db_info.password,
    'port': db_info.port
}

# 데이터베이스 연결
conn = psycopg2.connect(**db_config)

# 커서 생성
cur = conn.cursor()


def insert_index_data(dataframe_name, dataframe, db_cursor, db_conn):
    # create DB 
    create_template = """
    CREATE TABLE IF NOT EXISTS finance.{table_name} (
        date VARCHAR(100) PRIMARY KEY,
        open VARCHAR(100),
        high VARCHAR(100),
        low VARCHAR(100),
        close VARCHAR(100),
        volume VARCHAR(100)
    );
    """
    create_query = create_template.format(table_name=dataframe_name.split(".")[0])

    # Insert DB
    insert_template = """
    INSERT INTO finance.{table_name} VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (DATE) DO NOTHING
    """

    # 쿼리 템플릿 포맷팅 (테이블 이름 삽입)
    insert_query = insert_template.format(table_name=dataframe_name.split(".")[0])

    for k in range(len(dataframe)):

        db_cursor.execute(create_query)

        date = str(dataframe.loc[k]["Date"])
        open = str(dataframe.loc[k]["Open"])
        high = str(dataframe.loc[k]["High"])
        low = str(dataframe.loc[k]["Low"])
        close = str(dataframe.loc[k]["Close"])
        volume = str(dataframe.loc[k]["Volume"])

        db_cursor.execute(insert_query, (date, open, high, low, close, volume))

    db_conn.commit()


# Store Batch Data
def store_index_data(today_date, index_name: list, db_cursor, db_conn):

    for i in range(len(index_name)):

        folder_path = f"{parent_dir}/{index_name[i]}"

        # 주요 국가의 Index 중 S&P만 특수문자를 표시하고 있어 처리하기 위해 함수를 만드는 것은 비효율적이라 작접 값 입력
        if index_name[i] == "S&P500":
            dataframe_name = "SP500_df.csv"
        else:
            dataframe_name = index_name[i] + "_df.csv"

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        dataframe = fdr.DataReader(index_name[i], "2000-01-01", today_date)
        reset_dataframe = dataframe.reset_index()

        # 백업용으로 Local에 저장
        reset_dataframe.to_csv(f"{folder_path}/{dataframe_name}",
                               encoding="utf-8",
                               index=False)

        # Insert Database
        insert_index_data(dataframe_name, reset_dataframe, db_cursor, db_conn)

        # Upload_S3
        csv_buffer = StringIO()
        reset_dataframe.to_csv(csv_buffer)
        s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=s3_bucket_name,
                             Key=s3_object_key+f"{dataframe_name}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # UTC 기준으로 한국 시간 08:30 (UTC 23:30)에 실행하도록 설정
    'start_date': datetime(2024, 1, 2, 23, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'index_data_processing',
    default_args=default_args,
    description='Process and store index data to PostgreSQL',
    # 매일 반복 실행 설정
    schedule_interval='30 23 * * *',  # 매일 23:30 UTC (한국 시간 08:30)
)

# store_index_data 함수를 태스크로 변환
store_index_data_task = PythonOperator(
    task_id='store_index_data',
    python_callable=store_index_data,  # 호출할 함수
    op_kwargs={'today_date': today_date_str, 'index_name': origin_index_list,
               'db_cursor': cur, 'db_conn': conn},
    dag=dag,
)
# 태스크 의존성 설정
# preprocess_csv_task >> insert_into_postgres_task
store_index_data_task
