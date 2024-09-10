from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from dotenv import load_dotenv
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import mysql.connector
from mysql.connector import Error
# import matplotlib.pyplot as plt

# 한국 시간대 설정
KST = timezone('Asia/Seoul')

# Airflow DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 8, hour=0, minute=0, tzinfo=KST),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fine_dust_data_pipeline',
    default_args=default_args,
    description='미세먼지 데이터를 크롤링하고 MySQL에 저장 및 시각화하는 파이프라인',
    schedule_interval=timedelta(days=1),
)

# .env 파일에서 환경 변수를 로드
load_dotenv()

# 환경 변수 가져오기
API_URL = os.getenv('API_URL')
SERVICE_KEY = os.getenv('SERVICE_KEY')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', 3306)
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

def crawl_fine_dust_data(**kwargs):
    # 미세먼지 데이터를 API로부터 크롤링
    url = f"{API_URL}?year=2020&pageNo=1&numOfRows=100&returnType=xml&serviceKey={SERVICE_KEY}"
    response = requests.get(url)
    data_bs = BeautifulSoup(response.text, "lxml-xml")
    items = data_bs.find_all('item')
    data_list = []

    for item in items:
        row = {
            "districtName": item.find("districtName").text,
            "dataDate": item.find("dataDate").text,
            "issueVal": item.find("issueVal").text,
            "issueTime": item.find("issueTime").text,
            "clearVal": item.find("clearVal").text,
            "clearTime": item.find("clearTime").text,
            "issueGbn": item.find("issueGbn").text,
            "itemCode": item.find("itemCode").text,
        }
        data_list.append(row)

    df = pd.DataFrame(data_list)
    kwargs['ti'].xcom_push(key='fine_dust_df', value=df)
    print(f"총 {len(data_list)}개의 데이터를 수집했습니다.")

def load_data_to_mysql(**kwargs):
    df = kwargs['ti'].xcom_pull(key='fine_dust_df')
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS fine_dust (
                districtName VARCHAR(50),
                dataDate DATE,
                issueVal FLOAT,
                issueTime TIME,
                clearVal FLOAT,
                clearTime TIME,
                issueGbn VARCHAR(20),
                itemCode VARCHAR(10)
            )
            """)
            for _, row in df.iterrows():
                cursor.execute("""
                INSERT INTO fine_dust (districtName, dataDate, issueVal, issueTime, clearVal, clearTime, issueGbn, itemCode)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['districtName'],
                    row['dataDate'],
                    row['issueVal'],
                    row['issueTime'],
                    row['clearVal'],
                    row['clearTime'],
                    row['issueGbn'],
                    row['itemCode']
                ))
            conn.commit()
            print("데이터가 MySQL에 성공적으로 저장되었습니다.")
    except Error as e:
        print(f"MySQL 오류: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# def visualize_fine_dust_data(**kwargs):
#     try:
#         conn = mysql.connector.connect(
#             host=DB_HOST,
#             port=DB_PORT,
#             user=DB_USER,
#             password=DB_PASSWORD,
#             database=DB_NAME
#         )
#         if conn.is_connected():
#             query = """
#             SELECT dataDate, AVG(issueVal) as avg_issueVal
#             FROM fine_dust
#             GROUP BY dataDate
#             ORDER BY dataDate;
#             """
#             df = pd.read_sql(query, conn)
#             plt.figure(figsize=(10, 6))
#             plt.plot(df['dataDate'], df['avg_issueVal'], marker='o', linestyle='-', color='b')
#             plt.title('Average Fine Dust Issue Value Over Time')
#             plt.xlabel('Date')
#             plt.ylabel('Average Issue Value')
#             plt.xticks(rotation=45)
#             plt.grid(True)
#             plt.savefig('/tmp/fine_dust_report.png')
#             print("시각화가 성공적으로 완료되었습니다. /tmp/fine_dust_report.png에 저장되었습니다.")
#     except Error as e:
#         print(f"MySQL 오류: {e}")
#     finally:
#         if conn.is_connected():
            # conn.close()

crawl_data_task = PythonOperator(
    task_id='crawl_fine_dust_data',
    python_callable=crawl_fine_dust_data,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data_to_mysql',
    python_callable=load_data_to_mysql,
    dag=dag,
)

# visualize_data_task = PythonOperator(
#     task_id='visualize_fine_dust_data',
#     python_callable=visualize_fine_dust_data,
#     dag=dag,
# )

crawl_data_task >> load_data_task 
