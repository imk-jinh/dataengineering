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

# 한국 시간대 설정
KST = timezone('Asia/Seoul')

# .env 파일에서 환경 변수를 로드
load_dotenv()

# 환경 변수 가져오기
API_URL = os.getenv('API_URL')
SERVICE_KEY = os.getenv('SERVICE_KEY')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT', 3306))
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

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

def process_csv_and_load_to_mysql():
    file_path = '/opt/airflow/data/patient_data.csv'
    df = pd.read_csv(file_path)
    df_filtered = df[df['환자수'] > 100]
    df_filtered = df_filtered[['시도', '상병구분', '환자수']]
    
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor()
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS patient_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            시도 VARCHAR(255),
            상병구분 VARCHAR(255),
            환자수 INT
        )
        '''
        cursor.execute(create_table_query)
        
        for index, row in df_filtered.iterrows():
            insert_query = '''
            INSERT INTO patient_data (시도, 상병구분, 환자수)
            VALUES (%s, %s, %s)
            '''
            cursor.execute(insert_query, (row['시도'], row['상병구분'], row['환자수']))
        
        conn.commit()
        print("CSV 데이터가 MySQL에 성공적으로 저장되었습니다.")
    except Error as e:
        print(f"MySQL 오류: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# 정의된 DAG을 사용하여 태스크 생성
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

process_csv_task = PythonOperator(
    task_id='process_csv_and_load_to_mysql',
    python_callable=process_csv_and_load_to_mysql,
    dag=dag,
)

# 태스크 간 의존성 설정
crawl_data_task >> load_data_task
process_csv_task
