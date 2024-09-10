from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone  # 한국 시간대 설정을 위해 pytz 사용
from dotenv import load_dotenv
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import mysql.connector
from mysql.connector import Error

# 한국 시간대 설정
KST = timezone('Asia/Seoul')

# Airflow DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 8, hour=0, minute=0, tzinfo=KST),  # 시간대 설정은 여기서
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의 (파이프라인의 이름, 기본 설정, 스케줄 간격 등)
dag = DAG(
    'fine_dust_data_pipeline',
    default_args=default_args,
    description='미세먼지 데이터를 크롤링하고 MySQL에 저장하는 파이프라인',
    schedule_interval=timedelta(days=1),  # 하루마다 실행
    # timezone 인자를 제거해야 함
)


# .env 파일에서 환경 변수를 로드
load_dotenv()

# 환경 변수 가져오기 (API URL, 서비스 키, MySQL 관련 정보)
API_URL = os.getenv('API_URL')  # API URL
SERVICE_KEY = os.getenv('SERVICE_KEY')  # API 서비스 키
DB_HOST = os.getenv('DB_HOST')  # MySQL 호스트
DB_PORT = os.getenv('DB_PORT', 3306)  # MySQL 포트, 기본값은 3306
DB_USER = os.getenv('DB_USER')  # MySQL 사용자명
DB_PASSWORD = os.getenv('DB_PASSWORD')  # MySQL 비밀번호
DB_NAME = os.getenv('DB_NAME')  # MySQL 데이터베이스 이름

# 미세먼지 데이터를 API에서 크롤링하는 함수
def crawl_fine_dust_data(**kwargs):
    """
    미세먼지 데이터를 외부 API로부터 크롤링하여 Pandas DataFrame으로 변환한 후,
    XCom을 사용하여 다음 태스크로 DataFrame을 전달한다.
    """
    # API 호출을 위한 URL 구성
    url = f"{API_URL}?year=2020&pageNo=1&numOfRows=100&returnType=xml&serviceKey={SERVICE_KEY}"
    
    # API 요청 전송
    response = requests.get(url)
    
    # XML 응답을 파싱하여 BeautifulSoup 객체로 변환
    data_bs = BeautifulSoup(response.text, "lxml-xml")
    
    # 'item' 태그 내의 데이터를 모두 추출
    items = data_bs.find_all('item')

    # 데이터를 저장할 리스트 생성
    data_list = []
    
    # 각 'item' 태그 내의 데이터를 파싱하여 리스트에 저장
    for item in items:
        row = {
            "districtName": item.find("districtName").text,  # 지역명
            "dataDate": item.find("dataDate").text,  # 데이터 날짜
            "issueVal": item.find("issueVal").text,  # 발령 값
            "issueTime": item.find("issueTime").text,  # 발령 시간
            "clearVal": item.find("clearVal").text,  # 해제 값
            "clearTime": item.find("clearTime").text,  # 해제 시간
            "issueGbn": item.find("issueGbn").text,  # 발령 구분
            "itemCode": item.find("itemCode").text,  # 항목 코드
        }
        data_list.append(row)

    # 리스트를 Pandas DataFrame으로 변환
    df = pd.DataFrame(data_list)
    
    # 수집된 데이터를 XCom에 저장하여 다음 태스크로 전달
    kwargs['ti'].xcom_push(key='fine_dust_df', value=df)
    
    # 수집된 데이터의 개수를 출력
    print(f"총 {len(data_list)}개의 데이터를 수집했습니다.")

# MySQL에 데이터를 저장하는 함수
def load_data_to_mysql(**kwargs):
    """
    XCom에서 전달받은 DataFrame을 MySQL 데이터베이스에 저장한다.
    테이블이 없으면 생성하고, 데이터가 있으면 삽입한다.
    """
    # XCom에서 DataFrame 가져오기 (이전 태스크에서 전달한 데이터)
    df = kwargs['ti'].xcom_pull(key='fine_dust_df')
    
    try:
        # MySQL에 연결 시도
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )

        # 연결이 성공적으로 되었는지 확인
        if conn.is_connected():
            cursor = conn.cursor()

            # 테이블이 없으면 생성
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS fine_dust (
                districtName VARCHAR(50),  # 지역명
                dataDate DATE,  # 데이터 날짜
                issueVal FLOAT,  # 발령 값
                issueTime TIME,  # 발령 시간
                clearVal FLOAT,  # 해제 값
                clearTime TIME,  # 해제 시간
                issueGbn VARCHAR(20),  # 발령 구분
                itemCode VARCHAR(10)  # 항목 코드
            )
            """)

            # DataFrame의 각 행을 MySQL 테이블에 삽입
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

            # 삽입된 데이터 커밋 (저장)
            conn.commit()
            print("데이터가 MySQL에 성공적으로 저장되었습니다.")
        
    except Error as e:
        # MySQL 연결 또는 작업 중 발생한 오류를 출력
        print(f"MySQL 오류: {e}")
    
    finally:
        # MySQL 연결을 종료
        if conn.is_connected():
            cursor.close()
            conn.close()

def process_csv_and_load_to_mysql():
    file_path = '/opt/airflow/dags/patient_data.csv'
    df = pd.read_csv(file_path, encoding='ISO-8859-1')
    df_filtered = df[df['환자 수'] > 100]
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


# 첫 번째 태스크: 데이터를 크롤링하여 XCom에 저장
crawl_data_task = PythonOperator(
    task_id='crawl_fine_dust_data',  # 태스크 ID
    python_callable=crawl_fine_dust_data,  # 실행할 함수
    provide_context=True,  # XCom을 사용하기 위해 컨텍스트 제공
    dag=dag,  # DAG에 태스크 추가
)

# 두 번째 태스크: XCom에서 데이터를 가져와 MySQL에 저장
load_data_task = PythonOperator(
    task_id='load_data_to_mysql',  # 태스크 ID
    python_callable=load_data_to_mysql,  # 실행할 함수
    provide_context=True,  # XCom을 사용하기 위해 컨텍스트 제공
    dag=dag,  # DAG에 태스크 추가
)

process_csv_task = PythonOperator(
    task_id='process_csv_and_load_to_mysql',
    python_callable=process_csv_and_load_to_mysql,
    dag=dag,
)

# 태스크 의존성 설정: 데이터 크롤링 후 MySQL에 저장하는 작업이 실행되도록 설정
crawl_data_task >> load_data_task
process_csv_task
