import json
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

import requests


DAG_FOLDER = "/opt/airflow/dags"

def _get_AQI_data():
    # API_KEY = os.environ.get("WEATHER_API_KEY")
    API_KEY = Variable.get("AQI_api_dag")
   
    url = "http://api.airvisual.com/v2/city?city=Sai Mai&state=Bangkok&country=Thailand&key="+API_KEY
    response = requests.get(url)
    print(response.url)

    data = response.json()
    print(data)

    with open(f"{DAG_FOLDER}/data.json", "w") as f:
        json.dump(data, f)

#ตรวจสอบว่ามีข้อมูลหรือไม่
def _validate_data():
    with open(f"{DAG_FOLDER}/data.json", "r") as f:
        data = json.load(f)

        # ตรวจสอบว่า 'pollution' และ 'weather' มีข้อมูลหรือไม่
        assert data["data"]["current"].get("pollution") is not None, "Pollution data is missing"
        assert data["data"]["current"].get("weather") is not None, "Weather data is missing"
        
        # ตรวจสอบข้อมูล air_quality (pollution)
        pollution_data = data["data"]["current"]["pollution"]
        assert "aqius" in pollution_data, "Missing AQI US value in pollution data"
        assert "aqicn" in pollution_data, "Missing AQI CN value in pollution data"

        # ตรวจสอบค่าของ AQI US และ AQI CN ว่ามีค่าที่เหมาะสม
        aqius = pollution_data["aqius"]
        aqicn = pollution_data["aqicn"]
             
#ตรวจสอบค่า AQI ให้อยู่ในช่วงที่เหมาะสม
def _validate_AQI():
    with open(f"{DAG_FOLDER}/data.json", "r") as f:
        data = json.load(f)

        pollution_data = data["data"]["current"]["pollution"]

        # ตรวจสอบค่าของ AQI US และ AQI CN ว่ามีค่าที่เหมาะสม
        aqius = pollution_data["aqius"]
        aqicn = pollution_data["aqicn"]
        
        assert 0 <= aqius <= 500, f"AQI US value out of range: {aqius}"
        assert 0 <= aqicn <= 500, f"AQI CN value out of range: {aqicn}"
       
#สร้างตาราง pollution
def _create_pollution_data_table():
    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
   
    sql = """
        CREATE TABLE IF NOT EXISTS pollution_data (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMPTZ NOT NULL,
        aqius INT NOT NULL,
        mainus VARCHAR(10) NOT NULL,
        aqicn INT NOT NULL,
        maincn VARCHAR(10) NOT NULL

        );
    """
    cursor.execute(sql)
    connection.commit()

#โหลดข้อมูล AQI ลงตาราง pollution
def _load_pollution_data_to_postgres():

    #คำสั่งไว้ connec เสมอ
    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="postgres"
    )    
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    #//////////////////////////////////////////

    # โหลดข้อมูลจากไฟล์ JSON เปิดไฟล์ ตามพาร์ท DAG_FOLDER ชื่อไฟล์ว่า data.json  โหลดข้อมูลเหล่านั้นมาไว้ใน data แพคเตรียมเอาไปใช้
    with open(os.path.join(DAG_FOLDER, "data.json"), "r") as f:
        data = json.load(f)
    #//////////////////////////////////////////

    # ดึงค่าที่ต้องการจาก JSON
    pollution = data["data"]["current"]["pollution"]
    timestamp = pollution["ts"]
    aqius = pollution["aqius"]
    mainus = pollution["mainus"]
    aqicn = pollution["aqicn"]
    maincn = pollution["maincn"]

    # ใช้ parameterized query เพื่อป้องกัน SQL Injection
    sql = """
        INSERT INTO pollution_data (timestamp, aqius, mainus, aqicn, maincn) 
        VALUES ( %s, %s, %s, %s, %s)
    """
    cursor.execute(sql, ( timestamp, aqius, mainus, aqicn, maincn))
    connection.commit()
    cursor.close()
    connection.close()

# เมล์จ้า
default_args = {
    "email": ["gunyaphut.s@gmail.com"],
    "retries": 3,
}

#/////////// ส่วนที่ 2 มัดรวมส่วน 1 เพื่อสร้างแทคชื่อ เอาไปทำสะพาน
with DAG(
    "AQI_api_dag",
    schedule="30 * * * *",
    start_date=timezone.datetime(2025, 4, 1),
    tags=["AQI"],
):
    start = EmptyOperator(task_id="start")

    get_AQI_data = PythonOperator(
        #ตั้งชื่อไอดี เพื่อเอาไปมัดรวม สร้างสะพาน
        task_id="get_AQI_data",
        #อ้างถึงฟังค์ชั่นข้างบนส่วน 1
        python_callable=_get_AQI_data,
    )

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=_validate_data,
    )

    validate_AQI = PythonOperator(
        task_id="validate_AQI",
        python_callable=_validate_AQI,
    )

    create_pollution_data_table = PythonOperator(
        task_id="create_pollution_data_table",
        python_callable=_create_pollution_data_table,
    )

    load_pollution_data_to_postgres = PythonOperator(
        task_id="load_pollution_data_to_postgres",
        python_callable=_load_pollution_data_to_postgres,
    )

    send_email = EmailOperator(
        task_id="send_email",
        to=["kan@odds.team"],
        subject="Finished getting open weather data",
        html_content="Done",
    )

    end = EmptyOperator(task_id="end")

    #/////////////// ส่วน 3 สะพาน แสดงเส้นทางว่าต้องทำไรก่อน
    start >> get_AQI_data >> [validate_data ,validate_AQI] >> load_pollution_data_to_postgres >> send_email >> end
    start >> create_pollution_data_table >> load_pollution_data_to_postgres