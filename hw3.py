from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract(latitude, longitude):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": 60,
        "forecast_days": 0,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code"
        ],
        "timezone": "America/Los_Angeles"
    }
    response = requests.get(url, params=params)
    data = response.json()

    records = []
    for i in range(len(data["daily"]["time"])):
        records.append([
            latitude,
            longitude,
            data["daily"]["time"][i],
            data["daily"]["temperature_2m_max"][i],
            data["daily"]["temperature_2m_min"][i],
            data["daily"]["precipitation_sum"][i],
            data["daily"]["weather_code"][i]
        ])
    return records


@task
def load(records):
    cur = return_snowflake_conn()
    target_table = "DEMO_DB.RAW.weather_data"
    try:
        cur.execute("BEGIN;")

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                latitude FLOAT,
                longitude FLOAT,
                date DATE,
                temp_max FLOAT,
                temp_min FLOAT,
                precipitation FLOAT,
                weather_code INT,
                PRIMARY KEY (latitude, longitude, date)
            )
        """)

        cur.execute(f"DELETE FROM {target_table}")

        for r in records:
            sql = f"""
                INSERT INTO {target_table}
                (latitude, longitude, date, temp_max, temp_min, precipitation, weather_code)
                VALUES ({r[0]}, {r[1]}, '{r[2]}', {r[3]}, {r[4]}, {r[5]}, {r[6]})
            """
            cur.execute(sql)

        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id='weather_data_pipeline',
    start_date=datetime(2026, 9, 16),
    catchup=False,
    tags=['homework3'],
    schedule='0 2 * * *'
) as dag:
    latitude = Variable.get("latitude")
    longitude = Variable.get("longitude")

    records = extract(latitude, longitude)
    load(records)