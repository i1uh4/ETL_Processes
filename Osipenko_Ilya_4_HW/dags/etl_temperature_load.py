from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

INPUT_FILE = "~/Desktop/HSE/ETL_Processes/Osipenko_Ilya_4_HW/data/IOT-temp.csv"
OUTPUT_DIR = "~/Desktop/HSE/ETL_Processes/Osipenko_Ilya_4_HW/output"
TARGET_DIR = "~/Desktop/HSE/ETL_Processes/Osipenko_Ilya_4_HW/target"

CLEAN_DATA = f"{OUTPUT_DIR}/clean_data.csv"
HISTORY_TARGET = f"{TARGET_DIR}/temperature_history.csv"
INCREMENT_TARGET = f"{TARGET_DIR}/temperature_increment.csv"


def transform_data():
    df = pd.read_csv(INPUT_FILE)

    df['noted_date'] = pd.to_datetime(
        df['noted_date'],
        format='%d-%m-%Y %H:%M'
    ).dt.date

    hot_days = df.sort_values('temp', ascending=False).head(5)
    cold_days = df.sort_values('temp', ascending=True).head(5)

    df = df[df['out/in'] == 'In']

    p5 = df['temp'].quantile(0.05)
    p95 = df['temp'].quantile(0.95)
    df = df[(df['temp'] >= p5) & (df['temp'] <= p95)]

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df.to_csv(CLEAN_DATA, index=False)
    hot_days.to_csv(f"{OUTPUT_DIR}/hot_days.csv", index=False)
    cold_days.to_csv(f"{OUTPUT_DIR}/cold_days.csv", index=False)


def load_full_history():
    """Полная загрузка исторических данных"""
    df = pd.read_csv(CLEAN_DATA)

    os.makedirs(TARGET_DIR, exist_ok=True)

    df.to_csv(HISTORY_TARGET, index=False)


def load_increment(**context):
    """Инкрементальная загрузка за последние N дней"""
    df = pd.read_csv(CLEAN_DATA)
    dag_run = context.get("dag_run")

    load_date = pd.to_datetime(dag_run.conf["load_date"]).date()
    df['noted_date'] = pd.to_datetime(df['noted_date']).dt.date

    increment_df = df[df['noted_date'] >= load_date]

    os.makedirs(TARGET_DIR, exist_ok=True)
    if os.path.exists(INCREMENT_TARGET):
        existing_df = pd.read_csv(INCREMENT_TARGET)
        result_df = pd.concat([existing_df, increment_df]).drop_duplicates()
    else:
        result_df = increment_df

    result_df.to_csv(INCREMENT_TARGET, index=False)



with DAG(
    dag_id="etl_temperature_load",
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=["etl", "load"]
) as dag:

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    load_history = PythonOperator(
        task_id="load_full_history",
        python_callable=load_full_history
    )

    load_incremental = PythonOperator(
        task_id="load_incremental",
        python_callable=load_increment
    )

    transform >> [load_history, load_incremental]
