from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

OUTPUT_DIR = "~/Desktop/HSE/ETL_Processes/Osipenko_Ilya_3_HW/output"
INPUT_DIR = "~/Desktop/HSE/ETL_Processes/Osipenko_Ilya_3_HW/data/IOT-temp.csv"

def transform_data():
    df = pd.read_csv(INPUT_DIR)

    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M').dt.date

    hot_days = df.sort_values('temp', ascending=False).head(5)
    cold_days = df.sort_values('temp', ascending=True).head(5)

    df = df[df['out/in'] == 'In']

    p5 = df['temp'].quantile(0.05)
    p95 = df['temp'].quantile(0.95)
    df = df[(df['temp'] >= p5) & (df['temp'] <= p95)]


    df.to_csv(f"{OUTPUT_DIR}/clean_data.csv", index=False)
    hot_days.to_csv(f"{OUTPUT_DIR}/hot_days.csv", index=False)
    cold_days.to_csv(f"{OUTPUT_DIR}/cold_days.csv", index=False)

with DAG(
    dag_id='etl_temperature_transform',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    transform = PythonOperator(
        task_id='transform_temperature_data',
        python_callable=transform_data
    )

    transform
