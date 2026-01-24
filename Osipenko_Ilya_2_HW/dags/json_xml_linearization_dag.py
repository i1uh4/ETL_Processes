from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import xml.etree.ElementTree as ET


JSON_URL = "https://raw.githubusercontent.com/LearnWebCode/json-example/master/pets-data.json"
XML_URL = "https://gist.githubusercontent.com/pamelafox/3000322/raw/6cc03bccf04ede0e16564926956675794efe5191/nutrition.xml"
OUTPUT_PATH = "~/Desktop/HSE/ETL_Processes/Osipenko_Ilya_2_HW/data/processed"


def process_json():
    response = requests.get(JSON_URL)
    data = response.json()

    rows = []

    for pet in data.get("pets", []):
        rows.append({
            "name": pet.get("name"),
            "species": pet.get("species"),
            "birth_year": pet.get("birthYear"),
            "fav_foods": ", ".join(pet.get("favFoods", [])),
            "photo": pet.get("photo")
        })

    df = pd.DataFrame(rows)
    df.to_csv(f"{OUTPUT_PATH}/pets_from_json.csv", index=False)


def process_xml():
    response = requests.get(XML_URL)
    root = ET.fromstring(response.text)

    rows = []

    for food in root.findall("food"):
        calories = food.find("calories")
        vitamins = food.find("vitamins")
        minerals = food.find("minerals")

        rows.append({
            "name": food.findtext("name"),
            "mfr": food.findtext("mfr"),
            "serving_value": food.findtext("serving"),
            "serving_units": food.find("serving").attrib.get("units"),
            "calories_total": calories.attrib.get("total") if calories is not None else None,
            "calories_fat": calories.attrib.get("fat") if calories is not None else None,
            "total_fat": food.findtext("total-fat"),
            "saturated_fat": food.findtext("saturated-fat"),
            "cholesterol": food.findtext("cholesterol"),
            "sodium": food.findtext("sodium"),
            "carb": food.findtext("carb"),
            "fiber": food.findtext("fiber"),
            "protein": food.findtext("protein"),
            "vitamin_a": vitamins.findtext("a") if vitamins is not None else None,
            "vitamin_c": vitamins.findtext("c") if vitamins is not None else None,
            "calcium": minerals.findtext("ca") if minerals is not None else None,
            "iron": minerals.findtext("fe") if minerals is not None else None,
        })

    df = pd.DataFrame(rows)
    df.to_csv(f"{OUTPUT_PATH}/nutrition_foods.csv", index=False)


with DAG(
    dag_id="json_xml_linearization",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags={"etl", "json", "xml"}
) as dag:

    json_task = PythonOperator(
        task_id="process_json",
        python_callable=process_json
    )

    xml_task = PythonOperator(
        task_id="process_xml",
        python_callable=process_xml
    )

    json_task >> xml_task
