from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.extract import extract  # Asumiendo que extract.py está en la carpeta src

# Configuración para la base de datos olist.db
from sqlalchemy import create_engine
engine = create_engine('sqlite:////opt/airflow/olist.db')  # Usamos olist.db dentro del contenedor

def extract_task():
    # Aquí asumes que tienes una carpeta `data/` con los archivos CSV y que se mapearán
    csv_folder = "data"
    csv_table_mapping = {"temperature.csv": "temperature", "other_data.csv": "other_data"}  # Ajusta esto a tus archivos
    public_holidays_url = "https://api.url"  # Ajusta la URL de la API
    dataframes = extract(csv_folder, csv_table_mapping, public_holidays_url)
    
    # Aquí puedes cargar directamente a la base de datos si es necesario.
    return dataframes

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'extract_data_dag',
    default_args=default_args,
    description='DAG to extract data for ETL process',
    schedule_interval=None,  # Puede poner un cronograma, o None si solo se ejecuta manualmente
)

# Definir el operador de Airflow para ejecutar la tarea de extracción
extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_task,
    dag=dag,
)
