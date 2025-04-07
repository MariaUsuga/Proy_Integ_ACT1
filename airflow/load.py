from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

# Configuración para la base de datos olist.db
DATABASE_URL = 'sqlite:////opt/airflow/olist.db'  # Ruta de la base de datos SQLite dentro del contenedor
engine = create_engine(DATABASE_URL)

def load_data_to_db(dataframes):
    """
    Carga los dataframes a las tablas correspondientes de la base de datos.
    Asume que `dataframes` es un diccionario donde las claves son los nombres de las tablas 
    y los valores son los dataframes a cargar.
    """
    for table_name, dataframe in dataframes.items():
        # Cargar cada dataframe en su tabla correspondiente
        dataframe.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Datos cargados en la tabla {table_name}.")

def load_task(**kwargs):
    """
    Tarea de carga en el DAG de Airflow.
    Esta tarea tomará los dataframes extraídos y los cargará en la base de datos.
    """
    # Suponemos que los dataframes han sido previamente generados en el paso de extracción.
    # Si la extracción y carga son pasos separados en el DAG, este código puede necesitar
    # pasar los resultados de la extracción al DAG de carga.
    dataframes = kwargs['ti'].xcom_pull(task_ids='extract_data')  # Obtener los dataframes desde XCom
    load_data_to_db(dataframes)
    return "Datos cargados correctamente."

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'load_data_dag',
    default_args=default_args,
    description='DAG para cargar datos a la base de datos',
    schedule_interval=None,  # Puedes ajustar esto a tu preferencia
)

# Definir el operador de Airflow para ejecutar la tarea de carga
load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_task,
    provide_context=True,  # Para pasar el contexto (como XCom) al operador
    dag=dag,
)

