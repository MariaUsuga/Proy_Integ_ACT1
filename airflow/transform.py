from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
from sqlalchemy import text

# Configuración para la base de datos olist.db
DATABASE_URL = 'sqlite:////opt/airflow/olist.db'  # Ruta de la base de datos SQLite dentro del contenedor
engine = create_engine(DATABASE_URL)

def read_query(query_name: str) -> str:
    """Lee el archivo SQL de consulta y lo devuelve como texto."""
    query_path = f"/opt/airflow/dags/sql_queries/{query_name}.sql"
    with open(query_path, 'r') as file:
        return file.read()

def run_query(query_name: str):
    """Ejecuta una consulta SQL sobre la base de datos."""
    query = read_query(query_name)
    return pd.read_sql(text(query), engine)

def transform_data(**kwargs):
    """Realiza las transformaciones necesarias sobre los datos de la base de datos."""
    # Recupera los resultados de las consultas desde el paso anterior utilizando XCom
    ti = kwargs['ti']
    
    # Obtener los resultados de las consultas. Aquí puedes agregar las consultas que necesites.
    # Como ejemplo, ejecutamos la consulta "revenue_by_month_year"
    revenue_by_month_year = run_query("revenue_by_month_year")
    global_ammount_order_status = run_query("global_ammount_order_status")
    
    # Realiza cualquier transformación adicional si es necesario. Por ejemplo:
    # Aquí podrías hacer operaciones sobre los dataframes como 'groupby', 'pivot', etc.
    
    # Guarda los resultados transformados de vuelta a la base de datos (esto es opcional)
    revenue_by_month_year.to_sql('revenue_by_month_year_transformed', engine, if_exists='replace', index=False)
    global_ammount_order_status.to_sql('global_ammount_order_status_transformed', engine, if_exists='replace', index=False)

    print("Transformación de datos completa.")

def transform_task(**kwargs):
    """Tarea de transformación en el DAG de Airflow."""
    transform_data(**kwargs)
    return "Transformación completada."

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'transform_data_dag',
    default_args=default_args,
    description='DAG para transformar datos en la base de datos',
    schedule_interval=None,  # Puedes cambiar esto si necesitas un horario
)

# Definir el operador de Airflow para ejecutar la tarea de transformación
transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_task,
    provide_context=True,  # Para pasar el contexto (como XCom) al operador
    dag=dag,
)
