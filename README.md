# Proy_Integ_ACT1

# PROYECTO INTEGRADO IV

# Entrega 1:

Proyecto de Pipeline ELT: Comercio Electrónico (2016-2018)

Descripción
Este proyecto tiene como objetivo construir un pipeline ELT que recopile, transforme y almacene datos de comercio electrónico de la temporada 2016-2018. Además, se analizará la relación entre los días festivos y las fechas de las órdenes de compra mediante visualizaciones.

Estructura del Proyecto:
El proyecto consta de tres fases principales:

1. Extracción
La fase de extracción de datos implica completar las funciones marcadas con TODO en el módulo src/extract.py. Estas funciones están diseñadas para recolectar datos de diferentes fuentes y prepararlos para ser cargados.

2. Carga
Una vez que los datos hayan sido extraídos correctamente, se almacenarán en un Data Warehouse. Este proyecto utiliza SQLite como motor de base de datos para simplificar la configuración y pruebas. En entornos empresariales más grandes, opciones como Snowflake suelen ser preferidas.
Tareas:
Completa todas las funciones marcadas con TODO en el módulo src/load.py. Asegúrate de que los datos se carguen correctamente en SQLite.

3. Se hace la Verificación:
Para garantizar que el código cumple con los requisitos, utilizamos el siguiente comando para probar el módulo:
pytest tests/test_extract.py

# Entrega 2:

Transformación

Teniendo los datos almacenados en el Data Warehouse, se hacen consultas y transformaciones.

Se completan todos los scripts .sql con la marca TODO dentro de la carpeta queries/.


# Entrega 3:

Se sube la carpeta airflow, donde se trabajan los archivos y Dags - Airflow, Docker.

Proyecto con Apache Airflow

Este proyecto usa Apache Airflow para orquestar flujos de trabajo de datos. Airflow se ejecuta dentro de un contenedor Docker y gestiona un pipeline que incluye tareas de extracción, transformación y carga de datos (ETL).

Estructura del Proyecto
airflow/
│
├── dags/               # Carpeta donde se encuentran los archivos de los DAGs
│   ├── extract.py      # Tarea de extracción de datos
│   ├── transform.py    # Tarea de transformación de datos
│   └── load.py         # Tarea de carga de datos
│
├── airflow.cfg         # Configuración de Airflow
└── Dockerfile          # Dockerfile para crear la imagen de Airflow


