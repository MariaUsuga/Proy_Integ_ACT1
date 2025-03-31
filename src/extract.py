from typing import Dict
import requests
import pandas as pd
import sys
import json
from pandas import DataFrame, read_csv, to_datetime
import traceback

def temp() -> DataFrame:
    """Get the temperature data.
    Returns:
        DataFrame: A dataframe with the temperature data.
    """
    return read_csv("data/temperature.csv")

def get_public_holidays(public_holidays_url: str, year: str) -> DataFrame:
    """Get the public holidays for the given year for Brazil."""
    
    try:
        # Usa f-string para construir la URL
        url = f"{public_holidays_url}/{year}/BR"

        
        # Solicitar los días festivos desde la API
        resp = requests.get(url)
        
        # Verificar si la respuesta fue exitosa
        resp.raise_for_status()

        # Transformar la respuesta JSON en un DataFrame
        df = pd.DataFrame(json.loads(resp.text))

        # Eliminar las columnas innecesarias si existen
        if 'counties' in df.columns:
            df.drop(columns=['counties'], inplace=True)
        if 'types' in df.columns:
            df.drop(columns=['types'], inplace=True)

        # Convertir la columna 'date' a datetime
        df['date'] = pd.to_datetime(df['date'])

        return df
    
    except requests.exceptions.HTTPError as e:
        # Manejo de errores HTTP
        print(f"HTTP error occurred: {e}")
        print(f"URL solicitada: {url}")
        sys.exit()  # Detener la ejecución si ocurre un error HTTP

    except Exception as e:
        # Captura de excepciones más generales
        print(f"An error occurred: {e}")
        traceback.print_exc()  # Imprimir el traceback completo del error
        sys.exit()  # Detener la ejecución si ocurre otro tipo de error


def extract(
    csv_folder: str, csv_table_mapping: Dict[str, str], public_holidays_url: str
) -> Dict[str, DataFrame]:
    """Extract the data from the csv files and load them into the dataframes.
    Args:
        csv_folder (str): The path to the csv's folder.
        csv_table_mapping (Dict[str, str]): The mapping of the csv file names to the table names.
        public_holidays_url (str): The url to the public holidays.
    Returns:
        Dict[str, DataFrame]: A dictionary with keys as the table names and values as the dataframes.
    """
    dataframes = {}
    
    # Extraer los datos de los archivos CSV
    for csv_file, table_name in csv_table_mapping.items():
        file_path = f"{csv_folder}/{csv_file}"
        try:
            # Asegúrate de que el archivo exista
            dataframes[table_name] = read_csv(file_path)
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            sys.exit()  # Detener si no se encuentra el archivo
    
    # Obtener los días festivos desde la URL proporcionada
    holidays = get_public_holidays(public_holidays_url, "2017")
    
    # Agregar los días festivos al diccionario de DataFrames
    dataframes["public_holidays"] = holidays
    
    return dataframes
