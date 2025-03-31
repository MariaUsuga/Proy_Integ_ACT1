from typing import Dict
from pandas import DataFrame
from sqlalchemy.engine.base import Engine

def load(data_frames: Dict[str, DataFrame], database: Engine):
    """Load the dataframes into the sqlite database.

    Args:
        data_frames (Dict[str, DataFrame]): A dictionary with keys as the table names
        and values as the dataframes.
        database (Engine): An SQLAlchemy Engine object representing the database connection.
    """

    for table_name, df in data_frames.items():
        try:
            df.to_sql(table_name, database, if_exists='replace', index=False)
            print(f"DataFrame '{table_name}' loaded successfully into the database.")
        except Exception as e:
            print(f"Error loading DataFrame '{table_name}': {e}")