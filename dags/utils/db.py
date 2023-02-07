import os
from typing import Optional

import psycopg2 as ps
from airflow.models import Connection
from utils.logs import configure_logger

logger = configure_logger()


DEBUG_URLS = {
    'dwh': 'postgres://admin:admin@0.0.0.0:5434/dwh',
    'mrr': 'postgres://admin:admin@0.0.0.0:5434/mrr',
    'oper': 'postgres://admin:admin@0.0.0.0:5433/oper',
    'stg': 'postgres://admin:admin@0.0.0.0:5434/stg',
    'airflow': 'postgres://airflow:airflow@0.0.0.0:5432/airflow'
}


def get_postgres_connection_url(conn_id: Optional[str] = None) -> str:
    conn = Connection.get_connection_from_secrets(conn_id=conn_id)
    return conn.get_uri()


def connect_to_postgres(conn_id: str):

    if os.getenv('PYTHON_BASE_IMAGE') == 'python:3.7-slim-bullseye':
        logger.info(f'[ {conn_id} postgres init ]')
        return ps.connect(get_postgres_connection_url(conn_id))
    else:
        return ps.connect(DEBUG_URLS[conn_id])

