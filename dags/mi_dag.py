# Importa las bibliotecas necesarias
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import os
import sys
# Agregar el directorio raíz al PYTHONPATH
sys.path.append(os.path.dirname(__file__))
from Primer_entregable import cargar_datos_spotify, cargar_datos_lastfm

# Define los argumentos predeterminados del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Inicializa el DAG con los argumentos predeterminados
dag = DAG(
    'cargar_datos_redshift',  
    default_args=default_args,  
    description='DAG para cargar datos en Redshift desde Spotify y Last.fm', 
    schedule_interval='@daily',  
    catchup=True  # Habilita el mecanismo de backfill
)

# Define la tarea para cargar datos de Spotify en Redshift
cargar_datos_spotify_task = PythonOperator(
    task_id='cargar_datos_spotify',  
    python_callable=cargar_datos_spotify,  
    dag=dag,  
)

# Define la tarea para cargar datos de Last.fm en Redshift
cargar_datos_lastfm_task = PythonOperator(
    task_id='cargar_datos_lastfm',  
    python_callable=cargar_datos_lastfm,  
    dag=dag,  
)

# Define las dependencias entre las tareas
cargar_datos_spotify_task >> cargar_datos_lastfm_task
