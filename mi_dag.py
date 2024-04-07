# Importa las bibliotecas necesarias
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from primer_entregable import cargar_datos_redshift  

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
    'cargar_datos_spotify_redshift',  
    default_args=default_args,  
    description='DAG para cargar datos de Spotify en Redshift', 
    schedule_interval='@daily',  
)

# Define la tarea que ejecutará la función para cargar datos en Redshift
cargar_datos_task = PythonOperator(
    task_id='cargar_datos_spotify_redshift',  
    python_callable=cargar_datos_redshift,  
    dag=dag,  
)

# Retorna la tarea 
cargar_datos_task
