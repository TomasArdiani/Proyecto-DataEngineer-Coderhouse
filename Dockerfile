# Usa la imagen oficial de Apache Airflow como base
FROM apache/airflow:2.2.3

# Establece el directorio de trabajo en el contenedor
WORKDIR /usr/src/app

# Copia el archivo requirements.txt al contenedor en /usr/src/app
COPY requirements.txt .

# Instala los paquetes necesarios especificados en requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copia el script al contenedor en /usr/src/app
COPY Primer_entregable.py .

# Define las variables de entorno para Airflow
ENV AIRFLOW_HOME=/usr/local/airflow

# Ejecuta el script cuando se lance el contenedor
CMD ["python", "./Primer_entregable.py"]

