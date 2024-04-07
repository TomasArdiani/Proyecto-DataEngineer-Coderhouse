# Estoy usando una imagen oficial de Python como imagen base
FROM python:3.9-slim

# Establece el directorio de trabajo en el contenedor
WORKDIR /usr/src/app

# Copia el archivo requirements.txt al contenedor en /usr/src/app
COPY requirements.txt .

# Instala los paquetes necesarios especificados en requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copia el script al contenedor en /usr/src/app
COPY primer_entregable.py .

# Ejecuta el script cuando se lance el contenedor
CMD ["python", "./primer_entregable.py"]
