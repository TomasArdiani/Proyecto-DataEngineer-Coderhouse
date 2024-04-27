import os
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import requests
from sqlalchemy import create_engine
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Obtener las variables de entorno
spotify_client_id = os.getenv("SPOTIFY_CLIENT_ID")
spotify_client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
redshift_url = os.getenv("REDSHIFT_URL")
redshift_db = os.getenv("REDSHIFT_DB")
redshift_user = os.getenv("REDSHIFT_USER")
redshift_password = os.getenv("REDSHIFT_PASSWORD")
email_username = os.getenv("EMAIL_USERNAME")
email_password = os.getenv("EMAIL_PASSWORD")
lastfm_api_key = os.getenv("LASTFM_API_KEY")  
lastfm_country = os.getenv("LASTFM_COUNTRY")
email_recipient = os.getenv("EMAIL_RECIPIENT")  # Nueva variable de entorno para la dirección de correo electrónico del destinatario

# Crear motor SQLAlchemy para la conexión a Redshift
engine = create_engine(f'postgresql://{redshift_user}:{redshift_password}@{redshift_url}:5439/{redshift_db}')

# Obtener la URL de la API de Last.fm desde las variables de entorno
lastfm_api_url = os.getenv("LASTFM_API_URL")

# Definir la función para cargar datos de Spotify en Redshift
def cargar_datos_spotify():
    try:
        # Inicializar el cliente de Spotify
        sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=spotify_client_id, client_secret=spotify_client_secret))

        # Obtener resultados de búsqueda de Spotify
        results = sp.search(q='Oasis', limit=20)

        # Construir un diccionario con los datos de las canciones
        songs_data = {
            'id': [],
            'artista': [],
            'cancion': [],
            'album': [],
            'popularidad': [],
            'fecha_lanzamiento': [],
            'duracion_ms': [],
            'album_img': []
        }

        for track in results['tracks']['items']:
            # Extraer información de la canción
            songs_data['id'].append(track['id'])
            songs_data['artista'].append(track['artists'][0]['name'])
            songs_data['cancion'].append(track['name'])
            songs_data['album'].append(track['album']['name'])
            songs_data['popularidad'].append(track['popularity'])
            songs_data['fecha_lanzamiento'].append(track['album']['release_date'])
            songs_data['duracion_ms'].append(track['duration_ms'])
            songs_data['album_img'].append(track['album']['images'][0]['url'])

        # Convertir el diccionario en un DataFrame
        songs_df = pd.DataFrame(songs_data)

        # Definir clave primaria compuesta
        songs_df['id_artista'] = songs_df['id'] + '_' + songs_df['artista']

        # Eliminar columnas no necesarias
        songs_df.drop(columns=['id', 'artista'], inplace=True)

        # Cargar datos en Redshift
        songs_df.to_sql('canciones', engine.connect(), if_exists='replace', index=False)
        print("Datos cargados en Redshift desde Spotify con éxito!")

    except Exception as e:
        print("Error al conectar o cargar datos en Redshift desde Spotify:")
        print(e)

# Definir la función para cargar datos de Last.fm en Redshift
def cargar_datos_lastfm():
    try:
        # Definir los parámetros de la solicitud a la API de Last.fm
        params = {
            "method": "geo.getTopArtists",
            "country": lastfm_country,
            "api_key": lastfm_api_key,
            "format": "json"
        }

        # Realizar la solicitud a la API de Last.fm
        response = requests.get(lastfm_api_url, params=params)
        data = response.json()

        # Extraer los datos relevantes
        if "topartists" in data and "artist" in data["topartists"]:
            artists = data["topartists"]["artist"]
            artists_data = {
                "name": [artist["name"] for artist in artists],
                "listeners": [artist["listeners"] for artist in artists]
            }

            # Convertir los datos en un DataFrame de Pandas
            artists_df = pd.DataFrame(artists_data)

            # Cargar datos en Redshift
            artists_df.to_sql("top_artists", engine, if_exists="replace", index=False)
            print("Datos cargados en Redshift desde Last.fm con éxito!")
            
            # Verificar cambio en el artista número 1
            verificar_cambio_artista_lastfm(artists_df)

        else:
            print("No se encontraron datos de artistas principales para el país especificado.")

    except Exception as e:
        print("Error al conectar o cargar datos en Redshift desde Last.fm:")
        print(e)

# Función para verificar cambio en el artista número 1 de Last.fm
def verificar_cambio_artista_lastfm(artists_df):
    try:
        # Leer el artista anterior desde un archivo
        with open("artista_anterior.txt", "r") as file:
            artista_anterior = file.read().strip()
        
        # Obtener el nuevo artista número 1
        nuevo_artista = artists_df.loc[0, "name"]
        
        # Comparar con el artista anterior
        if nuevo_artista != artista_anterior:
            # Si hay cambio, guardar el nuevo artista en el archivo
            with open("artista_anterior.txt", "w") as file:
                file.write(nuevo_artista)
            
            # Enviar alerta por correo electrónico
            enviar_alerta_cambio_artista(nuevo_artista, artista_anterior)
    
    except FileNotFoundError:
        # Si el archivo no existe, escribir el artista actual en el archivo
        with open("artista_anterior.txt", "w") as file:
            file.write(artists_df.loc[0, "name"])
            
    except Exception as e:
        print("Error al verificar el cambio en el artista número 1:")
        print(e)

# Función para enviar alerta por correo electrónico
def enviar_alerta_cambio_artista(nuevo_artista, artista_anterior):
    try:
        # Configurar conexión SMTP
        smtp_server = 'smtp.example.com'  # Dirección del servidor SMTP
        smtp_port = 587  # Puerto del servidor SMTP (ejemplo: 587 para TLS)
        smtp_username = email_username  # Nombre de usuario del correo electrónico
        smtp_password = email_password  # Contraseña del correo electrónico
        
        # Dirección de correo electrónico del remitente y destinatario
        from_email = email_username
        to_email = email_recipient
        
        # Crear instancia de MIMEMultipart
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = to_email
        msg['Subject'] = 'Alerta de cambio en el artista número 1 de Last.fm'
        
        # Crear el cuerpo del mensaje
        body = f"El artista top 1 en {lastfm_country} ha cambiado.\nAnterior: {artista_anterior}\nNuevo: {nuevo_artista}"
        msg.attach(MIMEText(body, 'plain'))
        
        # Establecer conexión SMTP y enviar el correo electrónico
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # Iniciar TLS para la conexión segura
            server.login(smtp_username, smtp_password)  # Autenticarse en el servidor SMTP
            server.sendmail(from_email, to_email, msg.as_string())  # Enviar el correo electrónico
            print("Correo electrónico de alerta enviado con éxito!")
    
    except Exception as e:
        print("Error al enviar el correo electrónico de alerta:")
        print(e)

# Ejecutar la función principal
if __name__ == "__main__":
    cargar_datos_spotify()
    cargar_datos_lastfm()
