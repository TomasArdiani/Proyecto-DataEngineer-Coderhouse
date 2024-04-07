import os
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
from sqlalchemy import create_engine

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Obtener las variables de entorno
spotify_client_id = os.getenv("SPOTIFY_CLIENT_ID")
spotify_client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
redshift_url = os.getenv("REDSHIFT_URL")
redshift_db = os.getenv("REDSHIFT_DB")
redshift_user = os.getenv("REDSHIFT_USER")
redshift_password = os.getenv("REDSHIFT_PASSWORD")

# Definición de la función para cargar datos en Redshift
def cargar_datos_redshift():
    try:
        # Inicializar el cliente de Spotify
        sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(spotify_client_id, spotify_client_secret))

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

        # Crear motor SQLAlchemy para la conexión a Redshift
        engine = create_engine(f'postgresql://{redshift_user}:{redshift_password}@{redshift_url}:5439/{redshift_db}')
        
        # Cargar datos en Redshift
        songs_df.to_sql('canciones', engine.connect(), if_exists='replace', index=False)
        print("Datos cargados en Redshift con éxito!")

    except Exception as e:
        print("Error al conectar o cargar datos en Redshift:")
        print(e)

# Llamada a la función para cargar datos en Redshift
cargar_datos_redshift()
