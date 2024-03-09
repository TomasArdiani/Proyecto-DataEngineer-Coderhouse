import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

client_id = "c467385725ed4a4f86a682ed1a096d44"
client_secret = "5e337668008a4319b6a333cfcfc7e573"

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id, client_secret))

# Aumenta el límite para obtener más resultados
results = sp.search(q='Oasis', limit=20)

'''
for idx, track in enumerate(results['tracks']['items']):
    # Verifica que el artista principal sea "Oasis" antes de imprimir la información
    if 'Oasis' in [artist['name'] for artist in track['artists'] if artist['type'] == 'artist']:
        # Extrae la información requerida
        name = track['name']
        artist = track['artists'][0]['name']
        album = track['album']['name']
        release_year = track['album']['release_date'][:4]  # Solo obtenemos el año de lanzamiento
        popularity = track['popularity']

        # Imprime la información
        print(f"{idx + 1}. Nombre: {name}")
        print(f"   Artista: {artist}")
        print(f"   Álbum: {album}")
        print(f"   Año de lanzamiento: {release_year}")
        print(f"   Popularidad: {popularity}")
        print("-" * 30)
   
''' 
import psycopg2

# Configuración de Redshift
redshift_url = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
redshift_db = "data-engineer-database"
redshift_user = "tomi_ardiani_coderhouse"

# Leer la contraseña de Redshift desde un archivo
redshift_pwd_file_path = "C:/Users/Tomas/Desktop/Cursos/Curso Data Engineering/Primer entregable/pwd_redshift.txt"
with open(redshift_pwd_file_path, 'r') as f:
    redshift_pwd = f.read()

try:
    # Crear conexión a Redshift
    conn = psycopg2.connect(
        host=redshift_url,
        dbname=redshift_db,
        user=redshift_user,
        password=redshift_pwd,
        port='5439'
    )
    print("Conectado a Redshift con éxito!")
    
except Exception as e:
    print("No es posible conectar a Redshift")
    print(e)
    
    
#Crear la tabla si no existe
with conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tomi_ardiani_coderhouse.canciones
        (
	    id VARCHAR(50) primary key, 
	    artista VARCHAR(255),  
	    cancion VARCHAR(255),    
	    album VARCHAR(100),   
	    popularidad INTEGER, 
	    fecha_lanzamiento date,   
	    duracion_ms INTEGER,   
	    album_img VARCHAR(300) 
        )
    """)
    conn.commit()
    