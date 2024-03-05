import psycopg2
import requests

# Aqui colocamos las credenciales de la base de datos Redshift
dbname = 'data-engineer-database'
user = 'tomi_ardiani_coderhouse'
password = '4lsmFL6p85'
host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
port = '5439'

# URL de la API donde consultamos los datos
url = "https://api.covidtracking.com/v1/states/current.json"

# Aqui vamos a hacer la solicitud a la API
response = requests.get(url)

# Verificar el estado de la respuesta
if response.status_code == 200:
    data = response.json()
    print(data)
    
    # Conexión a la base de datos Redshift si la respuesta es exitosa
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )

    # Crear el cursor
    cursor = conn.cursor()

    # Lista de columnas de la respuesta
    columns = ['date', 'state', 'positive', 'probableCases', 'negative', 'pending', 'totalTestResults', 'hospitalizedCurrently', 'hospitalizedCumulative', 'inIcuCurrently', 'inIcuCumulative', 'onVentilatorCurrently', 'onVentilatorCumulative', 'recovered', 'lastUpdateEt', 'dateModified', 'checkTimeEt', 'death', 'hospitalized', 'hospitalizedDischarged', 'dateChecked', 'totalTestsViral', 'positiveTestsViral', 'negativeTestsViral', 'positiveCasesViral', 'deathConfirmed', 'deathProbable', 'totalTestEncountersViral', 'totalTestsPeopleViral', 'totalTestsAntibody', 'positiveTestsAntibody', 'negativeTestsAntibody', 'totalTestsPeopleAntibody', 'positiveTestsPeopleAntibody', 'negativeTestsPeopleAntibody', 'totalTestsPeopleAntigen', 'positiveTestsPeopleAntigen', 'totalTestsAntigen', 'positiveTestsAntigen', 'fips', 'positiveIncrease', 'negativeIncrease', 'total', 'totalTestResultsIncrease', 'posNeg', 'dataQualityGrade', 'deathIncrease', 'hospitalizedIncrease', 'hash', 'commercialScore', 'negativeRegularScore', 'negativeScore', 'positiveScore', 'score', 'grade']

    # Crear una tabla para almacenar los datos de COVID-19 si previamente no existia una tabla en la base de datos que se llamara "datos_covid"
    create_table_query = f"CREATE TABLE IF NOT EXISTS datos_covid ({', '.join([f'{column} VARCHAR(255)' for column in columns])}, probableCases VARCHAR(255), probablecases VARCHAR(255));"


    cursor.execute(create_table_query)
    conn.commit()

    # Insertar los datos en la tabla
    for item in data:
        insert_query = f"INSERT INTO datos_covid ({', '.join(columns)}) VALUES ({', '.join(['%s']*len(columns))});"
        cursor.execute(insert_query, tuple(item[column] for column in columns))

    conn.commit()

    # Cerrar la conexión
    cursor.close()
    conn.close()
else:
    print("Error al obtener los datos de la API")