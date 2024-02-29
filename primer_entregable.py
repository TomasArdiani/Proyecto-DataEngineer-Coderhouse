import requests

url = "https://api.covidtracking.com/v1/states/current.json"

response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print("Error al hacer la solicitud:", response.status_code)