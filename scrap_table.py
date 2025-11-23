import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):

    # URL donde está la tabla de los sismos
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    # Obtener la página HTML
    response = requests.get(url)
    if response.status_code != 200:
        return {
            "statusCode": response.status_code,
            "body": "Error al acceder al sitio del IGP"
        }

    soup = BeautifulSoup(response.text, "html.parser")

    tabla = soup.find("table")
    if not tabla:
        return {
            "statusCode": 404,
            "body": "No se encontró la tabla de sismos"
        }

    # Procesar encabezados
    headers = [h.text.strip() for h in tabla.find_all("th")]

    # Procesar filas
    filas_sismos = []
    for fila in tabla.find_all("tr")[1:11]:  # SOLO 10 sismos
        columnas = fila.find_all("td")

        datos = {headers[i]: columnas[i].text.strip() for i in range(len(columnas))}
        filas_sismos.append(datos)

    # DynamoDB
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("TablaSismosIGP")

    # Guardar en DynamoDB
    for sismo in filas_sismos:
        sismo["id"] = str(uuid.uuid4())
        table.put_item(Item=sismo)

    return {
        "statusCode": 200,
        "body": filas_sismos
    }
