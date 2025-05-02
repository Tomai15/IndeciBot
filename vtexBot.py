import logging
import os
from datetime import datetime, timedelta
import requests
import pandas as pd

# Configuración de logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'log_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log', encoding="utf-8")
    ]
)

# Cargar credenciales
credenciales = {}
with open("credencialesVTEX.txt", "r", encoding="utf-8") as archivo:
    for linea in archivo:
        clave, valor = linea.strip().split("=")
        credenciales[clave] = valor

# Configuración API
url = "https://carrefourar.vtexcommercestable.com.br/api/oms/pvt/orders"
headers = {
    'Accept': "application/json",
    'Content-Type': "application/json",
    'X-VTEX-API-AppKey': credenciales["X-VTEX-API-AppKey"],
    'X-VTEX-API-AppToken': credenciales["X-VTEX-API-AppToken"]
}

# Fechas de entrada
fecha_desde = datetime.strptime("2025-04-14", "%Y-%m-%d") + timedelta(hours=3)
fecha_hasta = datetime.strptime("2025-04-28", "%Y-%m-%d") + timedelta(hours=23, minutes=59, seconds=59) + timedelta(hours=3)
per_page = 100

def formatear(fecha):
    return fecha.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def get_pedidos(ini, fin):
    """Hace una request y devuelve los pedidos + cantidad de páginas"""
    params = {
        "f_creationDate": f"creationDate:[{formatear(ini)} TO {formatear(fin)}]",
        "page": 1,
        "per_page": per_page,
        "orderBy": "creationDate,asc"
    }
    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    return data.get("list", []), data.get("paging", {}).get("pages", 0)

todos_los_pedidos = []
fecha_actual = fecha_desde
delta = timedelta(days=1)

while fecha_actual < fecha_hasta:
    fecha_siguiente = fecha_actual + delta
    if fecha_siguiente > fecha_hasta:
        fecha_siguiente = fecha_hasta

    pedidos, paginas = get_pedidos(fecha_actual, fecha_siguiente)
    logging.info(f"Proband con {fecha_actual} a {fecha_siguiente} - {paginas} páginas")

    if paginas > 30:
        delta = delta / 2
        logging.info("Demasiadas páginas, achicando intervalo")
        continue

    # Si está bien, descargamos todas las páginas del subintervalo
    for page in range(1, paginas + 1):
        params = {
            "f_creationDate": f"creationDate:[{formatear(fecha_actual)} TO {formatear(fecha_siguiente)}]",
            "page": page,
            "per_page": per_page,
            "orderBy": "creationDate,asc"
        }
        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        pedidos = data.get("list", [])
        todos_los_pedidos.extend(pedidos)
        logging.info(f"Página {page}/{paginas} del intervalo - {len(pedidos)} pedidos")

    fecha_actual = fecha_siguiente
    delta = timedelta(days=1)  # restauramos el paso si venía de achicarlo

# Exportar a Excel
ruta_carpeta = os.path.join(os.getcwd(), "descargas_vtex")
os.makedirs(ruta_carpeta, exist_ok=True)
archivo = os.path.join(ruta_carpeta, f"pedidos_vtex_{fecha_desde.date()}_a_{fecha_hasta.date()}.xlsx")
pd.DataFrame(todos_los_pedidos).to_excel(archivo, index=False)
logging.info(f"Exportado a: {archivo}")
