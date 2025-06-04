import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta
from ratelimit import limits, sleep_and_retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd
import time

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

def buscarSeller(order_id, reintentos=3):
    url_detalle = f"{url}/{order_id}"
    for intento in range(reintentos):
        try:
            response = requests.get(url_detalle, headers=headers, timeout=10)
            data = response.json()
            return order_id, data.get("sellers", [{}])[0].get("name", "No encontrado")
        except requests.exceptions.RequestException as e:
            logging.warning(f"Error al buscar seller para {order_id}: {e}")
            time.sleep(2 ** intento)  # Backoff exponencial
    return order_id, "Error al obtener seller"


def procesar_lote(pedidos_lote):
    resultados = []
    with ThreadPoolExecutor(max_workers=300) as executor:
        futures = [executor.submit(buscarSeller, pedido["orderId"]) for pedido in pedidos_lote]
        for future in as_completed(futures):
            resultados.append(future.result())
    return resultados



todos_los_pedidos = pd.read_excel("descargas_vtex/pedidos_vtex_2025-01-01_a_2025-03-01_SIN_SELLER.xlsx")
todos_los_pedidos = todos_los_pedidos.to_dict(orient="records")


logging.info(f"Eliminando repetidos")
pedidos_unicos = {}
for pedido in todos_los_pedidos:
    print(f"Tipo: {type(pedido)}, Contenido: {pedido}")
    pedidos_unicos[pedido["orderId"]] = pedido

# Convertir de nuevo a lista
todos_los_pedidos = list(pedidos_unicos.values())

logging.info(f"Eliminando repetidos")

logging.info(f"Buscando el seller de cada pedido")

for i in range(0, len(todos_los_pedidos), 6000):
    lote = todos_los_pedidos[i:i + 6000]
    inicio = time.time()
    resultados = procesar_lote(lote)

    # Asignar seller al pedido correspondiente
    for order_id, seller in resultados:
        for pedido in todos_los_pedidos:
            if pedido["orderId"] == order_id:
                pedido["seller"] = seller
                break

    # Esperar si el lote fue muy rápido (respetar 60 segundos por 6000 requests)
    duracion = time.time() - inicio
    if duracion < 60:
        logging.info(f"Se alcanzaron las transacciones maximas, durmiendo {60 - duracion} segundos")
        time.sleep(60 - duracion)
    logging.info(f"Se descargaron {i*6000}")

logging.info(f"Descargas finalizadas. Convirtiendo a Excel")
# Exportar a Excel
ruta_carpeta = os.path.join(os.getcwd(), "descargas_vtex")
os.makedirs(ruta_carpeta, exist_ok=True)
archivo = os.path.join(ruta_carpeta, f"pedidos_vtex_inical_a_final.xlsx")
pd.DataFrame(todos_los_pedidos).to_excel(archivo, index=False)
logging.info(f"Exportado a: {archivo}")

