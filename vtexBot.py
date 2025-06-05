import logging
import os
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



def formatear(fecha):
    return fecha.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def get_pedidos(ini, fin):
    """Hace una request y devuelve los pedidos + cantidad de páginas"""
    params = {
        "f_creationDate": f"creationDate:[{formatear(ini)} TO {formatear(fin)}]",
        "page": 1,
        "per_page": 100,
        "orderBy": "creationDate,asc"
    }
    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    return data.get("list", []), data.get("paging", {}).get("pages", 0)


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
    with ThreadPoolExecutor(max_workers=150) as executor:
        futures = [executor.submit(buscarSeller, pedido["orderId"]) for pedido in pedidos_lote]
        for future in as_completed(futures):
            resultados.append(future.result())
    return resultados

def descargarVtex(fecha_inicio_usuario,fecha_fin_usuario):
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
    fecha_desde = datetime.strptime(fecha_inicio_usuario, "%d/%m/%Y") + timedelta(hours=3)
    fecha_hasta = datetime.strptime(fecha_fin_usuario, "%d/%m/%Y") + timedelta(hours=23, minutes=59, seconds=59) + timedelta(hours=3)
    per_page = 100
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


    logging.info(f"Generando Excel con los pedidos sin seller")

    ruta_carpeta = os.path.join(os.getcwd(), "descargas_vtex")
    os.makedirs(ruta_carpeta, exist_ok=True)
    archivo = os.path.join(ruta_carpeta, f"pedidos_vtex_{fecha_desde.date()}_a_{fecha_hasta.date()}_SIN_SELLER.xlsx")
    pd.DataFrame(todos_los_pedidos).to_excel(archivo, index=False)
    logging.info(f"Exportado a: {archivo}")

    # Convertir de nuevo a lista
    todos_los_pedidos = pd.read_excel(archivo)
    todos_los_pedidos = todos_los_pedidos.to_dict(orient="records")

    logging.info(f"Eliminando repetidos")

    pedidos_unicos = {}
    for pedido in todos_los_pedidos:
        pedidos_unicos[pedido["orderId"]] = pedido

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
    archivo = os.path.join(ruta_carpeta, f"pedidos_vtex_{fecha_desde.date()}_a_{fecha_hasta.date()}.xlsx")
    pedidos_vtex = pd.DataFrame(todos_los_pedidos)
    pedidos_vtex = pedidos_vtex[["orderId","sequence","creationDate","paymentNames","seller","statusDescription"]]
    pedidos_vtex.to_excel(archivo, index=False)
    logging.info(f"Exportado a: {archivo}")

if __name__ == "__main__":
    descargarVtex("18/05/2025","21/05/2025")