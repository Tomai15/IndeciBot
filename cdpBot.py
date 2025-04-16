import sys

from playwright.sync_api import sync_playwright
import os
import pandas as pd
from datetime import datetime, timedelta
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(),logging.FileHandler(f'log_ejecucion_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log', encoding="utf-8") ]
)
logging.getLogger("asyncio").setLevel(logging.WARNING)

with sync_playwright() as navegador:
    tiempo_de_espera = 900000
    fecha_inicio_usuario = input("Ingrese la fecha de inicio (dd/mm/aaaa): ")
    fecha_fin_usuario = input("Ingrese la fecha de fin (dd/mm/aaaa): ")

    navegador_web = navegador.chromium.launch(headless=False)
    pagina = navegador_web.new_page()

    credenciales = {}
    with open("credencialesCDP.txt", "r", encoding="utf-8") as archivo:
        for linea in archivo:
            clave, valor = linea.strip().split("=")  # Separa clave y valor
            credenciales[clave] = valor

    usuario = credenciales["usuario"]
    contrasena = credenciales["contrasena"]

    logging.info("Pasando a descargar en CDP")
    logging.info("Ingresando a:http://10.94.164.155:16000/ConcentradorDePedidos/puntoAdm ")
    pagina.goto("http://10.94.164.155:16000/ConcentradorDePedidos/puntoAdm")

    pagina.wait_for_load_state("load",timeout=tiempo_de_espera)
    pagina.fill("input[name='username']", usuario)
    pagina.fill("input[name='password']", contrasena)

    pagina.click("input[type='submit']")
    pagina.wait_for_load_state("load",timeout=tiempo_de_espera)
    pagina.wait_for_selector('select#mySelect')
    pagina.select_option('select#mySelect',value= "14")
    pagina.click("input[type='button']")
    pagina.wait_for_load_state("load",timeout=tiempo_de_espera)
    pagina.goto("http://10.94.164.155:16000/ConcentradorDePedidos/secciones/listadoVentas")
    pagina.wait_for_load_state("load",timeout=tiempo_de_espera)

    pagina.click("text=FILTRAR")
    pagina.fill("input[name='fechaMin']", fecha_inicio_usuario)
    pagina.fill("input[name='ctrl.fechaMax']", fecha_fin_usuario)
    pagina.click("text=BUSCAR")
    pagina.wait_for_selector("table tbody tr", timeout=tiempo_de_espera)
    pagina.click("text=EXPORTAR")

    with pagina.expect_download(timeout=tiempo_de_espera) as informacion_descarga:
        pagina.click("text=Sólo Cabecera", timeout=90000)

    ruta_carpeta = os.path.join(os.getcwd(), "descargas_CDP")
    os.makedirs(ruta_carpeta, exist_ok=True)

    archivo_descargado = informacion_descarga.value
    nombre_archivo = f"transacciones_CDP_{fecha_inicio_usuario}_{fecha_fin_usuario}.xlsx"
    ruta_csv = os.path.join(ruta_carpeta, nombre_archivo)
    archivo_descargado.save_as(ruta_csv)

    input("Presioná ENTER para continuar una vez que terminaste en el navegador...")

