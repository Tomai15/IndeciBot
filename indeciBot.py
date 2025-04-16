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

credenciales = {}
with open("credencialesPayway.txt", "r", encoding="utf-8") as archivo:
    for linea in archivo:
        clave, valor = linea.strip().split("=")  # Separa clave y valor
        credenciales[clave] = valor

usuario = credenciales["usuario"]
contrasena = credenciales["contrasena"]

tiempo_de_espera = 900000  # Ajustado a un tiempo razonable

def entrarPagina(pagina):
    # Navegar a la página de login
    pagina.goto("https://ventasonline.payway.com.ar/sac/SAC")
    pagina.wait_for_load_state("load")

    # Completar el formulario de login
    pagina.fill("input[name='usuariosps']", usuario)
    pagina.fill("input[name='passwordsps']", contrasena)

    # Hacer clic en el botón de login
    pagina.click("input[id='image1']")
    pagina.wait_for_load_state("networkidle", timeout=tiempo_de_espera)


def descargar_y_convertir(pagina, fecha_formato_guardado, hora_inicio, minuto_inicio, hora_fin, minuto_fin, etiqueta):
    """Descarga el CSV, lo convierte a Excel y lo almacena en la lista de archivos."""

    # Esperar a que los campos de hora estén disponibles
    pagina.wait_for_selector("input[name='sacparam_horaini']", timeout=tiempo_de_espera)

    # Verificar si existen antes de llenarlos
    if pagina.locator("input[name='sacparam_horaini']").count() > 0:
        pagina.fill("input[name='sacparam_horaini']", hora_inicio)
        pagina.fill("input[name='sacparam_minutoini']", minuto_inicio)
        pagina.fill("input[name='sacparam_horafin']", hora_fin)
        pagina.fill("input[name='sacparam_minutofin']", minuto_fin)
    else:
        logging.error("No se encontraron los campos de hora en la página.")
        return None  # Evita continuar si los campos no están

    with pagina.expect_download() as informacion_descarga:
        pagina.click("input[name='b_downloadform']", timeout=tiempo_de_espera)

    archivo_descargado = informacion_descarga.value
    nombre_csv = f"transacciones_{fecha_formato_guardado}_{etiqueta}.csv"
    ruta_csv = os.path.join(ruta_carpeta, nombre_csv)
    archivo_descargado.save_as(ruta_csv)

    # Convertir CSV a Excel
    datos = pd.read_csv(ruta_csv, delimiter="\t", encoding="ISO-8859-1",dtype=str, index_col=False)
    datos.columns = datos.columns.str.strip()
    nombre_excel = nombre_csv.replace(".csv", ".xlsx")
    ruta_excel = os.path.join(ruta_carpeta, nombre_excel)
    datos.to_excel(ruta_excel, index=False)
    os.remove(ruta_csv)
    return ruta_excel

def buscarDia(fecha,pagina):
    global fecha_formato_mostrar
    global fecha_formato_guardado
    fecha_formato_mostrar = fecha.strftime("%d/%m/%Y")
    fecha_formato_guardado = fecha.strftime("%Y-%m-%d")
    # Establecer la búsqueda para todo el día (00:00 - 23:59)
    pagina.fill("input[name='sacparam_fechaini']", fecha_formato_mostrar)
    pagina.fill("input[name='sacparam_fechafin']", fecha_formato_mostrar)
    pagina.fill("input[name='sacparam_horaini']", "00")
    pagina.fill("input[name='sacparam_minutoini']", "00")
    pagina.fill("input[name='sacparam_horafin']", "23")
    pagina.fill("input[name='sacparam_minutofin']", "59")
    pagina.click("input[name='b_consultaform']", timeout=tiempo_de_espera)
    pagina.wait_for_load_state("networkidle", timeout=tiempo_de_espera)

def errorAlBuscarDia(pagina):
    mesaje_errorExtraño = pagina.locator("p:has-text('Ha ocurrido un error')")
    return mesaje_errorExtraño.count() > 0
def transaccionesSuperadas(pagina):
    mensaje_error = pagina.locator("td.textonaranja")
    return mensaje_error.count() > 0 and "5000 transacciones" in mensaje_error.text_content()
def procesarDia(pagina,fecha_actual):
    global fecha_formato_mostrar
    global fecha_formato_guardado
    global fecha_inicio_usuario
    global fecha_fin_usuario
    global fecha_inicio
    global fecha_fin
    global reintentos_actuales




    if errorAlBuscarDia(pagina):
        logging.warning(f"El dia{fecha_actual} arrojo un error.Se reintentara mas tarde.")
        dias_con_error.append(fecha_actual)
        entrarPagina(pagina)
    else:
        if transaccionesSuperadas(pagina):
            logging.info(f" Más de 5000 transacciones el {fecha_formato_mostrar}, dividiendo en mañana y tarde")

            for parte_del_dia, hora_inicio, minuto_inicio, hora_fin, minuto_fin in [
                ("mañana", "00", "00", "11", "59"),
                ("tarde", "12", "00", "23", "59")
            ]:
                archivo = descargar_y_convertir(pagina, fecha_formato_guardado, hora_inicio, minuto_inicio, hora_fin,
                                                minuto_fin, parte_del_dia)
                lista_archivos_excel.append(archivo)

                # Rehacer la consulta para ver si hay más de 5000 registros en la mañana o tarde
                pagina.fill("input[name='sacparam_horaini']", hora_inicio)
                pagina.fill("input[name='sacparam_minutoini']", minuto_inicio)
                pagina.fill("input[name='sacparam_horafin']", hora_fin)
                pagina.fill("input[name='sacparam_minutofin']", minuto_fin)
                pagina.click("input[name='b_consultaform']", timeout=tiempo_de_espera)
                pagina.wait_for_load_state("networkidle", timeout=tiempo_de_espera)

                mensaje_error = pagina.locator("td.textonaranja")

                if transaccionesSuperadas(pagina):
                    logging.info(
                        f" Más de 5000 transacciones en {parte_del_dia} del {fecha_formato_mostrar}, dividiendo en 4 intervalos")

                    for sub_parte, h_inicio, m_inicio, h_fin, m_fin in [
                        ("madrugada", "00", "00", "05", "59"),
                        ("mañana", "06", "00", "11", "59"),
                        ("tarde", "12", "00", "17", "59"),
                        ("noche", "18", "00", "23", "59")
                    ]:
                        archivo = descargar_y_convertir(pagina, fecha_formato_guardado, h_inicio, m_inicio, h_fin,
                                                        m_fin, sub_parte)
                        lista_archivos_excel.append(archivo)

        else:
            archivo = descargar_y_convertir(pagina, fecha_formato_guardado, "00", "00", "23", "59", "completo")
            lista_archivos_excel.append(archivo)

        logging.info(f"Finalizado el dia {fecha_actual}")
    return fecha_actual
lista_archivos_excel = []  # Lista de archivos Excel descargados y convertidos
reintentos_actuales = 0
dias_con_error = []
fecha_formato_mostrar = 0
fecha_formato_guardado = 0
ejecucion_inicial_terminada = False

with sync_playwright() as navegador:
    fecha_inicio_usuario = input("Ingrese la fecha de inicio (dd/mm/aaaa): ")
    fecha_fin_usuario = input("Ingrese la fecha de fin (dd/mm/aaaa): ")

    fecha_inicio = datetime.strptime(fecha_inicio_usuario, "%d/%m/%Y")
    fecha_fin = datetime.strptime(fecha_fin_usuario, "%d/%m/%Y")

    nombre_carpeta = f"Transacciones del intervalo {fecha_inicio.date()} a {fecha_fin.date()}"
    ruta_carpeta = os.path.join(os.getcwd(), nombre_carpeta)

    # Crear la carpeta si no existe
    if not os.path.exists(ruta_carpeta):
        os.makedirs(ruta_carpeta)

    logging.info(f"Carpeta creada en {ruta_carpeta}")
    # Lanzar el navegador
    navegador_web = navegador.chromium.launch(headless=False)
    pagina = navegador_web.new_page()

    entrarPagina(pagina)

    fecha_actual = fecha_inicio
    while fecha_actual <= fecha_fin:
       buscarDia(fecha_actual, pagina)
       procesarDia(pagina, fecha_actual)
       fecha_actual += timedelta(days=1)
    logging.info("Descarga de dias terminados. Se procede a reintar los dias con error")
    for dia_con_error in dias_con_error:
        logging.info("Se reintenta el dia")
        buscarDia(dia_con_error, pagina)
        procesarDia(pagina,dia_con_error)

    logging.info("Descargas y conversiones a Excel completadas. Uniendo archivos...")

    # Unir todos los archivos Excel en uno solo
    lista_datos = [pd.read_excel(archivo) for archivo in lista_archivos_excel]
    datos_finales = pd.concat(lista_datos, ignore_index=True)
    datos_finales.to_excel(os.path.join(ruta_carpeta,"transacciones_completas.xlsx") , index=False)

    logging.info("Archivos combinados en transacciones_completas.xlsx")

    navegador_web.close()
