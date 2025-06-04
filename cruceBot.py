import concurrent.futures
import os

import pandas as pd

from indeciBot import descargarDecidir
from cdpBot import descargarCDP
from vtexBot import descargarVtex
import logging
from datetime import datetime,timedelta

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(),logging.FileHandler(f'log_ejecucion_cdpBot_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log', encoding="utf-8") ]
)
logging.getLogger("asyncio").setLevel(logging.WARNING)

def ejecutarOpcionSeleccionada (opcionSeleccionada,fecha_inicio_usuario,fecha_fin_usuario):
    match opcionSeleccionada:
        case "A":
            descargarDecidir(fecha_inicio_usuario,fecha_fin_usuario)
        case "B":
            descargarVtex(fecha_inicio_usuario,fecha_fin_usuario)
        case "C":
            descargarCDP(fecha_inicio_usuario,fecha_fin_usuario)
        case "D":
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                futures = [
                            executor.submit(descargarDecidir, fechaInicio, fechaFin),
                            executor.submit(descargarCDP, fechaInicio, fechaFin),
                            executor.submit(descargarVtex, fechaInicio, fechaFin)]
                concurrent.futures.wait(futures)
        case "E":
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                futures = [
                            executor.submit(descargarDecidir, fechaInicio, fechaFin),
                            executor.submit(descargarCDP, fechaInicio, fechaFin),
                            executor.submit(descargarVtex, fechaInicio, fechaFin)]
            concurrent.futures.wait(futures)

            fecha_inicio = datetime.strptime(fecha_inicio_usuario, "%d/%m/%Y")
            fecha_fin = datetime.strptime(fecha_fin_usuario, "%d/%m/%Y")

            nombreCarpetaDecidir = f"Transacciones del intervalo {fecha_inicio.date()} a {fecha_fin.date()}"
            rutaCarpetaDecidir = os.path.join(os.getcwd(), nombreCarpetaDecidir)
            rutaExcelDecidir = os.path.join(rutaCarpetaDecidir,"transacciones_completas.xlsx")
            pedidosDecidir = pd.read_excel(rutaExcelDecidir)

            fecha_desde = datetime.strptime(fecha_inicio_usuario, "%d/%m/%Y") + timedelta(hours=3)
            fecha_hasta = datetime.strptime(fecha_fin_usuario, "%d/%m/%Y") + timedelta(hours=23, minutes=59, seconds=59) + timedelta(hours=3)
            nombreCarpetaVtex = "descargas_vtex"
            rutaCarpetaVtex = os.path.join(os.getcwd(), nombreCarpetaVtex)
            rutaExcelVtex = os.path.join(rutaCarpetaVtex,f"pedidos_vtex_{fecha_desde.date()}_a_{fecha_hasta.date()}.xlsx")
            pedidosVtex = pd.read_excel(rutaExcelVtex)

            rutaCarpetaCDP = os.path.join(os.getcwd(), "descargas_CDP")
            nombreArchicoCDP = f"transacciones_CDP_{fecha_inicio.strftime("%Y-%m-%d")}_{fecha_fin.strftime("%Y-%m-%d")}.xlsx"

            rutaPedidosCDP = os.path.join(rutaCarpetaCDP, nombreArchicoCDP)
            pedidosCDP = pd.read_excel(rutaPedidosCDP)

            ruta_final = os.path.join(os.getcwd(),
                                      f"pedidos_unificados_{fecha_inicio.date()}_a_{fecha_fin.date()}.xlsx")

            with pd.ExcelWriter(ruta_final, engine="openpyxl") as writer:
                pedidosDecidir.to_excel(writer, sheet_name="Decidir", index=False)
                pedidosVtex.to_excel(writer, sheet_name="Vtex", index=False)
                pedidosCDP.to_excel(writer, sheet_name="CDP", index=False)

            logging.info(f"Archivo unificado exportado en: {ruta_final}")

        case "F":
            print("Esta funcion aun no esta disponible. Por favor ejecute de nuevo"
                  " seleccionando otra ")

        case _:
            print("Opcion no valida")





print("Por favor seleccione la operacion que necesita")
print("A- Descargar pedidos de Decidir")
print("B- Descargar pedidos de Vtex")
print("C- Descargar pedidos de CDP")
print("D- Descargar pedidos de las 3 fuentes")
print("E- Descargar pedidos de las 3 fuentes y juntarlos en un archivo")
print("F- Descarpedidos de las 3 fuentes y cruzarlos en un archivo")

opcionSeleccionada = input("Escriba la letra de la opcion: ")

fechaInicio = input("Ingrese la fecha de inicio: ")
fechaFin = input("Ingrese la fecha de fin: ")


ejecutarOpcionSeleccionada(opcionSeleccionada,fechaInicio,fechaFin)





