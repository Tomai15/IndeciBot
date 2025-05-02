import concurrent.futures

from indeciBot import descargarDecidir
from cdpBot import descargarCDP
from vtexBot import descargarVtex

fechaInicio = input("Ingrese la fecha de inicio: ")
fechaFin = input("Ingrese la fecha de fin: ")
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    futures = [
        executor.submit(descargarDecidir, fechaInicio, fechaFin),
        executor.submit(descargarCDP, fechaInicio, fechaFin),
        executor.submit(descargarVtex, fechaInicio, fechaFin)
    ]
    concurrent.futures.wait(futures)

print("¡Descargas completadas! Ahora podés cruzar los datos.")
