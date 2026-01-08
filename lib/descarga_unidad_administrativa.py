# -*- coding: utf-8 -*-
import io
import time
import zipfile
import logging
import json
import os
import sys
from logging.handlers import RotatingFileHandler
import requests

# =========================
# Configuración de logging
# =========================
logging.basicConfig(
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename="descarga_unidades_administrativas.log",
                    filemode='w',
                    encoding="utf-8"
)
logger = logging.getLogger(__name__)


# =========================
# Lógica principal
# =========================

"""
https://api-features.ign.es/collections/administrativeunit/items?f=json&limit=1
https://api-features.ign.es/collections/administrativeunit/items?f=json&limit=1&nationallevelname=Municipio
https://api-features.ign.es/collections/administrativeunit/items?f=json&limit=1&nationallevelname=Pa%C3%ADs
https://api-features.ign.es/collections/administrativeunit/items?f=json&limit=1&nationallevelname=Comunidad%20aut%C3%B3noma
https://api-features.ign.es/collections/administrativeunit/items?f=json&limit=1&nationallevelname=Provincia
"""

def pais(path=None, pag = 50):
    logger.info("Descargando: ------ PAÍS - IGN ------")

    session = requests.Session()

    url_ori = "https://api-features.ign.es/collections/administrativeunit/items?f=json&nationallevelname=Pa%C3%ADs"

    url_limit1 = url_ori + "&limit=1"

    try:
        response = session.get(url_limit1, timeout=20)
        status = response.status_code

        if status == 200:
            data = response.json()

            num_obj_geo = data["numberMatched"]

            logger.info(
                    "Número de objetos grográficos = %s",
                    num_obj_geo
            )

        else:
            logger.warning(
                "HTTP %d | URL: %s : %s",
                status, url_limit1, response.text[:300]
            )

    except requests.exceptions.Timeout:
        logger.warning(" timeout (20s)",)
    except requests.exceptions.RequestException as e:
        logger.error("Error de petición: %s | URL: %s", e, url_limit1)
    except Exception as e:
        logger.exception("Error procesando respuesta  %s | URL: %s", e, url_limit1)

    num_obj_geo = None
    if not num_obj_geo:
        logger.error("No se pudo continuar con la descarga")
        raise ValueError("No se pudo continuar con la descarga")
    
    if num_obj_geo > pag:
        pass
    else:
        
        contador = 0
        while True:
            contador += 1
            print(f"Iteración {contador}")
            if contador == 5:
                break




def codigos_postales(path=None, descarcaID=True):
    """
    Descarga un ZIP con tablas de códigos postales, extrae pares {cod, name},
    y consulta la API de CartoCiudad por cada código (en GEOJSON).
    """
    logger.info("Descargando: ------ CODIGOS POSTALES - Geocoder ------")

    if descarcaID:

        URLzipCod = "https://www.codigospostales.com/codigos1220n.zip"

        logger.info("Descargando ZIP de códigos postales: %s", URLzipCod)

        try:
            resp = requests.get(URLzipCod, timeout=30)
            resp.raise_for_status()  # lanza HTTPError si status >= 400
        except requests.exceptions.Timeout:
            logger.error("Timeout al descargar el ZIP (30s).")
            return []
        except requests.exceptions.RequestException as e:
            logger.error("Error al descargar el ZIP: %s", e)
            return []

        zip_bytes = io.BytesIO(resp.content)
        codPostArray = []

        try:
            with zipfile.ZipFile(zip_bytes) as zf:
                names = zf.namelist()
                logger.debug("Contenido del ZIP: %s", names)

                for name in names:
                    if not name.lower().endswith(".txt"):
                        logger.debug("Saltando (no .txt): %s", name)
                        continue

                    # Excluir ficheros específicos
                    if name in ("codciu.txt", "ADxcodpos.txt"):
                        logger.debug("Excluyendo %s", name)
                        continue

                    logger.info("Procesando archivo: %s", name)
                    sys.stdout.flush()
                    with zf.open(name, "r") as f:
                        # Ajusta encoding si no es UTF-8
                        contenido = f.read().decode("utf-8", errors="replace")
                        for lineno, linea in enumerate(contenido.splitlines(), start=1):
                            if not linea:
                                continue
                            if ";" in linea:
                                linea = linea.replace(";", ":")
                            lineaArray = linea.split(":")
                            if len(lineaArray) < 2:
                                logger.debug("Línea inválida %s:%d → %r", name, lineno, linea)
                                continue
                            cod = lineaArray[0].strip()
                            nombre = lineaArray[1].strip()
                            if not cod or not nombre:
                                logger.debug("Campos vacíos %s:%d → %r", name, lineno, linea)
                                continue
                            codPostArray.append({"cod": cod, "name": nombre})

        except zipfile.BadZipFile:
            logger.error("El archivo descargado no es un ZIP válido.")
            return []
        except Exception as e:
            logger.exception("Error procesando el ZIP: %s", e)
            return []

        logger.info("Total de registros extraídos: %d", len(codPostArray))

        # Si quieres guardar el array intermedio en disco (opcional)
        try:
            with open("codigos_postales.json", "w", encoding="utf-8") as fh:
                json.dump(codPostArray, fh, ensure_ascii=False, indent=2)
            logger.info("Guardado JSON intermedio en: %s", path)
        except Exception:
            logger.exception("No se pudo guardar %s", path)

    else:
        try:
            with open("codigos_postales.json", "r", encoding="utf-8") as fh:
                print(fh)
                codPostArray = json.load(fh)
            logger.info(f"Cargado JSON desde: {path}")
        except Exception:
            logger.exception(f"No se pudo leer {path}")


    # Consulta a Geocoder por cada código
    session = requests.Session()
    headers = {
        "Origin": "https://www.ign.es",
        "Referer": "https://www.ign.es",
        "User-Agent": "Mozilla/5.0"
    }

    geojson_cod_postales = {'type': 'FeatureCollection', 'features': []}

    for i, codi in enumerate(codPostArray, start=1):
        codigo = codi["cod"].lstrip("0")  # quitar ceros a la izquierda
        # OJO: usa '&' en la URL real, no '&amp;'
        url = (
            "https://www.cartociudad.es/geocoder/api/geocoder/find"
            f"?q={codigo}&type=Codpost&id={codigo}&outputformat=geojson"
        )

        try:
            response = session.get(url, headers=headers, timeout=20)
            status = response.status_code

            if status == 200:
                data = response.json()
                # Validaciones defensivas
                features = data.get("features", [])
                geometry_type = (
                    features[0]["geometry"]["type"] if features and "geometry" in features[0] else None
                )
                geojson_cod_postales["features"].append(features[0])
                logger.info(
                    "[%d/%d] Código %s -> geometry_type=%s",
                    i, len(codPostArray), codigo, geometry_type
                )
            else:
                logger.warning(
                    "[%d/%d] Código %s -> HTTP %d | URL: %s : %s",
                    i, len(codPostArray), codigo, status, url, response.text[:300]
                )

        except requests.exceptions.Timeout:
            logger.warning("[%d/%d] Código %s -> timeout (20s)", i, len(codPostArray), codigo)
        except requests.exceptions.RequestException as e:
            logger.error("[%d/%d] Código %s -> error de petición: %s | URL: %s", i, len(codPostArray), codigo, e, url)
        except Exception:
            logger.exception("[%d/%d] Código %s ->  error procesando respuesta | URL: %s", i, len(codPostArray), codigo, url)

        # Respeto de rate-limit (ajusta si hace falta)
        time.sleep(1)

    try:
        with open(path+"codigos_postales.GeoJSON", "w", encoding="utf-8") as fh:
            json.dump(geojson_cod_postales, fh, ensure_ascii=False, indent=2)
        logger.info("Guardado GeoJSON: %s", path)
    except Exception:
        logger.exception("No se pudo guardar %s", path)
    logger.info("Proceso completado.")
    return





if __name__ == "__main__":
    logger.setLevel(logging.DEBUG) # "DEBUG" "INFO", "WARNING", "ERROR"
    # codigos_postales(path="./codigos_postales/", descarcaID=False)
    pais(path="./pais/")
