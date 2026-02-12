# -*- coding: utf-8 -*-
import io
import os
import sys
import time
import json
import random
import zipfile
import logging
import requests
import shapefile
from pyproj import Geod
import geopandas as gpd
from pyproj import CRS, Transformer
from xml.etree import ElementTree as ET
from typing import Optional, Tuple, Dict, Any
from shapely.geometry.base import BaseGeometry
from shapely.geometry import shape, mapping, Polygon, MultiPolygon


# =========================
# Configuración de logging
# =========================
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="descarga_unidades_administrativas.log",
    filemode="w",
    encoding="utf-8",
)
logger = logging.getLogger(__name__)


# =========================
# Constantes
# =========================
BASE_URL_API_IGN = "https://api-features.ign.es/collections/administrativeunit/items"
TIMEOUT = 60
MAX_RETRIES =  5
MIN_VERTICES = 4
DEFAULT_PAGE_SIZE = 20
SLEEP_BETWEEN_REQUESTS = 2 + random.uniform(-1, 1)
USER_AGENTS = [
    # Chrome (el rey indiscutible ~65-70% del mercado)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",

    # Firefox (muy usado por privacidad)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:134.0) Gecko/20100101 Firefox/134.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:134.0) Gecko/20100101 Firefox/134.0",

    # Edge (cada vez más común en entornos Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0",

    # Variaciones "congeladas" muy populares (muchos siguen reportando Win10)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0",
]
SLEEP_BETWEEN_REQUESTS = 3.0
VALID_SCALES = ["60M", "20M", "10M", "03M", "01M"]
NUTS_LEVELS=[None,0,1,2,3]

VALID_BARRIOS_DISTRITOS_HISTORICOS_ANNO = [None, 1612, 1768, 1802, 1835, 1840, 1845, 1863, 1898, 1955, 1970, 1987]
VALID_BARRIOS_DISTRITOS_HISTORICOS_CAPAS  = [None, "cuarteles", "parroquias", "termino", "barrios", "comisarias", "juzgados", "distritos"]


# =========================
# Lógica principal
# =========================

def IGN_pais(path: Optional[str] = None, pag: int = DEFAULT_PAGE_SIZE):
    gjson_name = "IGN_pais"
    gjson=descarga_IGN("País", path, pag, gjson_name)
    logger.info("Proceso completado.")
    return gjson, gjson_name

def IGN_comunidades_autonomas(path: Optional[str] = None, pag: int = DEFAULT_PAGE_SIZE):
    gjson_name = "IGN_comunidades_autonomas"
    gjson = descarga_IGN("Comunidad autónoma", path, pag, gjson_name)
    logger.info("Proceso completado.")
    return gjson, gjson_name

def IGN_provincias(path: Optional[str] = None, pag: int = DEFAULT_PAGE_SIZE):
    gjson_name = "IGN_provincias"
    gjson = descarga_IGN("Provincia", path, pag, gjson_name)
    logger.info("Proceso completado.")
    return gjson, gjson_name

def IGN_municipios(path: Optional[str] = None, pag: int = DEFAULT_PAGE_SIZE):
    gjson_name = "IGN_municipios"
    gjson = descarga_IGN("Municipio", path, pag, gjson_name)
    logger.info("Proceso completado.")
    return gjson, gjson_name

def IGN_codigos_postales(path: Optional[str], descarga_ID_json: bool=True):
    """
    Descarga un ZIP con tablas de códigos postales, extrae pares {cod, name},
    y consulta la API de CartoCiudad por cada código (en gjson).
    """
    logger.info("Descargando: ------ CODIGOS POSTALES - Geocoder ------")

    gjson_name = "IGN_codigos_postales"

    if descarga_ID_json:
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
        codPostArray = {}

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
                                logger.debug(
                                    "Línea inválida %s:%d → %r", name, lineno, linea
                                )
                                continue
                            cod = lineaArray[0].strip()
                            nombre = lineaArray[1].strip()
                            if not cod or not nombre:
                                logger.debug(
                                    "Campos vacíos %s:%d → %r", name, lineno, linea
                                )
                                continue
                            if cod in codPostArray:
                                codPostArray[cod]["names"] +=  f" | {nombre}"
                            else:
                                codPostArray[cod] = {"names": nombre}

        except zipfile.BadZipFile:
            logger.error("El archivo descargado no es un ZIP válido.")
            return []
        except Exception as e:
            logger.exception("Error procesando el ZIP: %s", e)
            return

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
                codPostArray = json.load(fh)
            logger.info(f"Cargado JSON desde: {path}")
        except Exception:
            logger.exception(f"No se pudo leer {path}")

    # Consulta a Geocoder por cada código
    session = crear_session_robusta()
    headers = {
        "Origin": "https://www.ign.es",
        "Referer": "https://www.ign.es",
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "es-ES,es;q=0.9",
        "Accept": "*/*",
        "Host": "https://www.ign.es"
    }

    gjson= {"type": "FeatureCollection", "features": []}

    for i, (clave, valor) in enumerate(codPostArray.items(), start=1):
        codigo = clave.lstrip("0")  # quitar ceros a la izquierda
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
                    features[0]["geometry"]["type"]
                    if features and "geometry" in features[0]
                    else None
                )
                features[0]["properties"]["names"] = valor["names"]
                gjson["features"].append(features[0])
                logger.info(
                    "[%d/%d] Código %s -> geometry_type=%s",
                    i,
                    len(codPostArray),
                    codigo,
                    geometry_type,
                )
            else:
                logger.warning(
                    "[%d/%d] Código %s -> HTTP %d | URL: %s : %s",
                    i,
                    len(codPostArray),
                    codigo,
                    status,
                    url,
                    response.text[:300],
                )

        except requests.exceptions.Timeout:
            logger.warning(
                "[%d/%d] Código %s -> timeout (20s)", i, len(codPostArray), codigo
            )
        except requests.exceptions.RequestException as e:
            logger.error(
                "[%d/%d] Código %s -> error de petición: %s | URL: %s",
                i,
                len(codPostArray),
                codigo,
                e,
                url,
            )
        except Exception:
            logger.exception(
                "[%d/%d] Código %s ->  error procesando respuesta | URL: %s",
                i,
                len(codPostArray),
                codigo,
                url,
            )

        # Respeto de rate-limit (ajusta si hace falta)
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    save_geojson(gjson, path, gjson_name)
    logger.info("Proceso completado.")
    return gjson, gjson_name

def correos_codigos_postales(
    path: Optional[str] = None,
    descarga_ID_json: bool = True
) -> Tuple[Dict[str, Any], str]:
    """
    Descarga ZIP códigos postales → extrae {código: nombres},
    y consulta la API de CORREOS por cada código para obtener polígono.
    Devuelve FeatureCollection GeoJSON estándar.
    """
    logger.info("------ CODIGOS POSTALES - API Correos Polygon ------")

    gjson_name = "correos_codigos_postales"

    # ── 1. Obtener/obtener códigos postales ───────────────────────────────
    if descarga_ID_json:
        URLzipCod = "https://www.codigospostales.com/codigos1220n.zip"
        logger.info("Descargando ZIP códigos postales: %s", URLzipCod)

        try:
            resp = requests.get(URLzipCod, timeout=TIMEOUT)
            resp.raise_for_status()
        except requests.exceptions.Timeout:
            logger.error("Timeout al descargar ZIP (30s)")
            return {"type": "FeatureCollection", "features": []}, gjson_name
        except requests.exceptions.RequestException as e:
            logger.error("Error descargando ZIP: %s", e)
            return {"type": "FeatureCollection", "features": []}, gjson_name

        zip_bytes = io.BytesIO(resp.content)
        codPostArray: Dict[str, Dict[str, str]] = {}

        try:
            with zipfile.ZipFile(zip_bytes) as zf:
                for name in zf.namelist():
                    if not name.lower().endswith(".txt"):
                        continue
                    if name in ("codciu.txt", "ADxcodpos.txt"):
                        logger.debug("Excluyendo %s", name)
                        continue

                    logger.info("Procesando: %s", name)

                    with zf.open(name, "r") as f:
                        contenido = f.read().decode("utf-8", errors="replace")
                        for lineno, linea in enumerate(contenido.splitlines(), 1):
                            if not linea.strip():
                                continue
                            linea = linea.replace(";", ":")
                            parts = linea.split(":")
                            if len(parts) < 2:
                                continue

                            cod = parts[0].strip()
                            nombre = parts[1].strip()

                            if not cod or not nombre:
                                continue

                            if cod in codPostArray:
                                codPostArray[cod]["names"] += f" | {nombre}"
                            else:
                                codPostArray[cod] = {"names": nombre}

        except zipfile.BadZipFile:
            logger.error("ZIP inválido")
            return {"type": "FeatureCollection", "features": []}, gjson_name
        except Exception as e:
            logger.exception("Error procesando ZIP: %s", e)
            return {"type": "FeatureCollection", "features": []}, gjson_name

        logger.info("Registros extraídos: %d", len(codPostArray))

        # Guardado opcional del diccionario intermedio
        try:
            with open("codigos_postales.json", "w", encoding="utf-8") as fh:
                json.dump(codPostArray, fh, ensure_ascii=False, indent=2)
            logger.info("Guardado JSON intermedio en: %s", path)
        except Exception:
            logger.exception("No se pudo guardar %s", path)

    else:
        # Cargar desde disco
        try:
            with open("codigos_postales.json", "r", encoding="utf-8") as fh:
                codPostArray = json.load(fh)
            logger.info(f"Cargado JSON desde: {path}")
        except Exception:
            logger.exception(f"No se pudo leer {path}")

    # ── 2. Consulta API Correos por cada código ─────────────────────────────
    session = crear_session_robusta()

    
    gjson = {
        "type": "FeatureCollection",
        "features": []
    }

    codPostArrayAleatorio = reordenar_array_aleatoriamente(codPostArray)

    total = len(codPostArrayAleatorio)
    inicio = time.time()

    for i, (codigo_original, data) in enumerate(codPostArrayAleatorio.items(), 1):

        porcentaje = i / total
        bar_length = 30
        filled = int(bar_length * porcentaje)
        bar = "█" * filled + "░" * (bar_length - filled)
        print(f"\r[{bar}] {porcentaje:>6.1%}  ({i}/{total})", end="", flush=True)

        transcurrido = time.time() - inicio
    
        # ETA = tiempo restante estimado
        if i > 5:  # evitamos divisiones por números muy pequeños al inicio
            tiempo_por_item = transcurrido / i
            restante = tiempo_por_item * (total - i)
            min_rest = int(restante // 60)
            seg_rest = int(restante % 60)
        else:
            min_rest = seg_rest = "--"
        
        min_trans = int(transcurrido // 60)
        seg_trans = int(transcurrido % 60)
        
        print(
            f"\r[{bar}] {porcentaje:>6.1%}  ({i:5d}/{total})  "
            f" {min_trans:2d}m {seg_trans:02d}s  |  ETA: {min_rest:2d}m {seg_rest:02d}s   ",
            end="", flush=True
        )

        # La API acepta código con 5 dígitos (ceros a la izquierda)
        codigo = codigo_original.zfill(5)

        url = f"https://api1.correos.es/digital-services/searchengines/api/v1/postalcodes/polygon?postalcode={codigo}"

        try:

            headers = {
                "Origin": "https://www.correos.es",
                "Referer": "https://www.correos.es",
                "User-Agent": random.choice(USER_AGENTS),
                "Accept-Language": "es-ES,es;q=0.9",
                "Accept": "*/*",
                "Host": "api1.correos.es"
            }

            response = session.get(url, headers=headers, timeout=TIMEOUT)
            response.raise_for_status()

            raw_data = response.json()

            # ── Transformación a GeoJSON estándar ───────────────────────
            features_raw = raw_data.get("features", [])

            for feat_raw in features_raw:
                rings = feat_raw.get("geometry", {}).get("rings", [])
                if not rings:
                    continue

                # Construimos geometría GeoJSON estándar
                if len(rings) == 1:
                    geom_type = "Polygon"
                    coordinates = rings  # ya es lista de anillos
                else:
                    geom_type = "MultiPolygon"
                    coordinates = [[ring] for ring in rings]  # cada anillo → su propio polígono

                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": geom_type,
                        "coordinates": coordinates
                    },
                    "properties": {
                        "codigo_postal": codigo,
                        "names": data["names"],
                        "source": "Correos.es Polygon API"
                    }
                }

                gjson["features"].append(feature)

            logger.info(
                "[%d/%d] OK %s → %d polígono(s)",
                i, total, codigo, len(features_raw)
            )

        except requests.exceptions.HTTPError as e:
            print((
                "[%d/%d] HTTP %d → %s | %s",
                i, total, response.status_code, codigo, url
            ))
            logger.warning(
                "[%d/%d] HTTP %d → %s | %s",
                i, total, response.status_code, codigo, url
            )
        except requests.exceptions.Timeout:
            print(("[%d/%d] Timeout → %s", i, total, codigo))
            logger.warning("[%d/%d] Timeout → %s", i, total, codigo)
        except Exception as e:
            print((
                "[%d/%d] Error procesando %s | %s",
                i, total, codigo, url
            ))
            logger.exception(
                "[%d/%d] Error procesando %s | %s",
                i, total, codigo, url
            )

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    # ── Guardar resultado final ───────────────────────────────────────────
    if path:
        try:
            final_path = path.replace(".json", f"_{gjson_name}.geojson")
            with open(final_path, "w", encoding="utf-8") as f:
                json.dump(gjson, f, ensure_ascii=False, indent=2)
            logger.info("GeoJSON guardado en: %s", final_path)
        except Exception:
            logger.exception("No se pudo guardar GeoJSON final")

    logger.info("Proceso completado → %d features", len(gjson["features"]))

    return gjson, gjson_name

def codigospostales_codigos_postales(
    path: Optional[str] = None,
    combinar_todo: bool = True
) -> Tuple[Dict[str, Any], str]:
    """
    Descarga KML de provincias desde codigospostales.com,
    los convierte a GeoJSON y los guarda/combina en el path indicado.
    
    Returns:
        (geojson_resultado, nombre_archivo)
    """

    KML_NS = {
        "kml": "http://www.opengis.net/kml/2.2",
        "gx":  "http://www.google.com/kml/ext/2.2"
    }

    def kml_to_geojson_features(kml_root, province_code: str) -> list:
        """
        Convierte Placemarks de KML a Features GeoJSON.
        Maneja:
        - Polygon simple
        - MultiGeometry (varios Polygon)
        - innerBoundaryIs (agujeros)
        """
        features = []

        # Buscamos todos los Placemarks (pueden estar directamente o dentro de Folder)
        for placemark in kml_root.findall(".//kml:Placemark", KML_NS):
            # Nombre (puede no existir)
            name_elem = placemark.find("kml:name", KML_NS)
            name = name_elem.text.strip() if name_elem is not None and name_elem.text else f"CP desconocido - Prov {province_code}"

            # Intentamos obtener CODPOS desde ExtendedData (muy útil para propiedades)
            codpos = "desconocido"
            extended = placemark.find("kml:ExtendedData", KML_NS)
            if extended is not None:
                for sd in extended.findall(".//kml:SimpleData", KML_NS):
                    if sd.get("name") == "CODPOS":
                        codpos = sd.text.strip()

            # Recolectamos todas las geometrías del Placemark
            geometries = extract_geometries(placemark)

            for geom_type, coordinates in geometries:
                if not coordinates:
                    continue

                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": geom_type,
                        "coordinates": coordinates
                    },
                    "properties": {
                        "province_code": province_code,
                        "codpos": codpos,
                        "name": name,
                        "source": "codigospostales.com KML",
                        "description": f"Provincia {province_code} - Código Postal {codpos}"
                    }
                }
                features.append(feature)

        return features

    def extract_geometries(placemark_elem) -> list:
        """
        Devuelve lista de tuplas (geom_type, coordinates)
        Soporta Polygon y MultiGeometry
        """
        results = []

        # Caso 1: Polygon directo
        for poly in placemark_elem.findall(".//kml:Polygon", KML_NS):
            coords_list = extract_polygon_coordinates(poly)
            if coords_list:
                results.append(("Polygon", coords_list))

        # Caso 2: MultiGeometry
        for multi in placemark_elem.findall(".//kml:MultiGeometry", KML_NS):
            polygons_coords = []

            for poly in multi.findall(".//kml:Polygon", KML_NS):
                poly_coords = extract_polygon_coordinates(poly)
                if poly_coords:
                    polygons_coords.append(poly_coords)

            if polygons_coords:
                if len(polygons_coords) == 1:
                    # Si solo hay uno → tratamos como Polygon normal
                    results.append(("Polygon", polygons_coords[0]))
                else:
                    # Varios polígonos → MultiPolygon
                    results.append(("MultiPolygon", polygons_coords))

        return results

    def extract_polygon_coordinates(polygon_elem) -> list:
        """
        Extrae coordenadas de un <Polygon> incluyendo posibles innerBoundaryIs
        Retorna: [[outer_coords], [inner1], [inner2], ...]
        """
        outer_ring = None
        inner_rings = []

        # outerBoundaryIs (obligatorio)
        outer_elem = polygon_elem.find(".//kml:outerBoundaryIs/kml:LinearRing/kml:coordinates", KML_NS)
        if outer_elem is not None and outer_elem.text:
            outer_ring = parse_coordinates(outer_elem.text.strip())

        if not outer_ring:
            return []

        # innerBoundaryIs (opcional, puede haber varios)
        for inner_elem in polygon_elem.findall(".//kml:innerBoundaryIs/kml:LinearRing/kml:coordinates", KML_NS):
            if inner_elem.text:
                inner_coords = parse_coordinates(inner_elem.text.strip())
                if inner_coords:
                    inner_rings.append(inner_coords)

        # Formato GeoJSON para Polygon: [outer, inner1?, inner2?, ...]
        coordinates = [outer_ring]
        if inner_rings:
            coordinates.extend(inner_rings)

        return coordinates

    def parse_coordinates(text: str) -> list:
        """ 'lon,lat,alt lon,lat,alt ...' → [[lon, lat], ...] """
        coords = []
        for part in text.split():
            try:
                lon_str, lat_str, *_ = part.split(",")
                lon = float(lon_str)
                lat = float(lat_str)
                coords.append([lon, lat])  # GeoJSON: [lon, lat]
            except (ValueError, IndexError):
                continue

        # Mínimo razonable para un polígono cerrado
        if len(coords) >= 4 and coords[0] == coords[-1]:
            return coords[:-1]  # quitamos el último punto repetido (opcional pero limpio)

        return coords if len(coords) >= 3 else []
    
    logger.info("------ DESCARGA KML PROVINCIAS → GEOJSON ------")

    gjson_name = "codigospostales_codigos_postales"

    base_url = "https://www.codigospostales.com/kml/k{}.kml"
    
    provincias = [
        "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
        "21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
        "31", "32", "33", "34", "35", "36", "37", "38", "39", "40",
        "41", "42", "43", "44", "45", "46", "47", "48", "49", "50",
        "51", "52"
    ]

    feature_collection = {
        "type": "FeatureCollection",
        "features": []
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; KMLDownloader/1.0)",
        "Accept": "*/*"
    }
    headers = {
        "Origin": "https://www.codigospostales.com/",
        "Referer": "https://www.codigospostales.com/",
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "es-ES,es;q=0.9",
        "Accept": "*/*",
    }

    session = crear_session_robusta()

    for i, cod_prov in enumerate(provincias, 1):
        url = base_url.format(cod_prov)
        logger.info(f"[{i}/{len(provincias)}] Intentando descargar: {url}")

        try:
            resp = session.get(url, headers=headers, timeout=TIMEOUT)
            
            if resp.status_code == 404:
                logger.info(f"  → No existe KML para provincia {cod_prov}")
                continue
                
            resp.raise_for_status()
            
            # Intentamos parsear como KML/XML
            try:
                root = ET.fromstring(resp.content)
            except ET.ParseError:
                logger.warning(f"  → No es XML válido → {url}")
                continue

            # ── Conversión básica KML → GeoJSON (simplificada) ───────────────
            features = kml_to_geojson_features(root, cod_prov)
            
            if features:
                feature_collection["features"].extend(features)
                logger.info(f"  → OK - {len(features)} feature(s) añadida(s)")
            else:
                logger.warning(f"  → No se encontraron geometrías válidas")

        except requests.exceptions.HTTPError as e:
            logger.warning(f"  → HTTP {resp.status_code} - {e}")
        except requests.exceptions.Timeout:
            logger.warning(f"  → Timeout")
        except Exception as e:
            logger.exception(f"  → Error procesando {url}")

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    # Guardar resultado
    if feature_collection["features"]:
        save_geojson(feature_collection, path, gjson_name)
        return feature_collection, gjson_name
    
    else:
        logger.warning("No se obtuvo ninguna geometría válida")
        return {"type": "FeatureCollection", "features": []}, gjson_name

def INE_secciones_censales(path: Optional[str]):
    """
    Descarga el shapefile. de secciones censales (INE) dentro de un ZIP y lo convierte a gjson.
    - Lee .shp/.shx/.dbf directamente desde el ZIP (sin extraer a disco).
    - Reproyecta a EPSG:4326 si hay .prj.
    - Devuelve un dict gjson (FeatureCollection). Si `path` no es None, guarda el gjson en esa ruta.
    """
    URLzipsec_cens = "https://www.ine.es/prodyser/cartografia/seccionado_2025.zip"

    gjson_name = "INE_secciones_censales"

    logger.info("Descargando ZIP de secciones censales: %s", URLzipsec_cens)
    gjson = shp2geojson(URLzipsec_cens)
    save_geojson(gjson, path,gjson_name)
    logger.info("Proceso completado.")
    return gjson, gjson_name

def eurostat_countries(path: Optional[str], scale: str = "60M"):
    if scale not in VALID_SCALES:
        raise ValueError(f"scale {scale} no permitido. Valores: {VALID_SCALES}")

    url = f"https://gisco-services.ec.europa.eu/distribution/v2/countries/geojson/CNTR_RG_{scale}_2024_4326.geojson"
    return descarga_eurostat(path, "eurostat_countries", url)

def eurostat_communes(path: Optional[str]):
    url = "https://gisco-services.ec.europa.eu/distribution/v2/communes/geojson/COMM_RG_01M_2016_4326.geojson"
    return descarga_eurostat(path, "eurostat_communes", url)

def eurostat_coastal(path: Optional[str], scale: str = "60M"):
    if scale not in VALID_SCALES:
        raise ValueError(f"scale {scale} no permitido. Valores: {VALID_SCALES}")

    url = f"https://gisco-services.ec.europa.eu/distribution/v2/coas/geojson/COAS_RG_{scale}_2016_4326.geojson"
    return descarga_eurostat(path, "eurostat_coastal", url)

def eurostat_LAU(path: Optional[str]):
    url = "https://gisco-services.ec.europa.eu/distribution/v2/lau/geojson/LAU_RG_01M_2024_4326.geojson"
    return descarga_eurostat(path, "eurostat_LAU", url)

def eurostat_NUTS(path: Optional[str], scale: str = "60M", nut_level: Optional[int]= None):
    if scale not in VALID_SCALES:
        raise ValueError(f"scale {scale} no permitido. Valores: {VALID_SCALES}")

    if nut_level not in NUTS_LEVELS:
        raise ValueError(f"nut_level {nut_level} no permitido. Valores: {NUTS_LEVELS}")

    url = f"https://gisco-services.ec.europa.eu/distribution/v2/nuts/geojson/NUTS_RG_{scale}_2024_4326.geojson"
    return descarga_eurostat(path, "eurostat_NUTS", url,nut_level)

def eurostat_URAU(path: Optional[str]):
    url = "https://gisco-services.ec.europa.eu/distribution/v2/urau/geojson/URAU_RG_100K_2024_4326.geojson"
    return descarga_eurostat(path, "eurostat_URAU", url)

def madrid_barrios(path: Optional[str]):
    url = "https://geoportal.madrid.es/fsdescargas/IDEAM_WBGEOPORTAL/LIMITES_ADMINISTRATIVOS/Barrios/Barrios.zip"
    gjson_name = "madrid_barrios"

    logger.info("Descargando ZIP de barrios: %s", url)
    gjson = shp2geojson(url)
    save_geojson(gjson, path,gjson_name)
    logger.info("Proceso completado.")
    return gjson, gjson_name

def madrid_barrios_historicos(path: Optional[str], year: int = None, layer: str = None):
    url = "https://geoportal.madrid.es/fsdescargas/IDEAM_WBGEOPORTAL/LIMITES_ADMINISTRATIVOS/Barrios/Historicos/Divisiones_Historicas.zip"
    gjson_name = "madrid_barrios_historicos"

    logger.info("Descargando ZIP de barrios historicos: %s", url)
    gjson = shp2geojson(url)

    if year:
       if not year in VALID_BARRIOS_DISTRITOS_HISTORICOS_ANNO:
           logger.error(f"año {year} no permitido. Valores: {VALID_BARRIOS_DISTRITOS_HISTORICOS_ANNO}")
           raise ValueError(f"año {year} no permitido. Valores: {VALID_BARRIOS_DISTRITOS_HISTORICOS_ANNO}")
       gjson["features"] = [f for f in gjson["features"] if f["properties"]["nombre_archivo"] == f"SHAPES_{year}.zip"]
       gjson_name = f"{gjson_name}_{year}"
    if layer:
       if not layer in VALID_BARRIOS_DISTRITOS_HISTORICOS_CAPAS:
           logger.error(f"capa {layer} no permitido. Valores: {VALID_BARRIOS_DISTRITOS_HISTORICOS_CAPAS}")
           raise ValueError(f"capa {layer} no permitido. Valores: {VALID_BARRIOS_DISTRITOS_HISTORICOS_CAPAS}")
       gjson["features"] = [f for f in gjson["features"] if layer.lower() in  f["properties"]["layer"].lower()  ]
       gjson_name = f"{gjson_name}_{layer}"

    save_geojson(gjson, path,gjson_name)
    logger.info("Proceso completado.")
    return gjson, gjson_name

def madrid_distritos(path: Optional[str]):
    url = "https://geoportal.madrid.es/fsdescargas/IDEAM_WBGEOPORTAL/LIMITES_ADMINISTRATIVOS/Distritos/Distritos.zip"
    gjson_name = "madrid_distritos"

    logger.info("Descargando ZIP de distritos: %s", url)
    gjson = shp2geojson(url)
    save_geojson(gjson, path,gjson_name)
    logger.info("Proceso completado.")
    return gjson, gjson_name

def madrid_distritos_historicos(path: Optional[str], year: int = None, layer: str = None):
    url = "https://geoportal.madrid.es/fsdescargas/IDEAM_WBGEOPORTAL/LIMITES_ADMINISTRATIVOS/Distritos/Historicos/Divisiones_Historicas.zip"
    gjson_name = "madrid_distritos_historicos"

    logger.info("Descargando ZIP de distritos historicos: %s", url)
    gjson = shp2geojson(url)

    if year:
       if not year in VALID_BARRIOS_DISTRITOS_HISTORICOS_ANNO:
           logger.error(f"año {year} no permitido. Valores: {VALID_BARRIOS_DISTRITOS_HISTORICOS_ANNO}")
           raise ValueError(f"año {year} no permitido. Valores: {VALID_BARRIOS_DISTRITOS_HISTORICOS_ANNO}")
       gjson["features"] = [f for f in gjson["features"] if f["properties"]["nombre_archivo"] == f"SHAPES_{year}.zip"]
       gjson_name = f"{gjson_name}_{year}"
    if layer:
       if not layer in VALID_BARRIOS_DISTRITOS_HISTORICOS_CAPAS:
           logger.error(f"capa {layer} no permitido. Valores: {VALID_BARRIOS_DISTRITOS_HISTORICOS_CAPAS}")
           raise ValueError(f"capa {layer} no permitido. Valores: {VALID_BARRIOS_DISTRITOS_HISTORICOS_CAPAS}")
       gjson["features"] = [f for f in gjson["features"] if layer.lower() in  f["properties"]["layer"].lower()  ]
       gjson_name = f"{gjson_name}_{layer}"

    save_geojson(gjson, path,gjson_name)
    logger.info("Proceso completado.")
    return gjson, gjson_name




# =========================
# Funciones transversales
# =========================
def crear_session_robusta():
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=MAX_RETRIES)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def reordenar_array_aleatoriamente(array):
    d_aleatorio = dict(random.sample(list(array.items()), len(array)))
    return d_aleatorio

def shp2geojson(url: str):
    import requests, io, zipfile, shapefile
    from pyproj import CRS, Transformer

    def process_single_zip(zip_bytes: bytes, zip_label: str):
        """Procesa un ZIP que contiene un shapefile y devuelve una lista de features"""
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:

                names = zf.namelist()

                # Agrupar por base (ruta+nombre sin extensión) para soportar múltiples shapefiles
                bases = {}
                for name in names:
                    base, ext = os.path.splitext(name)
                    base_low = base.lower()
                    ext_low = ext.lower()
                    if base_low not in bases:
                        bases[base_low] = {}
                    bases[base_low][ext_low] = name

                # Seleccionar solo las bases que contienen shp+shx+dbf
                valid_bases = [b for b, files in bases.items() if 
                               ".shp" in files and ".shx" in files and ".dbf" in files]

                if not valid_bases:
                    logger.warning(f"{zip_label}: No contiene shapefile completo.")
                    return []

                features = []

                # Procesar cada shapefile encontrado dentro del ZIP
                for base_low in valid_bases:
                    logger.debug(f"capa: {base_low}")

                    files = bases[base_low]
                    shp_name = files['.shp']
                    shx_name = files['.shx']
                    dbf_name = files['.dbf']
                    prj_name = files.get('.prj')

                    # Open files from the ZIP
                    shp_f = zf.open(shp_name)
                    shx_f = zf.open(shx_name)
                    dbf_f = zf.open(dbf_name)

                    # Read CRS if available (por shapefile)
                    src_crs = None
                    if prj_name:
                        raw = zf.open(prj_name).read()
                        try: wkt = raw.decode("utf-8")
                        except: wkt = raw.decode("latin-1", errors="ignore")
                        try: src_crs = CRS.from_wkt(wkt)
                        except: pass

                    transformer = None
                    to_epsg = 4326
                    if src_crs:
                        try:
                            dst = CRS.from_epsg(to_epsg)
                            transformer = Transformer.from_crs(src_crs, dst, always_xy=True)
                        except:
                            transformer = None

                    reader = shapefile.Reader(shp=shp_f, shx=shx_f, dbf=dbf_f, encoding="latin-1")
                    fields = reader.fields[1:]
                    field_names = [f[0] for f in fields]

                    def proj(x, y):
                        return transformer.transform(x, y) if transformer else (x, y)

                    def proj_list(coords):
                        return [proj(x, y) for x, y in coords]

                    def shape_to_geom(shape):
                        t = shape.shapeType
                        if t in [shapefile.POINT, shapefile.POINTZ]:
                            x, y = proj(*shape.points[0])
                            return {"type": "Point", "coordinates": [x, y]}
                        elif t in [shapefile.MULTIPOINT, shapefile.MULTIPOINTZ]:
                            return {"type": "MultiPoint", "coordinates": proj_list(shape.points)}
                        elif t in [shapefile.POLYLINE, shapefile.POLYLINEZ]:
                            parts = list(shape.parts) + [len(shape.points)]
                            lines = [proj_list(shape.points[parts[i]:parts[i+1]])
                                     for i in range(len(parts)-1)]
                            return {"type": "LineString", "coordinates": lines[0]} if len(lines)==1 \
                                   else {"type":"MultiLineString","coordinates":lines}
                        elif t in [shapefile.POLYGON, shapefile.POLYGONZ]:
                            parts = list(shape.parts) + [len(shape.points)]
                            rings = [proj_list(shape.points[parts[i]:parts[i+1]])
                                     for i in range(len(parts)-1)]
                            return {"type": "Polygon", "coordinates": rings}
                        return shape.__geo_interface__

                    # Nombre de capa: zip_label + nombre del shp (sin extensión)
                    shp_basename = os.path.splitext(os.path.basename(shp_name))[0]
                    layer_name = f"{zip_label}_{shp_basename}"

                    for sr in reader.iterShapeRecords():
                        props = {field_names[i]: sr.record[i] for i in range(len(field_names))}
                        props["nombre_archivo"] = zip_label
                        props["layer"] = layer_name
                        geom = shape_to_geom(sr.shape)
                        features.append({"type":"Feature","geometry":geom,"properties":props})

                return features

        except Exception as e:
            logger.error(f"Error procesando zip interno {zip_label}: {e}")
            return []

    # -------------------------------------------------------------------------
    # Descargar ZIP externo
    # -------------------------------------------------------------------------
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"No se pudo descargar el ZIP: {e}")
        return []

    external_zip = zipfile.ZipFile(io.BytesIO(resp.content))
    names = external_zip.namelist()

    all_features = []

    # -------------------------------------------------------------------------
    # Detectar si existen ZIPs dentro del ZIP
    # -------------------------------------------------------------------------
    internal_zips = [n for n in names if n.lower().endswith(".zip")]

    if internal_zips:
        # Procesar cada ZIP interno
        for zname in internal_zips:
            logger.debug(f"Procesando ZIP interno: {zname}")
            data = external_zip.read(zname)
            feats = process_single_zip(data, zname)
            all_features.extend(feats)
    else:
        # Procesar el ZIP como si fuera un shapefile normal
        logger.debug("ZIP sin ZIPs internos. Procesando shapefile directo.")
        data = resp.content
        all_features.extend(process_single_zip(data, "archivo_principal"))

    return {"type": "FeatureCollection", "features": all_features}

def get_total_count(session: requests.Session, params: dict) -> Optional[int]:
    """Obtiene el número total de features (numberMatched) con limit=1"""
    url = f"{BASE_URL_API_IGN}?f=json&limit=1"
    url += "".join(f"&{k}={v}" for k, v in params.items())

    try:
        resp = session.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        count = data.get("numberMatched")
        logger.info("Total de objetos encontrados: %d", count)
        return count
    except requests.Timeout:
        logger.warning("Timeout al consultar número total de objetos")
    except requests.RequestException as e:
        logger.error("Error HTTP al obtener conteo: %s", e)
    except Exception as e:
        logger.exception("Error inesperado al obtener conteo: %s", e)
    
    return None

def download_all_features(
    session: requests.Session,
    params: dict,
    page_size: int = DEFAULT_PAGE_SIZE
) -> dict:
    """Descarga todas las features paginando si es necesario"""
    total = get_total_count(session, params)
    if total is None:
        raise ValueError("No se pudo obtener el número total de elementos")

    gjson = {"type": "FeatureCollection", "features": []}

    if total == 0:
        logger.warning("No se encontraron elementos")
        return gjson

    offset = 0
    total_returned = 0
    while True:
        url_params = params.copy()
        url_params["f"] = "json"
        url_params["limit"] = page_size
        url_params["offset"] = offset

        url = f"{BASE_URL_API_IGN}?{ '&'.join(f'{k}={v}' for k,v in url_params.items()) }"

        try:
            resp = session.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            page = resp.json()

            features = page.get("features", [])
            returned = page.get("numberReturned", 0)
            total_returned += returned


            if returned == 0:
                break

            # Guardamos TODAS las features de la página (no solo la primera)
            gjson["features"].extend(features)

            # Logging de progreso
            geometry_type = (
                features[0]["geometry"]["type"]
                if features and features[0].get("geometry")
                else "—"
            )
            logger.info(
                "offset=%d  total_returned=%d/%d  geometry=%s",
                offset, total_returned, total, geometry_type
            )

            offset += page_size
            time.sleep(SLEEP_BETWEEN_REQUESTS)

        except Exception as e:
            logger.exception("Error en página offset=%d: %s", offset, e)
            raise

    return gjson

def save_geojson(gjson: dict, filepath: str, name: str) -> None:
    """Guarda el FeatureCollection en disco"""
    # Comprobar si el directorio del archivo existe, de lo contrario crearlo
    if not filepath:
        filepath = f"./"

    directory = os.path.dirname(filepath)
    if not os.path.exists(directory):
        os.makedirs(directory)

    if filepath.lower().endswith(".geojson") or filepath.lower().endswith(".json"):
        filepath = filepath

    elif not filepath.endswith("/"):
        filepath = filepath.rstrip("/") + "/"
        filepath += f"{name}.geojson"
    else:
        filepath += f"{name}.geojson"
    
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(gjson, f, ensure_ascii=False, indent=2)
        logger.info("gjson guardado correctamente: %s", filepath)
    except Exception:
        logger.exception("No se pudo guardar el archivo: %s", filepath)

def descarga_IGN(
    nivel: str, 
    path: Optional[str] = None,
    pag: int = DEFAULT_PAGE_SIZE,
    name: Optional[str] = None,
) -> None:
    """
    Descarga los features de un nivel administrativo y los guarda en disco
    """

    logger.info("Iniciando descarga: %s - IGN API-Features", nivel)

    session = crear_session_robusta()

    params = {
        "nationallevelname": nivel
    }

    try:
        gjson = download_all_features(session, params, page_size=pag)
        save_geojson(gjson, path,name)
        return gjson
    except Exception as e:
        logger.error("Fallo general al descargar %s: %s", nivel, e)
    finally:
        logger.info("Proceso finalizado para %s", nivel)

def descarga_eurostat(path: Optional[str], name: str, url: str, nut_level: Optional[int]= None) -> tuple:
    """
    Función genérica para descargar un GeoJSON desde Eurostat.
    """
    try:
        resp = requests.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        gjson = resp.json()
        logger.info("Total de objetos encontrados: %d", len(gjson["features"]))

    except requests.Timeout:
        logger.warning("Timeout al consultar número total de objetos")
        raise
    except requests.RequestException as e:
        logger.error("Error HTTP al obtener conteo: %s", e)
        raise
    except Exception as e:
        logger.exception("Error inesperado al obtener conteo: %s", e)
        raise

    if nut_level:
        gjson["features"] = [f for f in gjson["features"] if f["properties"]["LEVL_CODE"] == nut_level]
        name = f"{name}_{nut_level}"

    save_geojson(gjson, path, name)
    logger.info("Proceso completado.")

    return gjson, name

def simplify_geojson(
    geojson_data,
    simplification_tolerance,           # en grados (ej: 0.0005 - 0.005)
    filepath,                           # ← parámetro para guardar
    geojson_name,                       # ← nombre base para el archivo de salida
    keep_largest_if_all_removed=True,
    simplify_boundary=True
):
    """
    Simplifica GeoJSON con:
    - Comportamiento configurable cuando TODAS las partes son pequeñas:
      keep_largest_if_all_removed=True  → conserva la parte más grande
      keep_largest_if_all_removed=False → elimina toda la feature
    """
    _GEOD = Geod(ellps="WGS84")

    # ── Utilidades de área geodésica ────────────────────────────────────────
    def ring_area_m2(coords):
        lons, lats = zip(*coords)
        area, _ = _GEOD.polygon_area_perimeter(lons, lats)
        return abs(area)

    def polygon_area_geodesic_m2(poly):
        if poly.is_empty:
            return 0.0
        ext = ring_area_m2(list(poly.exterior.coords))
        holes = sum(ring_area_m2(list(ring.coords)) for ring in poly.interiors)
        return max(0.0, ext - holes)

    # ── Quitar agujeros pequeños ────────────────────────────────────────────
    def remove_small_holes(poly: Polygon,  min_vertices_hole: int = MIN_VERTICES):
        """
        Elimina agujeros que sean:
        - demasiado pequeños (área < min_area)
        - o tengan muy pocos vértices (≤ min_vertices_hole)
        """
        if not poly.interiors:
            return poly, 0

        kept = []
        removed_count = 0

        for ring in poly.interiors:
            coords = list(ring.coords)
            vertex_count = len(coords) - 1  # el último punto es igual al primero
            
            # Condición de eliminación
            if  vertex_count <= min_vertices_hole:
                removed_count += 1
            else:
                kept.append(coords)

        if removed_count == 0:
            return poly, 0

        # Reconstruimos el polígono
        return Polygon(poly.exterior, kept), removed_count

    # ── Limpieza completa de una geometría ──────────────────────────────────
    def clean_geometry(
        geom,
        keep_largest: bool,
        min_vertices: int = MIN_VERTICES
    ) -> tuple[BaseGeometry | None, int, int, int]:  # geom, holes_rem, parts_rem, feat_rem
        if geom is None or geom.is_empty:
            return None, 0, 0, 0

        holes_removed = 0

        # Caso Polygon individual
        if isinstance(geom, Polygon):
            geom, h = remove_small_holes(geom, min_vertices)
            holes_removed += h

            exterior_coords = list(geom.exterior.coords)
            vertex_count = len(exterior_coords) - 1

            if vertex_count <= min_vertices:
                if keep_largest:
                    return geom, holes_removed, 0, 0
                else:
                    return None, holes_removed, 0, 1
            return geom, holes_removed, 0, 0

        # Caso MultiPolygon
        if isinstance(geom, MultiPolygon):
            cleaned_parts = []
            total_holes = 0
            total_parts_rem = 0
            
            for part in geom.geoms:
                part_clean, h = remove_small_holes(part, min_vertices)
                total_holes += h
                
                # Cálculos finales de la parte limpiada
                exterior_coords = list(part_clean.exterior.coords)
                vertex_count = len(exterior_coords) - 1  # sin contar punto de cierre
                
                # Mantenemos SOLO si cumple AMBOS criterios mínimos
                if vertex_count > min_vertices:
                    cleaned_parts.append(part_clean)
                else:
                    total_parts_rem += 1
            
            # Caso especial: no quedó ninguna parte válida
            if not cleaned_parts:
                if keep_largest and len(geom.geoms) > 0:
                    largest = max(geom.geoms, key=polygon_area_geodesic_m2)
                    largest_clean, h_extra = remove_small_holes(largest, min_vertices)
                    total_holes += h_extra
                    return largest_clean, total_holes, total_parts_rem, 0
                else:
                    return None, total_holes, total_parts_rem, 1
            
            # Resultado normal
            if len(cleaned_parts) == 1:
                return cleaned_parts[0], total_holes, total_parts_rem, 0
            return MultiPolygon(cleaned_parts), total_holes, total_parts_rem, 0

        # Otros tipos de geometría
        return geom, 0, 0, 0

    # ── Flujo principal ─────────────────────────────────────────────────────
    try:
        logger.info("Leyendo GeoJSON...")
        gdf = gpd.GeoDataFrame.from_features(geojson_data["features"], crs="EPSG:4326")

        if len(gdf) == 0:
            logger.warning("GeoDataFrame vacío")
            return geojson_data

        logger.info(f"Features originales: {len(gdf)}")

        stats = {"holes_removed": 0, "parts_removed": 0, "features_removed": 0}

        # 1. Limpieza inicial
        logger.info(f"Limpieza inicial ")
        results = gdf.geometry.apply(
            lambda g: clean_geometry(g, keep_largest_if_all_removed)
        )
        gdf["geometry"] = [r[0] for r in results]
        for r in results:
            stats["holes_removed"] += r[1]
            stats["parts_removed"] += r[2]
            stats["features_removed"] += r[3]
        gdf = gdf[gdf.geometry.notna()].copy()

        # 2. Simplificación
        if simplification_tolerance > 0:
            logger.info(f"Simplificando tolerance = {simplification_tolerance:.6f}°")
            gdf["geometry"] = gdf.geometry.simplify_coverage(
                tolerance=simplification_tolerance,
                simplify_boundary=simplify_boundary
            )
            gdf["geometry"] = gdf.geometry.make_valid()

        # 3. Limpieza final (captura artefactos de la simplificación)
        logger.info("Limpieza post-simplificación...")
        results = gdf.geometry.apply(
            lambda g: clean_geometry(g, keep_largest_if_all_removed)
        )
        gdf["geometry"] = [r[0] for r in results]
        for r in results:
            stats["holes_removed"] += r[1]
            stats["parts_removed"] += r[2]
            stats["features_removed"] += r[3]
        gdf = gdf[gdf.geometry.notna()].copy()

        # ── Generar y guardar resultado ─────────────────────────────────────
        simplified_geojson = {
            "type": "FeatureCollection",
            "features": json.loads(gdf.to_json())["features"]
        }

        # Guardado (descomenta o adapta según tu función save_geojson)
        out_name = f"{geojson_name}_simpl_{str(simplification_tolerance).replace(".", "-")}"
        save_geojson(simplified_geojson, filepath, out_name)  # ← tu función de guardado

        logger.info(
            "Proceso completado:\n"
            f"  Agujeros eliminados:     {stats['holes_removed']:,d}\n"
            f"  Partes eliminadas:       {stats['parts_removed']:,d}\n"
            f"  Features eliminados:     {stats['features_removed']:,d}\n"
            f"  Features finales:        {len(gdf):,d}"
        )

        return simplified_geojson

    except Exception as e:
        logger.error("Error en simplify_geojson", exc_info=True)
        raise

def fix_topology(geojson_data: dict, snap_tolerance: float = 0.0, dissolve: bool = False) -> dict:
    """Arregla la topología de una FeatureCollection GeoJSON.

    - Valida geometrías (intenta `make_valid` o `buffer(0)` como fallback).
    - Opcionalmente hace `snap` con tolerancia `snap_tolerance` (grados).
    - Si `dissolve=True` disuelve/une todas las geometrías y devuelve los polígonos resultantes
      como features (se pierden las propiedades originales).

    Devuelve un FeatureCollection GeoJSON corregido.
    """
    try:
        from shapely.ops import unary_union, snap
        try:
            from shapely.ops import make_valid as shapely_make_valid
        except Exception:
            shapely_make_valid = None
        from shapely.geometry import mapping, Polygon, MultiPolygon

        gdf = gpd.GeoDataFrame.from_features(geojson_data.get("features", []), crs="EPSG:4326")

        if gdf.empty:
            logger.warning("fix_topology: GeoDataFrame vacío")
            return {"type": "FeatureCollection", "features": []}

        # 1) Validar geometrías
        def _make_valid(g):
            if g is None:
                return g
            try:
                if shapely_make_valid:
                    mg = shapely_make_valid(g)
                else:
                    mg = g if g.is_valid else g.buffer(0)
                return mg
            except Exception:
                try:
                    return g.buffer(0)
                except Exception:
                    return g

        gdf["geometry"] = gdf.geometry.apply(_make_valid)

        # 2) Snap (si se solicita)
        if snap_tolerance and snap_tolerance > 0:
            union = unary_union([g for g in gdf.geometry if g is not None and not g.is_empty])
            gdf["geometry"] = gdf.geometry.apply(
                lambda geom: snap(geom, union, snap_tolerance) if geom is not None and not geom.is_empty else geom
            )
            # asegurar validez
            gdf["geometry"] = gdf.geometry.apply(lambda g: g if g is None or g.is_valid else g.buffer(0))

        # 3) Opcional: disolver/union (quita solapes, produce geometrías no superpuestas)
        if dissolve:
            merged = unary_union([g for g in gdf.geometry if g is not None and not g.is_empty])
            features = []
            if merged is None or merged.is_empty:
                return {"type": "FeatureCollection", "features": []}

            geoms = []
            if isinstance(merged, Polygon):
                geoms = [merged]
            elif isinstance(merged, MultiPolygon):
                geoms = list(merged.geoms)
            else:
                # si es otro tipo, lo incluimos tal cual
                geoms = [merged]

            for geom in geoms:
                features.append({"type": "Feature", "geometry": mapping(geom), "properties": {"source": "fix_topology"}})

            logger.info("fix_topology: disuelto en %d feature(s)", len(features))
            return {"type": "FeatureCollection", "features": features}

        # 4) Si no disolvemos, devolvemos las mismas features con geometrías corregidas
        gdf = gdf[gdf.geometry.notna() & (~gdf.geometry.is_empty)].copy()
        out = {"type": "FeatureCollection", "features": json.loads(gdf.to_json())["features"]}
        logger.info("fix_topology: resultado %d feature(s)", len(out["features"]))
        return out

    except Exception as e:
        logger.exception("Error en fix_topology: %s", e)
        return {"type": "FeatureCollection", "features": []}

# =========================
# Main
# =========================
if __name__ == "__main__":
    geojson_data, geojson_name = None, None
    logger.setLevel(logging.DEBUG)  # "DEBUG" "INFO", "WARNING", "ERROR"
    path="./geojson/"

    '''
    IGN
    '''
    # geojson_data, geojson_name = IGN_pais(path=path)
    # geojson_data, geojson_name = IGN_comunidades_autonomas(path=path)
    # geojson_data, geojson_name = IGN_provincias(path=path)
    # geojson_data, geojson_name = IGN_municipios(path=path)
    # geojson_data, geojson_name = IGN_codigos_postales(path=path, descarga_ID_json=True)

    '''
    INE
    '''
    # geojson_data, geojson_name = INE_secciones_censales(path=path)

    '''
    Correos y codigospostales
    '''
    # geojson_data, geojson_name = correos_codigos_postales(path=path, descarga_ID_json=False)
    # geojson_data, geojson_name = codigospostales_codigos_postales(path=path)

    '''
    Eurostats
    '''
    # geojson_data, geojson_name = eurostat_countries(path=path)
    # geojson_data, geojson_name = eurostat_communes(path=path)
    # geojson_data, geojson_name = eurostat_coastal(path=path)
    # geojson_data, geojson_name = eurostat_NUTS(path=path, nut_level=0)
    # geojson_data, geojson_name = eurostat_NUTS(path=path, nut_level=1)
    # geojson_data, geojson_name = eurostat_NUTS(path=path, nut_level=2)
    # geojson_data, geojson_name = eurostat_NUTS(path=path, nut_level=3)
    # geojson_data, geojson_name = eurostat_LAU(path=path)
    # geojson_data, geojson_name = eurostat_URAU(path=path)

    '''
    Madrid
    '''
    # geojson_data, geojson_name = madrid_barrios(path=path)
    # geojson_data, geojson_name = madrid_barrios_historicos(path=path)
    geojson_data, geojson_name = madrid_barrios_historicos(path=path, year=1612, layer="cuarteles")
    # geojson_data, geojson_name = madrid_distritos(path=path)
    # geojson_data, geojson_name = madrid_distritos_historicos(path=path)

    geojson_data = fix_topology(geojson_data, snap_tolerance= 0.0001)

    simpl = 0.01

    geojson_simpl = simplify_geojson(
        geojson_data=geojson_data,
        simplification_tolerance=simpl,
        filepath=path,
        geojson_name=geojson_name,
        simplify_boundary=True
    )
