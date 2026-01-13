# -*- coding: utf-8 -*-
import io
import os
import sys
import time
import json
import zipfile
import logging
import topojson
import requests
import shapefile
from typing import Optional
from pyproj import CRS, Transformer


from shapely.geometry import shape, mapping, Polygon, MultiPolygon
from shapely.geometry.base import BaseGeometry
from pyproj import Geod





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
TIMEOUT = 20
DEFAULT_PAGE_SIZE = 20
SLEEP_BETWEEN_REQUESTS = 3.0


# =========================
# Lógica principal
# =========================
"""
URL referencia
https://ec.europa.eu/eurostat/web/gisco/geodata
"""

def IGN_pais(path: Optional[str] = None, pag: int = DEFAULT_PAGE_SIZE):
    gjson_name = "IGN_pais"
    gjson=descargar_nivel_administrativo("País", path, pag, gjson_name)
    logger.info("Proceso completado.")
    return gjson, gjson_name

def IGN_comunidades_autonomas(path: Optional[str] = None, pag: int = DEFAULT_PAGE_SIZE):
    gjson_name = "IGN_comunidades_autonomas"
    gjson = descargar_nivel_administrativo("Comunidad autónoma", path, pag, gjson_name)
    logger.info("Proceso completado.")
    return gjson, gjson_name

def IGN_provincias(path: Optional[str] = None, pag: int = DEFAULT_PAGE_SIZE):
    gjson_name = "IGN_provincias"
    gjson = descargar_nivel_administrativo("Provincia", path, pag, gjson_name)
    logger.info("Proceso completado.")
    return gjson, gjson_name

def IGN_municipios(path: Optional[str] = None, pag: int = DEFAULT_PAGE_SIZE):
    gjson_name = "IGN_municipios"
    gjson = descargar_nivel_administrativo("Municipio", path, pag, gjson_name)
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
                            codPostArray.append({"cod": cod, "name": nombre})

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
    session = requests.Session()
    headers = {
        "Origin": "https://www.ign.es",
        "Referer": "https://www.ign.es",
        "User-Agent": "Mozilla/5.0",
    }

    gjson= {"type": "FeatureCollection", "features": []}

    for i, codi in enumerate(codPostArray, start=1):
        codigo = codi["cod"].lstrip("0")  # quitar ceros a la izquierda
        # OJO: usa '&' en la URL real, no '&amp;'
        url = (
            "https://www.cartociudad.es/geocoder/api/geocoder/find"
            f"?q={codigo}&type=Codpost&id={codigo}&outputformat=gjson"
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

def INE_secciones_censales(path: Optional[str]):
    """
    Descarga el shapefile. de secciones censales (INE) dentro de un ZIP y lo convierte a gjson.
    - Lee .shp/.shx/.dbf directamente desde el ZIP (sin extraer a disco).
    - Reproyecta a EPSG:4326 si hay .prj.
    - Devuelve un dict gjson (FeatureCollection). Si `path` no es None, guarda el gjson en esa ruta.
    """
    URLzipsec_cens = "https://www.ine.es/prodyser/cartografia/seccionado_2025.zip"
    to_epsg = 4326  # Cambia a None si quieres mantener el CRS original

    gjson_name = "INE_secciones_censales"

    logger.info("Descargando ZIP de secciones censales: %s", URLzipsec_cens)

    try:
        resp = requests.get(URLzipsec_cens, timeout=30)
        resp.raise_for_status()
    except requests.exceptions.Timeout:
        logger.error("Timeout al descargar el ZIP (30s).")
        return []
    except requests.exceptions.RequestException as e:
        logger.error("Error al descargar el ZIP: %s", e)
        return []

    try:
        zip_bytes = io.BytesIO(resp.content)
        zip_bytes.seek(0)
        with zipfile.ZipFile(zip_bytes) as zf:
            names = zf.namelist()
            logger.debug("Contenido del ZIP: %s", names)

            # Localizar componentes del shapefile.
            shp_name = shx_name = dbf_name = prj_name = None
            for name in names:
                low = name.lower()
                if low.endswith(".shp"): shp_name = name
                elif low.endswith(".shx"): shx_name = name
                elif low.endswith(".dbf"): dbf_name = name
                elif low.endswith(".prj"): prj_name = name

            if not (shp_name and shx_name and dbf_name):
                logger.error("El ZIP no contiene un shapefile. completo (.shp/.shx/.dbf).")
                return []

            # Abrir streams directamente del ZIP
            shp_f = zf.open(shp_name, "r")
            shx_f = zf.open(shx_name, "r")
            dbf_f = zf.open(dbf_name, "r")

            # Leer CRS desde .prj (con fallback de encoding)
            src_crs = None
            if prj_name:
                with zf.open(prj_name, "r") as prj_f:
                    raw = prj_f.read()
                    try:
                        wkt = raw.decode("utf-8")
                    except UnicodeDecodeError:
                        wkt = raw.decode("latin-1", errors="ignore")
                try:
                    src_crs = CRS.from_wkt(wkt)
                    logger.debug("CRS origen detectado: %s", src_crs.to_string())
                except Exception as crs_err:
                    logger.warning("No se pudo interpretar el .prj: %s", crs_err)
                    src_crs = None

            # Preparar reproyección
            transformer = None
            if to_epsg is not None and src_crs is not None:
                try:
                    dst_crs = CRS.from_epsg(to_epsg)
                    transformer = Transformer.from_crs(src_crs, dst_crs, always_xy=True)
                except Exception as tr_err:
                    logger.warning("No se pudo crear el transformador: %s", tr_err)
                    transformer = None

            # Reader con encoding Latin-1 para DBF
            reader = shapefile.Reader(shp=shp_f, shx=shx_f, dbf=dbf_f, encoding="latin-1")
            fields = reader.fields[1:]
            field_names = [f[0] for f in fields]

            def project_point(x, y):
                return transformer.transform(x, y) if transformer else (x, y)

            def project_coords(coords):
                return [project_point(x, y) for x, y in coords]

            def shape_to_geom(shape):
                t = shape.shapeType
                if t in [shapefile.POINT, shapefile.POINTZ, shapefile.POINTM]:
                    x, y = shape.points[0]
                    x, y = project_point(x, y)
                    return {"type": "Point", "coordinates": [x, y]}
                elif t in [shapefile.MULTIPOINT, shapefile.MULTIPOINTZ, shapefile.MULTIPOINTM]:
                    return {"type": "MultiPoint", "coordinates": project_coords(shape.points)}
                elif t in [shapefile.POLYLINE, shapefile.POLYLINEZ, shapefile.POLYLINEM]:
                    parts = list(shape.parts) + [len(shape.points)]
                    lines = []
                    for i in range(len(parts) - 1):
                        seg = shape.points[parts[i]:parts[i+1]]
                        lines.append(project_coords(seg))
                    return {"type": "LineString", "coordinates": lines[0]} if len(lines) == 1 else {"type":"MultiLineString","coordinates":lines}
                elif t in [shapefile.POLYGON, shapefile.POLYGONZ, shapefile.POLYGONM]:
                    parts = list(shape.parts) + [len(shape.points)]
                    rings = []
                    for i in range(len(parts) - 1):
                        seg = shape.points[parts[i]:parts[i+1]]
                        rings.append(project_coords(seg))
                    return {"type": "Polygon", "coordinates": [rings[0]]} if len(rings) == 1 else {"type":"Polygon","coordinates":rings}
                else:
                    return shape.__geo_interface__

            features = []
            for sr in reader.iterShapeRecords():
                props = {field_names[i]: sr.record[i] for i in range(len(field_names))}
                geom = shape_to_geom(sr.shape)
                features.append({"type": "Feature", "geometry": geom, "properties": props})

            # Cerrar manejadores
            reader.close()
            shp_f.close(); shx_f.close(); dbf_f.close()

            gjson = {"type": "FeatureCollection", "features": features}

            save_geojson(gjson, path, gjson_name)
            logger.info("Proceso completado.")

            return gjson, gjson_name

    except zipfile.BadZipFile:
        logger.error("El archivo descargado no es un ZIP válido.")
        return []
    except Exception as e:
        logger.exception("Error procesando el ZIP: %s", e)
        return []

def eurostat_countries(path: Optional[str], scale: Optional[str] = "60M"):
    gjson_name = "eurostat_countries"
    scales = ["60M", "20M", "10M", "03M", "01M"]
    if not scale in scales:
        raise ValueError(f"scale {scale} no está permitido. Los valores permitidos son: {scales}")

    url = f"https://gisco-services.ec.europa.eu/distribution/v2/countries/gjson/CNTR_RG_{scale}_2024_4326.gjson"

    try:
        resp = requests.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        gjson = resp.json()
        logger.info("Total de objetos encontrados: %d", len(gjson["features"]))

    except requests.Timeout:
        logger.warning("Timeout al consultar número total de objetos")
    except requests.RequestException as e:
        logger.error("Error HTTP al obtener conteo: %s", e)
    except Exception as e:
        logger.exception("Error inesperado al obtener conteo: %s", e)
    
    save_geojson(gjson, path, gjson_name)
    logger.info("Proceso completado.")
    return gjson, gjson_name

# =========================
# Funciones transversales
# =========================
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


def descargar_nivel_administrativo(
    nivel: str, 
    path: Optional[str] = None,
    pag: int = DEFAULT_PAGE_SIZE,
    name: Optional[str] = None,
) -> None:
    """
    Descarga los features de un nivel administrativo y los guarda en disco
    """

    logger.info("Iniciando descarga: %s - IGN API-Features", nivel)

    session = requests.Session()

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


def simplify_geojson_(geojson_data, simplification_distance, filepath, geojson_name):
    try:
        # 1️⃣ Convertir GeoJSON a TopoJSON
        logger.info("Convirtiendo GeoJSON a TopoJSON...")
        topology = topojson.Topology(geojson_data, prequantize=1e6)  # prequantize mejora la compresión

        # 2️⃣ Simplificar topológicamente
        logger.info(f"Simplificando con tolerancia {simplification_distance}...")
        topology = topology.toposimplify(simplification_distance)

        # 3️⃣ Convertir de nuevo a GeoJSON
        simplified_geojson = json.loads(topology.to_geojson())
        
        # 4️⃣ Guardar el resultado
        geojson_name = f"{geojson_name}_simpl"
        save_geojson(simplified_geojson, filepath, geojson_name)

        logger.info("Proceso completado con TopoJSON (simplificación topológica).")
        return simplified_geojson

    except Exception as e:
        logger.error("Error al simplificar el GeoJSON con TopoJSON: %s", e)
        raise ValueError(e)


def simplify_geojson(geojson_data, simplification_distance, filepath, geojson_name,
                     min_area_m2=None, keep_largest_if_all_removed=True):
    """
    min_area_m2: si se especifica, elimina minipolígonos con área < min_area_m2 (en m²).
    """

    _GEOD = Geod(ellps="WGS84")

    def _ring_area_m2(coords):
        """
        Área geodésica de un anillo (lista de [lon, lat]) en m².
        Devuelve valor positivo para el exterior si está CCW, negativo si CW (convención geod).
        """
        lons, lats = zip(*coords)
        area, _ = _GEOD.polygon_area_perimeter(lons, lats)
        return abs(area)  # nos interesa área absoluta

    def polygon_area_geodesic_m2(poly: Polygon) -> float:
        """
        Área de un shapely Polygon en m² sobre WGS84, teniendo en cuenta agujeros.
        """
        # Exterior
        area_ext = _ring_area_m2(list(poly.exterior.coords))
        # Agujeros
        holes_area = sum(_ring_area_m2(list(ring.coords)) for ring in poly.interiors)
        return max(0.0, area_ext - holes_area)

    def filter_small_polygons(geom: BaseGeometry, min_area_m2: float,
                            keep_largest_if_all_removed: bool = True) -> BaseGeometry | None:
        """
        Elimina componentes (polígonos) con área < min_area_m2 dentro de Polygon/MultiPolygon.
        - Si es Polygon y es < umbral → devuelve None (o el mismo si keep_largest_if_all_removed=True y no hay nada más).
        - Si es MultiPolygon, devuelve un MultiPolygon con solo los polígonos ≥ umbral.
        - Si todos son eliminados y keep_largest_if_all_removed=True → conserva el mayor.
        - Si tras filtrar queda 1 polígono → devuelve Polygon (no Multi).
        - Si queda vacío → None.
        """
        if geom.is_empty:
            return None

        if isinstance(geom, Polygon):
            area = polygon_area_geodesic_m2(geom)
            if area >= min_area_m2:
                return geom
            # demasiado pequeño
            return geom if keep_largest_if_all_removed else None

        if isinstance(geom, MultiPolygon):
            polys = list(geom.geoms)
            areas = [polygon_area_geodesic_m2(p) for p in polys]
            kept = [p for p, a in zip(polys, areas) if a >= min_area_m2]

            if kept:
                if len(kept) == 1:
                    return kept[0]
                return MultiPolygon(kept)

            # Nada cumple el umbral
            if keep_largest_if_all_removed and len(polys) > 0:
                # Mantener el mayor (evita vaciar la entidad por completo)
                idx = max(range(len(polys)), key=lambda i: areas[i])
                return polys[idx]

            return None

        # Si no es Polygon/MultiPolygon, devolvemos tal cual (o None si no quieres puntos/líneas)
        return geom

    try:
        # 1️⃣ Convertir a TopoJSON
        logger.info("Convirtiendo GeoJSON a TopoJSON...")
        topology = topojson.Topology(geojson_data, prequantize=1e6)

        # 2️⃣ Simplificar topológicamente
        logger.info(f"Simplificando con tolerancia {simplification_distance}...")
        topology = topology.toposimplify(simplification_distance)

        # 3️⃣ Volver a GeoJSON
        simplified_geojson = json.loads(topology.to_geojson())

        # 3b️⃣ (Opcional) Filtrar minipolígonos por área
        if min_area_m2 is not None:
            logger.info(f"Filtrando polígonos con área < {min_area_m2:.2f} m²...")
            filtered_features = []
            drop_count = 0

            for feat in simplified_geojson.get("features", []):
                geom = feat.get("geometry")
                if not geom:
                    continue

                # Solo tratamos Polygon/MultiPolygon. Otros tipos pasan tal cual.
                if geom["type"] in ("Polygon", "MultiPolygon"):
                    shp = shape(geom)
                    shp_filtered = filter_small_polygons(
                        shp, min_area_m2=min_area_m2,
                        keep_largest_if_all_removed=keep_largest_if_all_removed
                    )
                    if shp_filtered is None or shp_filtered.is_empty:
                        drop_count += 1
                        continue
                    # Actualizamos geometría filtrada
                    feat = {**feat, "geometry": mapping(shp_filtered)}
                    filtered_features.append(feat)
                else:
                    filtered_features.append(feat)

            simplified_geojson["features"] = filtered_features
            logger.info(f"Filtrado completado. Features descartadas: {drop_count}")

        # 4️⃣ Guardar
        out_name = f"{geojson_name}_simpl"
        save_geojson(simplified_geojson, filepath, out_name)

        logger.info("Proceso completado con TopoJSON + filtrado por área.")
        return simplified_geojson

    except Exception as e:
        logger.error("Error al simplificar el GeoJSON con TopoJSON: %s", e)
        raise ValueError(e)


# =========================
# Main
# =========================
if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)  # "DEBUG" "INFO", "WARNING", "ERROR"
    # codigos_postales(path="./codigos_postales/", descarga_ID_json=False)
    path=""
    # geojson_data, geojson_name = eurostat_countries(path=path)
    geojson_data, geojson_name = IGN_comunidades_autonomas(path=path)

    simpl = 0.1
    min_area_m2 = simpl * 10000
    geojson_simpl = simplify_geojson(geojson_data, simpl, path, geojson_name,min_area_m2)
