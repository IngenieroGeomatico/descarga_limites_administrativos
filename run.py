from lib.descarga_unidad_administrativa import *


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
geojson_data, geojson_name = madrid_barrios(path=path)
# geojson_data, geojson_name = madrid_barrios_historicos(path=path)
# geojson_data, geojson_name = madrid_barrios_historicos(path=path, year=1612, layer="cuarteles")
# geojson_data, geojson_name = madrid_distritos(path=path)
# geojson_data, geojson_name = madrid_distritos_historicos(path=path, year=1612, layer="cuarteles")



"""Arreglo topológico"""
snap_tolerance= 0.0001
geojson_data = fix_topology(geojson_data, snap_tolerance=snap_tolerance)


"""Simplificación"""
simpl = 0.01
geojson_simpl = simplify_geojson(
    geojson_data=geojson_data,
    simplification_tolerance=simpl,
    filepath=path,
    geojson_name=geojson_name,
    simplify_boundary=True
)
