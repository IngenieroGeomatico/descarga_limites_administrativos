
**Resumen**
- **Proyecto**:
- Script para descargar y procesar límites administrativos (GeoJSON) desde varias fuentes: IGN/CNIG, INE, Eurostat o el ayuntamiento de Madrid.

- Se puede ampliar con otros límites administrativos de acceso libre y gratuíto.

**Requisitos**
- **Python**: 3.10+ (recomendado).
- **Dependencias**: ver [requirements.txt](requirements.txt).

**Instalación**
- Crear y activar un entorno virtual, e instalar dependencias:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**Uso**
- Edita [run.py](run.py) para descomentar la función que quieras ejecutar y ajustar `path` o parámetros (por ejemplo `year` o `layer`).
- Ejecuta el script:

```bash
python run.py
```

**Funciones principales**
- Localizadas en [lib/descarga_unidad_administrativa.py](lib/descarga_unidad_administrativa.py).
- Algunas funciones disponibles:
	- `IGN_pais()`, `IGN_comunidades_autonomas()`, `IGN_provincias()`, `IGN_municipios()`
	- `eurostat_*()` (países, NUTS, LAU, URAU, costas,...)
	- `madrid_barrios()`, `madrid_barrios_historicos()`

**Datos incluidos**
- Carpeta de salida/ejemplo: [geojson/](geojson/)

**Salida**
- Los resultados se guardan en la carpeta indicada por `path` (por defecto `./geojson/`).
- El script puede arreglar la topología original si lo necesitara.
- El script genera versiones simplificadas (sufijo `_simpl_0-01`) y el fichero de log `descarga_unidades_administrativas.log`.

**Registro (logging)**
- El proyecto escribe un log en `descarga_unidades_administrativas.log` en el directorio de trabajo.

**Contribuir / Contacto**
- Pull requests y issues bienvenidos.


