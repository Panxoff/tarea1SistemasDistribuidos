import math
import time
import pandas as pd
import numpy as np
 
DATASET_PATH = "Dataset T1/santiago.csv"
 
ZONAS = {
    "Providencia": {
        "zona_id": "Z1",
        "lat_min": -33.445,
        "lat_max": -33.420,
        "lon_min": -70.640,
        "lon_max": -70.600,
    },
    "Las Condes": {
        "zona_id": "Z2",
        "lat_min": -33.420,
        "lat_max": -33.390,
        "lon_min": -70.600,
        "lon_max": -70.550,
    },
    "Maipu": {
        "zona_id": "Z3",
        "lat_min": -33.530,
        "lat_max": -33.490,
        "lon_min": -70.790,
        "lon_max": -70.740,
    },
    "Santiago Centro": {
        "zona_id": "Z4",
        "lat_min": -33.460,
        "lat_max": -33.430,
        "lon_min": -70.670,
        "lon_max": -70.630,
    },
    "Pudahuel": {
        "zona_id": "Z5",
        "lat_min": -33.470,
        "lat_max": -33.430,
        "lon_min": -70.810,
        "lon_max": -70.760,
    },
}
 
 #Configuraciones para inciar el sistema

PROCESSING_DELAY_SECONDS = 0.05
data: dict          = {}
zone_area_km2: dict = {}
_datos_cargados     = False
 
 #fun ciones para la consultas

def _calcular_area_bbox_km2(zona: dict) -> float:
    lat_centro = (zona["lat_min"] + zona["lat_max"]) / 2.0
    alto_km    = abs(zona["lat_max"] - zona["lat_min"]) * 111.32
    ancho_km   = abs(zona["lon_max"] - zona["lon_min"]) * 111.32 * math.cos(math.radians(lat_centro))
    return alto_km * ancho_km
 
 
def _filtrar_zona(df: pd.DataFrame, zona: dict) -> pd.DataFrame:
    return df[
        (df["latitude"]  >= zona["lat_min"]) &
        (df["latitude"]  <= zona["lat_max"]) &
        (df["longitude"] >= zona["lon_min"]) &
        (df["longitude"] <= zona["lon_max"])
    ].copy()
 
 
def _simular_procesamiento():
    """Simula la latencia real de una consulta geoespacial."""
    time.sleep(PROCESSING_DELAY_SECONDS)
 
 

# PRECARGA DATASET EN MEMORIA

def precargar_datos():
    """
    Carga el dataset desde disco y particiona por zona en memoria.
    Debe llamarse UNA sola vez al iniciar el sistema.
    """
    global data, zone_area_km2, _datos_cargados
 
    if _datos_cargados:
        return
 
    df = pd.read_csv(DATASET_PATH)
 
    columnas_requeridas = {"latitude", "longitude", "area_in_meters", "confidence"}
    faltantes = columnas_requeridas - set(df.columns)
    if faltantes:
        raise ValueError(f"Faltan columnas en el dataset: {faltantes}")
 
    for nombre_zona, zona in ZONAS.items():
        df_zona = _filtrar_zona(df, zona)
        data[nombre_zona]          = df_zona.reset_index(drop=True)
        zone_area_km2[nombre_zona] = _calcular_area_bbox_km2(zona)
 
    _datos_cargados = True
    total = sum(len(v) for v in data.values())
    print(f"[respuestas] Dataset cargado: {total} registros en {len(data)} zonas.")
 
 
# =========================================================
# FUNCIONES RAW (sin delay) — usadas internamente
# =========================================================
def _q1_count_raw(zone_id: str, confidence_min: float = 0.0) -> int:
    records = data[zone_id]
    return int((records["confidence"] >= confidence_min).sum())
 
 
def _q3_density_raw(zone_id: str, confidence_min: float = 0.0) -> float:
    count    = _q1_count_raw(zone_id, confidence_min)
    area_km2 = zone_area_km2[zone_id]
    return float(count / area_km2) if area_km2 else 0.0
 
 
# =========================================================
# Q1 — Conteo de edificios
# =========================================================
def q1_count(zone_id: str, confidence_min: float = 0.0) -> int:
    _simular_procesamiento()
    return _q1_count_raw(zone_id, confidence_min)
 
 
# =========================================================
# Q2 — Área promedio y total
# =========================================================
def q2_area(zone_id: str, confidence_min: float = 0.0) -> dict:
    _simular_procesamiento()
 
    records  = data[zone_id]
    filtrado = records[records["confidence"] >= confidence_min]
 
    if filtrado.empty:
        return {"avg_area": 0.0, "total_area": 0.0, "n": 0}
 
    areas = filtrado["area_in_meters"]
    return {
        "avg_area":   float(areas.mean()),
        "total_area": float(areas.sum()),
        "n":          int(len(areas)),
    }
 
 
# =========================================================
# Q3 — Densidad de edificaciones por km²
# =========================================================
def q3_density(zone_id: str, confidence_min: float = 0.0) -> float:
    _simular_procesamiento()
    # Usa la versión raw para no acumular delays adicionales
    return _q3_density_raw(zone_id, confidence_min)
 
 

# Q4 — Comparación de densidad entre dos zonas

def q4_compare(zone_a: str, zone_b: str, confidence_min: float = 0.0) -> dict:
    _simular_procesamiento()   # Un único delay para toda la operación Q4
 
    da = _q3_density_raw(zone_a, confidence_min)
    db = _q3_density_raw(zone_b, confidence_min)
 
    if da > db:
        winner = zone_a
    elif db > da:
        winner = zone_b
    else:
        winner = "empate"
 
    return {
        "zone_a":    zone_a,
        "density_a": da,
        "zone_b":    zone_b,
        "density_b": db,
        "winner":    winner,
    }
 
 
# Q5 — Distribución de confianza

def q5_confidence_dist(zone_id: str, bins: int = 5) -> list:
    _simular_procesamiento()
 
    records = data[zone_id]
    scores  = records["confidence"].dropna().to_numpy()
 
    if len(scores) == 0:
        return []
 
    counts, edges = np.histogram(scores, bins=bins, range=(0, 1))
 
    return [
        {
            "bucket": int(i),
            "min":    float(edges[i]),
            "max":    float(edges[i + 1]),
            "count":  int(counts[i]),
        }
        for i in range(bins)
    ]
 
 
#Respuestas centrales

def resolver_consulta(consulta: dict):
    tipo = consulta["tipo"]
 
    if tipo == "Q1":
        return q1_count(
            zone_id=consulta["zone_id"],
            confidence_min=consulta.get("confidence_min", 0.0),
        )
    if tipo == "Q2":
        return q2_area(
            zone_id=consulta["zone_id"],
            confidence_min=consulta.get("confidence_min", 0.0),
        )
    if tipo == "Q3":
        return q3_density(
            zone_id=consulta["zone_id"],
            confidence_min=consulta.get("confidence_min", 0.0),
        )
    if tipo == "Q4":
        return q4_compare(
            zone_a=consulta["zone_a"],
            zone_b=consulta["zone_b"],
            confidence_min=consulta.get("confidence_min", 0.0),
        )
    if tipo == "Q5":
        return q5_confidence_dist(
            zone_id=consulta["zone_id"],
            bins=consulta.get("bins", 5),
        )
 
    raise ValueError(f"Tipo de consulta no soportado: {tipo}")