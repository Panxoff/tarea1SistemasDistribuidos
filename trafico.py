import random
import time
import numpy as np
from cache import procesar_consulta
 

# ZONAS PREDEFINIDAS

ZONAS = [
    {
        "comuna":  "Providencia",
        "zona_id": "Z1",
        "lat_min": -33.445, "lat_max": -33.420,
        "lon_min": -70.640, "lon_max": -70.600,
    },
    {
        "comuna":  "Las Condes",
        "zona_id": "Z2",
        "lat_min": -33.420, "lat_max": -33.390,
        "lon_min": -70.600, "lon_max": -70.550,
    },
    {
        "comuna":  "Maipu",
        "zona_id": "Z3",
        "lat_min": -33.530, "lat_max": -33.490,
        "lon_min": -70.790, "lon_max": -70.740,
    },
    {
        "comuna":  "Santiago Centro",
        "zona_id": "Z4",
        "lat_min": -33.460, "lat_max": -33.430,
        "lon_min": -70.670, "lon_max": -70.630,
    },
    {
        "comuna":  "Pudahuel",
        "zona_id": "Z5",
        "lat_min": -33.470, "lat_max": -33.430,
        "lon_min": -70.810, "lon_max": -70.760,
    },
]
 
TIPOS_CONSULTA   = ["Q1", "Q2", "Q3", "Q4", "Q5"]
CONFIDENCE_VALUES = [0.0, 0.3, 0.5, 0.7, 0.9]
BINS_VALUES      = [3, 5, 10]
 

# AUXILIARES

def _construir_bbox(zona: dict) -> tuple:
    return (zona["lat_min"], zona["lat_max"], zona["lon_min"], zona["lon_max"])
 
 
def generar_consulta() -> dict:
    """Genera una consulta sintética aleatoria (Q1–Q5)."""
    tipo = random.choice(TIPOS_CONSULTA)
 
    if tipo == "Q4":
        zona_a, zona_b = random.sample(ZONAS, 2)
        return {
            "tipo":           "Q4",
            "zone_a":         zona_a["comuna"],
            "zone_b":         zona_b["comuna"],
            "bbox_a":         _construir_bbox(zona_a),
            "bbox_b":         _construir_bbox(zona_b),
            "confidence_min": random.choice(CONFIDENCE_VALUES),
        }
 
    zona = random.choice(ZONAS)
    base = {
        "tipo":    tipo,
        "zone_id": zona["comuna"],
        "bbox":    _construir_bbox(zona),
    }
 
    if tipo == "Q5":
        base["bins"] = random.choice(BINS_VALUES)
    else:
        base["confidence_min"] = random.choice(CONFIDENCE_VALUES)
 
    return base
 

# DISTRIBUCIONES DE TIEMPO

def tiempo_arribo_uniforme(min_t: float = 0.2, max_t: float = 1.0) -> float:
    """Distribución uniforme entre min_t y max_t segundos."""
    return random.uniform(min_t, max_t)
 
 
def tiempo_arribo_zipf(base: float = 0.1, alpha: float = 2.0, max_t: float = 3.0) -> float:
    """
    Distribución Zipf (ley de potencia).
    Valores bajos de alpha → cola más pesada → más variabilidad.
    """
    rank   = np.random.zipf(alpha)
    espera = base * rank
    return min(espera, max_t)
 

# GENERADOR DE TRÁFICO

def ejecutar_trafico(
    n_consultas: int   = 20,
    distribucion: str  = "uniforme",
    usar_sleep: bool   = False,
    verbose: bool      = True,
) -> dict:
    """
    Genera y procesa n_consultas usando el sistema de caché.
 
    Parámetros
    ----------
    n_consultas  : número total de consultas a emitir.
    distribucion : 'uniforme' | 'zipf'
    usar_sleep   : si True, espera el tiempo de arribo entre consultas
                   (simula tráfico real); si False, las envía lo más
                   rápido posible (benchmark de throughput).
    verbose      : imprime detalle de cada consulta.
 
    Retorna
    -------
    dict con hits, misses, throughput y distribución usada.
    """
    if distribucion not in ("uniforme", "zipf"):
        raise ValueError("distribucion debe ser 'uniforme' o 'zipf'")
 
    hits   = 0
    misses = 0
    t_inicio_total = time.time()
 
    for i in range(n_consultas):
        consulta = generar_consulta()
 
        if distribucion == "uniforme":
            espera = tiempo_arribo_uniforme()
        else:
            espera = tiempo_arribo_zipf()
 
        respuesta = procesar_consulta(consulta)
 
        if respuesta["status"] == "hit":
            hits += 1
        else:
            misses += 1
 
        if verbose:
            print(f"\n── Consulta {i + 1}/{n_consultas} ──────────────────")
            print(f"  Distribución : {distribucion}")
            print(f"  Tipo         : {consulta['tipo']}")
            print(f"  Cache status : {respuesta['status'].upper()}")
            print(f"  Cache key    : {respuesta['cache_key']}")
            print(f"  Resultado    : {respuesta['result']}")
            print(f"  Espera sim.  : {round(espera, 3)} seg")
 
        if usar_sleep:
            time.sleep(espera)
 
    duracion   = max(time.time() - t_inicio_total, 1e-9)
    throughput = n_consultas / duracion
    hit_rate   = hits / n_consultas if n_consultas else 0.0
 
    resumen = {
        "distribucion": distribucion,
        "n_consultas":  n_consultas,
        "hits":         hits,
        "misses":       misses,
        "hit_rate":     round(hit_rate, 4),
        "throughput_qps": round(throughput, 4),
        "duracion_seg": round(duracion, 3),
    }
 
    print(f"\n{'='*50}")
    print(f"  RESUMEN TRÁFICO [{distribucion.upper()}]")
    print(f"  Consultas  : {n_consultas}")
    print(f"  Hits       : {hits} ({hit_rate*100:.1f}%)")
    print(f"  Misses     : {misses}")
    print(f"  Throughput : {throughput:.2f} q/s")
    print(f"  Duración   : {duracion:.2f} seg")
    print(f"{'='*50}\n")
 
    return resumen