import json
import time
import threading
from collections import deque
import redis
import os

# CONFIG

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=1,
    decode_responses=True,
)
 
METRICAS_KEY   = "metricas:eventos"   # Lista Redis con todos los eventos
EVICTION_KEY   = "metricas:evictions" # Contador de evictions
_lock          = threading.Lock()
_inicio_sistema = time.time()
_latencias_buffer: deque = deque(maxlen=10_000)
 
 

# REGISTRO DE EVENTOS

def registrar_evento(tipo: str, cache_key: str, latencia_seg: float, extra: dict = None):
    """
    tipo: 'hit' | 'miss'
    Persiste el evento en Redis y lo agrega al buffer local.
    """
    evento = {
        "tipo":       tipo,
        "cache_key":  cache_key,
        "latencia":   round(latencia_seg, 6),
        "ts":         time.time(),
    }
    if extra:
        evento.update(extra)
 
    with _lock:
        _latencias_buffer.append(latencia_seg)
 
    try:
        redis_client.rpush(METRICAS_KEY, json.dumps(evento))
    except redis.RedisError:
        pass   # No interrumpir el sistema si métricas falla
 
 
def registrar_eviction():
    """Incrementa el contador de evictions (llamado externamente o via keyspace notifications)."""
    try:
        redis_client.incr(EVICTION_KEY)
    except redis.RedisError:
        pass
 
 

# CÁLCULO DE MÉTRICAS

def _leer_todos_eventos() -> list:
    try:
        raw = redis_client.lrange(METRICAS_KEY, 0, -1)
        return [json.loads(r) for r in raw]
    except redis.RedisError:
        return []
 
 
def calcular_metricas() -> dict:
    """
    Retorna un dict con:
      - hit_rate, miss_rate
      - throughput (consultas/seg desde inicio)
      - latencia_p50, latencia_p95
      - eviction_rate (evictions / minuto)
      - cache_efficiency
      - totales: hits, misses, total
    """
    eventos = _leer_todos_eventos()
 
    hits   = [e for e in eventos if e["tipo"] == "hit"]
    misses = [e for e in eventos if e["tipo"] == "miss"]
    total  = len(eventos)
 
    n_hits   = len(hits)
    n_misses = len(misses)
 
    hit_rate  = n_hits  / total if total else 0.0
    miss_rate = n_misses / total if total else 0.0
 
    # Throughput
    elapsed   = max(time.time() - _inicio_sistema, 1e-9)
    throughput = total / elapsed
 
    # Percentiles de latencia
    latencias = sorted([e["latencia"] for e in eventos]) if eventos else [0.0]
 
    def percentil(lst, p):
        if not lst:
            return 0.0
        idx = max(0, int(len(lst) * p / 100) - 1)
        return lst[idx]
 
    p50 = percentil(latencias, 50)
    p95 = percentil(latencias, 95)
 
    # Eviction rate (evictions / minuto)
    try:
        total_evictions = int(redis_client.get(EVICTION_KEY) or 0)
    except redis.RedisError:
        total_evictions = 0
    eviction_rate = total_evictions / (elapsed / 60) if elapsed else 0.0
 
    # Cache efficiency = hits*t_cache - misses*t_db / total
    # t_cache ≈ latencia promedio de hits, t_db ≈ latencia promedio de misses
    lat_hits   = [e["latencia"] for e in hits]
    lat_misses = [e["latencia"] for e in misses]
    t_cache = sum(lat_hits)   / len(lat_hits)   if lat_hits   else 0.0
    t_db    = sum(lat_misses) / len(lat_misses) if lat_misses else 0.0
    cache_efficiency = ((n_hits * t_cache) - (n_misses * t_db)) / total if total else 0.0
 
    return {
        "hits":              n_hits,
        "misses":            n_misses,
        "total":             total,
        "hit_rate":          round(hit_rate,  4),
        "miss_rate":         round(miss_rate, 4),
        "throughput_qps":    round(throughput, 4),
        "latencia_p50_seg":  round(p50, 6),
        "latencia_p95_seg":  round(p95, 6),
        "eviction_rate_rpm": round(eviction_rate, 4),
        "cache_efficiency":  round(cache_efficiency, 6),
        "t_avg_cache_seg":   round(t_cache, 6),
        "t_avg_db_seg":      round(t_db, 6),
    }
 
 
def imprimir_resumen():
    m = calcular_metricas()
    print("\n" + "="*50)
    print("       RESUMEN DE MÉTRICAS DEL SISTEMA")
    print("="*50)
    print(f"  Total consultas  : {m['total']}")
    print(f"  Hits             : {m['hits']}  ({m['hit_rate']*100:.1f}%)")
    print(f"  Misses           : {m['misses']}  ({m['miss_rate']*100:.1f}%)")
    print(f"  Throughput       : {m['throughput_qps']:.2f} consultas/seg")
    print(f"  Latencia p50     : {m['latencia_p50_seg']*1000:.2f} ms")
    print(f"  Latencia p95     : {m['latencia_p95_seg']*1000:.2f} ms")
    print(f"  Eviction rate    : {m['eviction_rate_rpm']:.2f} evictions/min")
    print(f"  Cache efficiency : {m['cache_efficiency']:.6f}")
    print("="*50 + "\n")
    return m
 
 
def limpiar_metricas():
    try:
        redis_client.delete(METRICAS_KEY)
        redis_client.delete(EVICTION_KEY)
    except redis.RedisError:
        pass