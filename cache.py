import os
import json
import time
import redis
from respuestas import resolver_consulta
from metricas import registrar_evento
 
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
    decode_responses=True,
)

DEFAULT_TTL = 300   # segundos; ajustar por tipo de consulta si se desea
 
 

# CONSTRUCCIÓN DE CACHE KEY

def construir_cache_key(consulta: dict) -> str:
    tipo = consulta["tipo"]
 
    if tipo == "Q1":
        return f"count:{consulta['zone_id']}:conf={consulta.get('confidence_min', 0.0)}"
    if tipo == "Q2":
        return f"area:{consulta['zone_id']}:conf={consulta.get('confidence_min', 0.0)}"
    if tipo == "Q3":
        return f"density:{consulta['zone_id']}:conf={consulta.get('confidence_min', 0.0)}"
    if tipo == "Q4":
        return (
            f"compare:density:{consulta['zone_a']}:{consulta['zone_b']}"
            f":conf={consulta.get('confidence_min', 0.0)}"
        )
    if tipo == "Q5":
        return f"confidence_dist:{consulta['zone_id']}:bins={consulta.get('bins', 5)}"
 
    raise ValueError(f"Tipo de consulta no soportado: {tipo}")
 
 
 # REDIS

def _obtener_desde_cache(cache_key: str):
    valor = redis_client.get(cache_key)
    if valor is None:
        return None
    return json.loads(valor)
 
 
def _guardar_en_cache(cache_key: str, resultado, ttl: int = DEFAULT_TTL):
    redis_client.setex(cache_key, ttl, json.dumps(resultado))
 
 

# ENTRADA 

def procesar_consulta(consulta: dict, ttl: int = DEFAULT_TTL) -> dict:
    """
    Intercepta la consulta:
      1. Genera la cache key.
      2. Si hay HIT → retorna resultado de Redis y registra métrica.
      3. Si hay MISS → delega a resolver_consulta(), guarda en caché
         y registra métrica.
 
    Retorna un dict con: status, cache_key, result.
    """
    cache_key = construir_cache_key(consulta)
 
    t_inicio = time.time()
    resultado_cache = _obtener_desde_cache(cache_key)
    latencia = time.time() - t_inicio
 
    if resultado_cache is not None:
        # ── HIT ──
        registrar_evento("hit", cache_key, latencia)
        return {
            "status":    "hit",
            "cache_key": cache_key,
            "result":    resultado_cache,
        }
 
    # ── MISS: delegar al Generador de Respuestas ──
    t_inicio = time.time()
    resultado = resolver_consulta(consulta)
    latencia  = time.time() - t_inicio
 
    _guardar_en_cache(cache_key, resultado, ttl=ttl)
    registrar_evento("miss", cache_key, latencia)
 
    return {
        "status":    "miss",
        "cache_key": cache_key,
        "result":    resultado,
    }
 
 

# UTILIDADES

def limpiar_cache():
    redis_client.flushdb()
 
 
def info_cache() -> dict:
    try:
        info = redis_client.info("memory")
        stats = redis_client.info("stats")
        return {
            "used_memory_human":      info.get("used_memory_human"),
            "maxmemory_human":        info.get("maxmemory_human"),
            "maxmemory_policy":       info.get("maxmemory_policy"),
            "keyspace_hits":          stats.get("keyspace_hits"),
            "keyspace_misses":        stats.get("keyspace_misses"),
            "evicted_keys":           stats.get("evicted_keys"),
            "total_commands_processed": stats.get("total_commands_processed"),
        }
    except redis.RedisError as e:
        return {"error": str(e)}