import sys
from respuestas import precargar_datos
from cache      import limpiar_cache, info_cache
from metricas   import limpiar_metricas, imprimir_resumen
from trafico    import ejecutar_trafico
 
 
def main():
    print("Iniciando sistema...\n")
 
    # 1. Precargar datos
    print("[1/4] Precargando dataset...")
    try:
        precargar_datos()
    except FileNotFoundError:
        print("Error: no se encontro el dataset en 'Dataset T1/santiago.csv'")
        sys.exit(1)
 
    # 2. Limpiar estado anterior
    print("[2/4] Limpiando cache y metricas anteriores...")
    limpiar_cache()
    limpiar_metricas()
 
    # 3. Ejecutar trafico
    print("[3/4] Generando trafico...\n")
    ejecutar_trafico(n_consultas=30, distribucion="uniforme")
    ejecutar_trafico(n_consultas=30, distribucion="zipf")
 
    # 4. Mostrar metricas
    print("[4/4] Metricas del sistema:")
    imprimir_resumen()
 
 
if __name__ == "__main__":
    main()
 