# Tarea_1 sistemas distribuidos: 

Requisitos:
Docker
Docker Compose

Despliegue
1. Clonar el repositorio
   git clone <URL_DEL_REPOSITORIO>
cd <NOMBRE_DEL_REPOSITORIO>
3. Agregar el dataset
Coloca santiago.csv (Dataset utilizado para la tarea se debe renombrar) dentro de la carpeta
 Dataset T1/:
 mkdir -p "Dataset T1"
Al realizar el commit se borro tanto el dataset como la carpeta por que son muy pesados para la primera ejecucion es necesario sumar 1 carpeta llamada  "Dataset T1" y dentro de esta un archivo con el Dataset pero con el nombre "santiago.csv" quedando la arquitectura de la siguiente forma:
Dataset T1/santiago.csv
cp /ruta/a/santiago.csv "Dataset T1/santiago.csv"
5. Levantar los servicios
   docker-compose up --build
El sistema ejecuta automáticamente los siguientes pasos:

Precarga del dataset en memoria
Limpieza de caché y métricas anteriores
Generación de tráfico con distribución uniforme y Zipf (30 consultas c/u)
Resumen de métricas globales

