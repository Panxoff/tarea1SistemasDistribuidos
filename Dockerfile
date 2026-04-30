FROM python:3.11-slim
 
WORKDIR /app
 
# Dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*
 
# Dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
 
# Código fuente
COPY *.py .
 
CMD ["python", "main.py", "--n", "30", "--limpiar"]
 