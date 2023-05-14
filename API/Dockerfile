FROM python:3.9-slim-bullseye

# Definimos working directory
WORKDIR /app

# Copiamos txt que contiene los paquetes necesarios
COPY ./requirements.txt .

# Instalamos los paquetes necesarios
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el código al container
COPY ./app .

# Exponemos el puerto
EXPOSE 8000

# Levantamos FastAPI usando uvicorn
CMD uvicorn main:app --host 0.0.0.0 --port 8000
