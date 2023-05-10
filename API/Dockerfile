FROM python:3.9-slim-bullseye

# Copia el código fuente de tu API en el contenedor
COPY . /app

# Establece el directorio de trabajo en el directorio de la aplicación
WORKDIR /app

# Instala las dependencias necesarias
RUN pip install --no-cache-dir -r requirements.txt

# Expone el puerto que utilizará tu API
EXPOSE 80

# Define el comando para ejecutar tu API
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]