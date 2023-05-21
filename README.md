# Sistema de Recomendación con Preprocesamiento de Datos en Airflow y API de FastAPI
Este proyecto consiste en un sistema de recomendación que utiliza la plataforma Airflow para realizar el preprocesamiento de datos, almacenar los productos recomendados por cada anunciante en una base de datos RDS y proporcionar una API de FastAPI para acceder a estos datos mediante un cluster de ECS.

Funcionalidades principales
Preprocesamiento de datos utilizando Airflow: El sistema utiliza Airflow para definir y programar tareas de preprocesamiento de datos, lo que permite una ejecución automatizada y programada de los flujos de trabajo.
Almacenamiento en una base de datos RDS: Los productos recomendados por cada anunciante se almacenan en una base de datos RDS (Relational Database Service) de Amazon Web Services (AWS), lo que garantiza una alta disponibilidad y durabilidad de los datos.
API de FastAPI: Se proporciona una API de FastAPI para acceder a los productos recomendados desde la base de datos RDS. Esto permite que otras aplicaciones o servicios consuman los datos de manera eficiente y rápida.
Cluster de ECS: La API de FastAPI se despliega en un cluster de Amazon Elastic Container Service (ECS) para garantizar una alta disponibilidad, escalabilidad y tolerancia a fallos.
Arquitectura del sistema
El sistema consta de los siguientes componentes principales:

Airflow: Se utiliza para definir y programar los flujos de trabajo de preprocesamiento de datos. Airflow permite ejecutar tareas de forma automática y programada, lo que garantiza la integridad y consistencia de los datos procesados.

RDS (Relational Database Service): La base de datos RDS almacena los productos recomendados por cada anunciante. Se utiliza una base de datos relacional para garantizar una estructura de datos coherente y permitir consultas eficientes.

FastAPI: Se implementa una API de FastAPI para acceder a los datos de productos recomendados desde la base de datos RDS. FastAPI proporciona una interfaz rápida y eficiente para la comunicación entre las aplicaciones y el sistema de recomendación.

Cluster de ECS (Elastic Container Service): La API de FastAPI se despliega en un cluster de ECS para garantizar la escalabilidad y disponibilidad del sistema. ECS permite administrar y escalar los contenedores de la API de forma automática, según las necesidades de la carga de trabajo.
