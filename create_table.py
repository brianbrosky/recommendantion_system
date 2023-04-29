import psycopg2
dbname = "database-1"
user = "postgres" #Configuracion / Disponibilidad / nombre de usuario maestro
password = "chavoLOCO23"
host = "database-1.c7mkdwca7kj0.us-east-1.rds.amazonaws.com" #Econectividad y seguridad
port = "5432"

#Creamos la conexión a RDS
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)

cur = conn.cursor()
cur.execute('CREATE TABLE tabla_RDS (adv_id string)')
conn.commit()

# Cerrar la conexión
cur.close()
conn.close()







