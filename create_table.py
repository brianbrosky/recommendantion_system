import psycopg2
dbname = "database"
user = "modelos" #Configuracion / Disponibilidad / nombre de usuario maestro
password = "Chavoloco23"
host = "database.crv8bjyoa2v8.us-east-1.rds.amazonaws.com" #Econectividad y seguridad
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
cur.execute('create table top_20_ctr (adv_id string, product_id string, cantidad int, fecha_recom string)')
cur.execute('create table top_20 (adv_id string, product_id string, click int, impression int, click-through-rate float, fecha_recom string)')

conn.commit()

# Cerrar la conexión
cur.close()
conn.close()







