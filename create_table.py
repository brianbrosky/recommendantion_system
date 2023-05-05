import psycopg2
dbname = "recomendaciones"
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
#cur.execute('create table top_20_ctr (adv_id varchar(50), product_id varchar(50), cantidad int, fecha_recom varchar(50))')
#cur.execute('create table top_20 (adv_id varchar(50), product_id varchar(50), click int, impression int, clickthroughrate float(3), fecha_recom varchar(50))')
cur.execute('alter table top_20 alter column fecha_recom type timestamp without time zone using fecha_recom::timestamp without time zone')


conn.commit()

# Cerrar la conexión
cur.close()
conn.close()







