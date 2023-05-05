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
#cur.execute('create table top_20_ctr (adv_id varchar(50), product_id varchar(50), cantidad int, fecha_recom timestamp)')
#cur.execute('create table top_20 (adv_id varchar(50), product_id varchar(50), click int, impression int, clickthroughrate float(3), fecha_recom timestamp)')
#cur.execute('alter table top_20 alter column fecha_recom type timestamp without time zone using fecha_recom::timestamp without time zone')
#cur.execute('alter table top_20 alter column fecha_recom type timestamp without time zone using fecha_recom::timestamp without time zone')
cur.execute('select *, data_type from information_schema.columns where table_name = "top_20_ctr"')



conn.commit()

# Cerrar la conexión
cur.close()
conn.close()
