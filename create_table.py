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

#CREACION
cur.execute('create table top_20 (adv_id varchar(50), product_id varchar(50), cantidad int, fecha_recom varchar(50))')
conn.commit()
cur.execute('create table top_20_ctr (adv_id varchar(50), product_id varchar(50), click int, impression int, clickthroughrate float(3), fecha_recom varchar(50))')
conn.commit()


#MODIFICAR TIPO DE DATO
# cur.execute('alter table top_20 alter column fecha_recom type varchar(50)')
# cur.execute('alter table top_20_ctr alter column fecha_recom type varchar(50)')


#CONSULTAR EL TIPO DE DATO DE CADA CAMPO
# cur.execute("select column_name, data_type from information_schema.columns where table_name = 'top_20'")
# for column_name, data_type in cur.fetchall():
#     print(column_name, data_type)


#BORRAR REGISTROS
# cur.execute('delete from top_20_ctr')
# cur.execute("DELETE FROM top_20_ctr WHERE fecha_recom <='2023-05-13'") 
# conn.commit()
# cur.execute('delete from top_20')
# cur.execute("DELETE FROM top_20 WHERE fecha_recom <='2023-05-13'") 
# conn.commit()


#SELECT PARA VER REGISTROS
# cur.execute('select * from top_20') 
# rows = cur.fetchall()
# for row in rows:
#     print(row)
# conn.commit()
# cur.execute('select * from top_20_ctr') 
# rows = cur.fetchall()
# for row in rows:
#     print(row)
# conn.commit()

#SELECT PARA GUARDAR CSV EN S3
# cur.execute('select * from top_20') 
# rows = cur.fetchall()
# # Generar archivo CSV y escribir los registros
# with open('registros.csv', 'w', newline='') as csvfile:
#     writer = csv.writer(csvfile)
#     writer.writerows(rows)
# # Cargar el archivo CSV en el bucket de S3
# s3.upload_file('registros.csv', bucket_name, 'registros.csv')

# cur.execute('select * from top_20_ctr') 
# rows = cur.fetchall()
# # Generar archivo CSV y escribir los registros
# with open('registros_ctr.csv', 'w', newline='') as csvfile:
#     writer = csv.writer(csvfile)
#     writer.writerows(rows)
# # Cargar el archivo CSV en el bucket de S3
# s3.upload_file('registros_ctr.csv', bucket_name, 'registros_ctr.csv')


# Cerrar la conexión
cur.close()
conn.close()
