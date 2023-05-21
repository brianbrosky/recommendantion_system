import psycopg2
import pandas as pd
import boto3
import csv

# instanciamos los objetos de s3
s3 = boto3.client("s3") #definimos un cliente para trabajar con S3 usando boto3
bucket_name = "udesa-tp" #el nombre de nuestro bucket creado



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


# #SELECT PARA VER REGISTROS
cur.execute('select * from top_20') 
rows = cur.fetchall()

# Generar archivo CSV y escribir los registros
with open('registros.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(rows)

# Cargar el archivo CSV en el bucket de S3
s3.upload_file('registros.csv', bucket_name, 'registros.csv')
# s3.put_object(Bucket=bucket_name, Key='Data/Processed/registros.csv', Body=df_product_views.to_csv(index=False))#.encode('utf-8'))



cur.execute('select * from top_20_ctr') 
rows = cur.fetchall()

# Generar archivo CSV y escribir los registros
with open('registros_ctr.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(rows)

# Cargar el archivo CSV en el bucket de S3
s3.upload_file('registros_ctr.csv', bucket_name, 'registros_ctr.csv')

# Cerrar la conexión
cur.close()
conn.close()



# #BORRAR REGISTROS VIEJOS
# cur.execute("DELETE FROM top_20 WHERE fecha_recom <='2023-05-13'") 
# conn.commit()
# cur.execute("DELETE FROM top_20_ctr WHERE fecha_recom <='2023-05-13'") 
# conn.commit()
# cur.close()
# conn.close()
