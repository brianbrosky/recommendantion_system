import boto3
import pandas as pd

s3 = boto3.client("s3") #definimos un cliente para trabajar con S3 usando boto3
bucket_name = "udesa-tp" #el nombre de nuestro bucket creado

s3_object_df_top20 = "Data/Processed/df_top20.csv"
s3_object_df_top20_CTR = "Data/Processed/df_top20_CTR.csv"

# obj = s3.get_object(Bucket = bucket_name, Key=s3_object_df_top20) #definimos el archivo a levantar
# df_topProduct = pd.read_csv(obj['Body']) #levantamos el DF
    
# obj = s3.get_object(Bucket = bucket_name, Key=s3_object_df_top20_CTR) #definimos el archivo a levantar
# df_topCTR = pd.read_csv(obj['Body']) #levantamos el DF
    
# print(df_topProduct.dtypes)
# print(df_topCTR.dtypes)
    
    
# obj = s3.get_object(Bucket = bucket_name, Key=s3_object_df_top20) #definimos el archivo a levantar
# df_topProduct = pd.read_csv(obj['Body']) #levantamos el DF
    
obj = s3.get_object(Bucket = bucket_name, Key=s3_object_df_top20_CTR) #definimos el archivo a levantar
df_topCTR = pd.read_csv(obj['Body']) #levantamos el DF

    #s3.put_object(Bucket=bucket_name, Key='Data/Processed/df_top20_CTR_final.csv', Body=df_topCTR.to_csv(index=False))#.encode('utf-8'))
    #s3.put_object(Bucket=bucket_name, Key='Data/Processed/df_top20_Product_final.csv', Body=df_topProduct.to_csv(index=False))#.encode('utf-8'))
    
    #Enviando a RDS
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

    # Poblar la tabla con los datos del dataframe
for index, row in df_topCTR.iterrows():
    #cur.execute(f"INSERT INTO top_20_ctr (adv_id, product_id, click, impression, clickthroughrate, fecha_recom) VALUES (%s);", tuple(row))
    cur.execute("INSERT INTO top_20_ctr (adv_id, product_id, click, impression, clickthroughrate, fecha_recom) VALUES (%(advertiser_id)s, %(product_id)s, %(click)s, %(impression)s, %(click-through-rate)s, %(fecha_recom)s);", row.to_dict())
    #cur.execute("INSERT INTO top_20 (adv_id, product_id, cantidad, fecha_recom) VALUES (%(advertiser_id)s, %(product_id)s, %(cantidad)s, %(fecha_recom)s);", row.to_dict())

    # Confirmar los cambios
conn.commit()

    # Poblar la tabla con los datos del dataframe
# for index, row in df_topProduct.iterrows():
#     cur.execute(f"INSERT INTO top_20 (adv_id, product_id, cantidad, fecha_recom) VALUES (%s);", tuple(row))
#         #cur.execute("INSERT INTO top_20_ctr (adv_id, product_id, cantidad, fecha_recom) VALUES (%s, %s, %s, %s);", (row['adv_id'], row['product_id'], row['cantidad'], row['fecha_recom']))

    # Confirmar los cambios
# conn.commit()
    
    # Cerrar la conexión
cur.close()
conn.close()
    
    
# s3 = boto3.client("s3") #definimos un cliente para trabajar con S3 usando boto3
# bucket_name = "data-raw-udesa-prueba" #el nombre de nuestro bucket creado
# s3_object = "advertiser_ids.csv" #el archivo que vamos a traernos

# obj = s3.get_object(Bucket = bucket_name, Key=s3_object) #definimos el archivo a levantar

# df_advertiser_ids = pd.read_csv(obj['Body']) #levantamos el DF
# df_advertiser_ids.head()

# df_product_views = pd.read_csv(df_product_views)
# df_ads_views = pd.read_csv(df_ads_views)


# s3 = boto3.client('s3')
# bucket_name = 'nombre_del_bucket'
# file_path = 'ruta_al_archivo_csv'
# object_name = 'nombre_del_archivo_csv_en_el_bucket'

# with open(file_path, 'rb') as file:
#     s3.put_object(Bucket=bucket_name, Key=object_name, Body=file)
