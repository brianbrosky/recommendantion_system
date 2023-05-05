import boto3
import pandas as pd

s3 = boto3.client("s3") #definimos un cliente para trabajar con S3 usando boto3
bucket_name = "udesa-tp" #el nombre de nuestro bucket creado

s3_object_df_top20 = "Data/Processed/df_top20.csv"
s3_object_df_top20_CTR = "Data/Processed/df_top20_CTR.csv"

obj = s3.get_object(Bucket = bucket_name, Key=s3_object_df_top20) #definimos el archivo a levantar
df_topProduct = pd.read_csv(obj['Body']) #levantamos el DF
    
obj = s3.get_object(Bucket = bucket_name, Key=s3_object_df_top20_CTR) #definimos el archivo a levantar
df_topCTR = pd.read_csv(obj['Body']) #levantamos el DF
    
print(df_topProduct.dtypes)
print(df_topCTR.dtypes)
    
    

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
