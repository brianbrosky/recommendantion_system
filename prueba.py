import datetime
import pandas as pd
import boto3

s3 = boto3.client("s3") #definimos un cliente para trabajar con S3 usando boto3
bucket_name = "udesa-tp" #el nombre de nuestro bucket creado

s3_object_advertiser_ids = "Data/Raw/advertiser_ids.csv" #el archivo que vamos a traernos
s3_object_ads_views = "Data/Raw/ads_views.csv" #el archivo que vamos a traernos
s3_object_product_views = "Data/Raw/product_views.csv" #el archivo que vamos a traernos

ds='2023-04-29'

def FiltrarDatos(s3_object_advertiser_ids, s3_object_ads_views, s3_object_product_views, ds):
  
  
  '''
  funcion que agarra los logs de vistas de cada advertiser, de cada producto,
   y los filtra según la fecha de corrida del script y los advertisers activos
  '''
  
  obj = s3.get_object(Bucket = bucket_name, Key=s3_object_advertiser_ids) #definimos el archivo a levantar
  df_advertiser_ids = pd.read_csv(obj['Body']) #levantamos el DF
  
  obj = s3.get_object(Bucket = bucket_name, Key=s3_object_ads_views) #definimos el archivo a levantar
  df_ads_views = pd.read_csv(obj['Body']) #levantamos el DF

  obj = s3.get_object(Bucket = bucket_name, Key=s3_object_product_views) #definimos el archivo a levantar
  df_product_views = pd.read_csv(obj['Body']) #levantamos el DF
  
  print(df_product_views.head())

  
  #df_advertiser_ids.head()#from datetime import timedelta



  #establecemos la fecha de corrida
  #df_advertiser_ids = pd.read_csv(df_advertiser_ids)
  #df_product_views = pd.read_csv(df_product_views)
  #df_ads_views = pd.read_csv(df_ads_views)


  fecha_ayer =  datetime.datetime.strptime(ds, '%Y-%m-%d') - datetime.timedelta(days=1)
  
  #convertimos los campos date en datetime
  df_product_views['date'] = pd.to_datetime(df_product_views['date'])
  df_ads_views['date'] = pd.to_datetime(df_ads_views['date'])
  
  #filtramos los dataframes para quedarnos con los datos antiguos a la fecha de hoy
  df_product_views = df_product_views[df_product_views['date']==fecha_ayer]
  df_ads_views = df_ads_views[df_ads_views['date']==fecha_ayer]
  
  #filtramos los datasets para quedarnos con los advertisers activos
  df_product_views = df_product_views[df_product_views['advertiser_id'].isin(df_advertiser_ids['advertiser_id'])]
  df_ads_views = df_ads_views[df_ads_views['advertiser_id'].isin(df_advertiser_ids['advertiser_id'])] 

  #Guardamos los DF filtrados
  #df_product_views.to_csv(location+"/Processed/product_views_filt.csv", index=False)
  #df_ads_views.to_csv(location+"/Processed/ads_views_filt.csv", index=False)

  s3.put_object(Bucket=bucket_name, Key='Data/Processed/product_views_filt.csv', Body=df_product_views.to_csv(index=False))#.encode('utf-8'))
  s3.put_object(Bucket=bucket_name, Key='Data/Processed/ads_views_filt.csv', Body=df_ads_views.to_csv(index=False))#.encode('utf-8'))

  print('GUARDADO EN S3')
  return

FiltrarDatos(s3_object_advertiser_ids, s3_object_ads_views, s3_object_product_views, ds)
