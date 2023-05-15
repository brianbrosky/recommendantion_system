from datetime import datetime, timedelta
from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import boto3

# instanciamos los objetos de s3
s3 = boto3.client("s3") #definimos un cliente para trabajar con S3 usando boto3
bucket_name = "udesa-tp" #el nombre de nuestro bucket creado


s3_object_advertiser_ids = "Data/Raw/advertiser_ids.csv" #el archivo que vamos a traernos
s3_object_ads_views = "Data/Raw/ads_views.csv" #el archivo que vamos a traernos
s3_object_product_views = "Data/Raw/product_views.csv" #el archivo que vamos a traernos

s3_object_product_views_filt = "Data/Processed/product_views_filt.csv"
s3_object_ads_views_filt = "Data/Processed/ads_views_filt.csv"

s3_object_df_top20 = "Data/Processed/df_top20.csv"
s3_object_df_top20_CTR = "Data/Processed/df_top20_CTR.csv"



def FiltrarDatos(s3_object_advertiser_ids, s3_object_ads_views, s3_object_product_views, **kwargs):
    obj = s3.get_object(Bucket = bucket_name, Key=s3_object_advertiser_ids) #definimos el archivo a levantar
    df_advertiser_ids = pd.read_csv(obj['Body']) #levantamos el DF

    obj = s3.get_object(Bucket = bucket_name, Key=s3_object_ads_views) #definimos el archivo a levantar
    df_ads_views = pd.read_csv(obj['Body']) #levantamos el DF

    obj = s3.get_object(Bucket = bucket_name, Key=s3_object_product_views) #definimos el archivo a levantar
    df_product_views = pd.read_csv(obj['Body']) #levantamos el DF

    #execution_date = kwargs['data_interval_end'].date()
    #fecha_hoy = datetime.combine(execution_date, datetime.min.time())
    #yesterday = fecha_hoy - timedelta(days=1)
    #fecha_ayer =  yesterday.date()
    fecha_ayer = kwargs['execution_date'].date()

    #convertimos los campos date en datetime
    df_product_views['date'] = pd.to_datetime(df_product_views['date']).dt.date
    df_ads_views['date'] = pd.to_datetime(df_ads_views['date']).dt.date

    #filtramos los dataframes para quedarnos con los datos antiguos a la fecha de hoy
    df_product_views = df_product_views[df_product_views['date']==fecha_ayer]
    df_ads_views = df_ads_views[df_ads_views['date']==fecha_ayer]

    #filtramos los datasets para quedarnos con los advertisers activos
    df_product_views = df_product_views[df_product_views['advertiser_id'].isin(df_advertiser_ids['advertiser_id'])]
    df_ads_views = df_ads_views[df_ads_views['advertiser_id'].isin(df_advertiser_ids['advertiser_id'])] 

    #Guardamos los DF filtrados

    s3.put_object(Bucket=bucket_name, Key='Data/Processed/product_views_filt.csv', Body=df_product_views.to_csv(index=False))#.encode('utf-8'))
    s3.put_object(Bucket=bucket_name, Key='Data/Processed/ads_views_filt.csv', Body=df_ads_views.to_csv(index=False))#.encode('utf-8'))

    #print('GUARDADO EN S3')
    return


def TopProduct(s3_object_product_views_filt, **kwargs):
    '''
    Esta función toma las vistas de productos ya filtradas y por cada advertiser
    se queda con el top 20 de productos vistos
    '''

    obj = s3.get_object(Bucket = bucket_name, Key=s3_object_product_views_filt) #definimos el archivo a levantar
    df_product_views_filt = pd.read_csv(obj['Body']) #levantamos el DF


    #df_product_views_filt = pd.read_csv(df_product_views_filt)

    #Agrupamos por advertiser y producto y contamos la cantidad de registros por cada combinación guardando el dato en la columna 'cantidad'
    df_count = df_product_views_filt.groupby(['advertiser_id', 'product_id']).size().reset_index(name='cantidad')

    #Ordenamos el DF anterior en order ascendente de vendedores y de mayor a menor en cuanto a la cantidad.
    #No ordenamos por producto a igualdad de cantidades para obtener cierta aleatoriedad.
    df_count_sorted = df_count.sort_values(['advertiser_id', 'cantidad'], ascending=[True, False])
    #df_count_sorted = df_count.sort_values(by=[ 'advertiser_id','cantidad', 'product_id'], ascending=[True, False, True])

    #Con el DF ordenado agrupamos por advertiser y tomamos el top 20.
    df_top20 = df_count_sorted.groupby('advertiser_id').head(20)

    #Creamos una columna con la fecha de recomendacion
    fecha_hoy =   kwargs['data_interval_end'].date()

    df_top20['fecha_recom'] = fecha_hoy 
    s3.put_object(Bucket=bucket_name, Key='Data/Processed/df_top20.csv', Body=df_top20.to_csv(index=False))#.encode('utf-8'))

    return 


def TopCTR (s3_object_ads_views_filt, **kwargs):

    obj = s3.get_object(Bucket = bucket_name, Key=s3_object_ads_views_filt) #definimos el archivo a levantar
    df_ads_views_filt = pd.read_csv(obj['Body']) #levantamos el DF


    # Agrupamos el DF por advertiser, producto y tipo para luego contar la cantidad de veces que aparece cada combinación
    df_count = df_ads_views_filt.groupby(['advertiser_id', 'product_id', 'type']).size().reset_index(name='cantidad')

    # Pivoteamos el DF para tener una fila por cada combinación de adviser-producto y columnas para la cantidad de vistas y clicks
    df_pivoted = df_count.pivot_table(index=['advertiser_id', 'product_id'], columns='type', values='cantidad', fill_value=0).reset_index()

    # Filtramos el DF para eliminar combinaciones donde no hay clicks
    df_filtered = df_pivoted[df_pivoted['click'] > 0]

    # Calculamos la tasa de click para cada combinación de advertiser-product
    df_filtered['click-through-rate'] = df_filtered['click'] / (df_pivoted['impression'] + df_filtered['click'])

    # Ordenamos el DF por advertiser en forma ascendente y rate descendente.
    df_sorted = df_filtered.sort_values(['advertiser_id', 'click-through-rate'], ascending=[True, False])

    # Agrupamos el DF por advertiser y tomamos los top 20 productos con mejor tasa para cada advertiser.
    df_top20_CTR = df_sorted.groupby('advertiser_id').head(20)

    #Creamos una columna con la fecha de recomendacion
    fecha_hoy =  kwargs['data_interval_end'].date()
    df_top20_CTR['fecha_recom'] = fecha_hoy #pd.to_datetime(pd.Timestamp.today().date()).strftime('%Y-%m-%d')

    s3.put_object(Bucket=bucket_name, Key='Data/Processed/df_top20_CTR.csv', Body=df_top20_CTR.to_csv(index=False))#.encode('utf-8'))

    return 


def DBWriting(s3_object_df_top20, s3_object_df_top20_CTR):
    obj = s3.get_object(Bucket = bucket_name, Key=s3_object_df_top20) #definimos el archivo a levantar
    df_topProduct = pd.read_csv(obj['Body']) #levantamos el DF

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

    # Confirmar los cambios
    conn.commit()

    # Poblar la tabla con los datos del dataframe
    for index, row in df_topProduct.iterrows():
        #cur.execute(f"INSERT INTO top_20 (adv_id, product_id, cantidad, fecha_recom) VALUES (%s);", tuple(row))
        cur.execute("INSERT INTO top_20 (adv_id, product_id, cantidad, fecha_recom) VALUES (%(advertiser_id)s, %(product_id)s, %(cantidad)s, %(fecha_recom)s);", row.to_dict())

    # Confirmar los cambios
    conn.commit()

    # Cerrar la conexión
    cur.close()
    conn.close()


    return 

#Definimos nuestro DAG y sus tareas.
with DAG(
    dag_id = 'Recomendar',
    schedule_interval= '0 1 * * *', #se ejecuta a las 01:00 todos los días, todas las semanas, todos los meses
    start_date=datetime(2022,4,1),
    catchup=False,
    #dagrun_timeout=timedelta(minutes=60)
    ) as dag:
    FiltrarDatos = PythonOperator(
        task_id='Filtro',
        python_callable=FiltrarDatos, #función definida arriba
        op_kwargs = {"s3_object_advertiser_ids" : s3_object_advertiser_ids,
                     "s3_object_ads_views": s3_object_ads_views,
                     "s3_object_product_views":s3_object_product_views})
    
    TopCTR = PythonOperator(
        task_id='TopCTR',
        python_callable=TopCTR, #función definida arriba
        op_kwargs = {"s3_object_ads_views_filt" : s3_object_ads_views_filt})
    
    TopProduct = PythonOperator(
        task_id='TopProduct',
        python_callable=TopProduct, #función definida arriba
        op_kwargs = {"s3_object_product_views_filt" : s3_object_product_views_filt})
    
    DBWriting = PythonOperator(
        task_id='DBWriting',
        python_callable=DBWriting, #función definida arriba
        op_kwargs = {"s3_object_df_top20" : s3_object_df_top20,
                     "s3_object_df_top20_CTR" : s3_object_df_top20_CTR})


# #Dependencias
FiltrarDatos >> TopCTR
FiltrarDatos >> TopProduct
[TopCTR, TopProduct] >> DBWriting
