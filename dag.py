import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
#from datetime import timedelta
#from datetime import datetime

pd.to_datetime(pd.Timestamp.today().date()) 

def FiltrarDatos(df_advertiser_ids, df_product_views, df_ads_views, ds, **kwargs):
  '''
  funcion que agarra los logs de vistas de cada advertiser, de cada producto,
   y los filtra según la fecha de corrida del script y los advertisers activos
  '''
  

  #establecemos la fecha de corrida
  df_advertiser_ids = pd.read_csv(df_advertiser_ids)
  df_product_views = pd.read_csv(df_product_views)
  df_ads_views = pd.read_csv(df_ads_views)


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
  df_product_views.to_csv(location+"/Processed/product_views_filt.csv", index=False)
  df_ads_views.to_csv(location+"/Processed/ads_views_filt.csv", index=False)


  return



def TopProduct (df_product_views_filt, ds, **kwargs):
    '''
    Esta función toma las vistas de productos ya filtradas y por cada advertiser
    se queda con el top 20 de productos vistos
    '''
    
    df_product_views_filt = pd.read_csv(df_product_views_filt)
    
    #Agrupamos por advertiser y producto y contamos la cantidad de registros por cada combinación guardando el dato en la columna 'cantidad'
    df_count = df_product_views_filt.groupby(['advertiser_id', 'product_id']).size().reset_index(name='cantidad')

    #Ordenamos el DF anterior en order ascendente de vendedores y y de mayor a menor en cuanto a la cantidad.
    df_count_sorted = df_count.sort_values(['advertiser_id', 'cantidad'], ascending=[True, False])

    #Con el DF ordenado agrupamos por advertiser y tomamos el top 20.
    df_top20 = df_count_sorted.groupby('advertiser_id').head(20)
    
    #Creamos una columna con la fecha de recomendacion
    fecha_hoy =   datetime.datetime.strptime(ds, '%Y-%m-%d')

    df_top20['fecha_recom'] = fecha_hoy 
    df_top20.to_csv(location+"/Processed/TopProduct.csv", index=False)

    return 


def TopCTR (df_ads_views_filt, ds, **kwargs):
    
    df_ads_views_filt = pd.read_csv(df_ads_views_filt)
    
    # Agrupamos el DF por advertiser, producto y tipo para luego contar la cantidad de veces que aparece cada combinación
    df_count = df_ads_views_filt.groupby(['advertiser_id', 'product_id', 'type']).size().reset_index(name='cantidad')

    # Pivoteamos el DF para tener una fila por cada combinación de adviser-producto y columnas para la cantidad de vistas y clicks
    df_pivoted = df_count.pivot_table(index=['advertiser_id', 'product_id'], columns='type', values='cantidad', fill_value=0).reset_index()

    # Filtramos el DF para eliminar combinaciones donde no hay clicks
    df_filtered = df_pivoted[df_pivoted['click'] > 0]
    
    # Calculamos la tasa de click para cada combinación de advertiser-product
    df_filtered['click-through-rate'] = df_filtered['click'] / df_pivoted['impression']

    # Ordenamos el DF por advertiser en forma ascendente y rate descendente.
    df_sorted = df_filtered.sort_values(['advertiser_id', 'click-through-rate'], ascending=[True, False])

    # Agrupamos el DF por advertiser y tomamos los top 20 productos con mejor tasa para cada advertiser.
    df_top20_CTR = df_sorted.groupby('advertiser_id').head(20)
    
    #Creamos una columna con la fecha de recomendacion
    fecha_hoy =   datetime.datetime.strptime(ds, '%Y-%m-%d')
    df_top20_CTR['fecha_recom'] = fecha_hoy #pd.to_datetime(pd.Timestamp.today().date()).strftime('%Y-%m-%d')

    df_top20_CTR.to_csv(location+"/Processed/TopCTR.csv", index=False)

    return 


def DBWritting(df_topCTR, df_topProduct):
    
    df_topCTR = pd.read_csv(df_topCTR)
    df_topProduct = pd.read_csv(df_topProduct)
    

    df_topCTR.to_csv(location + '/RDS/df_topCTR.csv', index=False)
    df_topProduct.to_csv(location + '/RDS/df_topProduct.csv', index=False)

    return 

location = "/home/brian/airflow/dags/Data"
df_ads_views = '/home/brian/airflow/dags/Data/Raw/ads_views.csv'
df_advertiser_ids = '/home/brian/airflow/dags/Data/Raw/advertiser_ids.csv'
df_product_views = '/home/brian/airflow/dags/Data/Raw/product_views.csv'
df_ads_views_filt = '/home/brian/airflow/dags/Data/Processed/ads_views_filt.csv'
df_product_views_filt = '/home/brian/airflow/dags/Data/Processed/product_views_filt.csv'
df_topCTR = '/home/brian/airflow/dags/Data/Processed/TopCTR.csv'
df_topProduct = '/home/brian/airflow/dags/Data/Processed/TopProduct.csv'

#Definimos nuestro DAG y sus tareas.
with DAG(
    dag_id = 'Recomendar',
    schedule_interval= '0 0 * * *', #se ejecuta a las 00:00 todos los días, todas las semanas, todos los meses
    start_date=datetime.datetime(2022,4,1),
    catchup=False
) as dag:

    FiltrarDatos = PythonOperator(
        task_id='Filtro',
        python_callable=FiltrarDatos, #función definida arriba
        op_kwargs = {"df_advertiser_ids" : df_advertiser_ids,
                    "df_product_views": df_product_views,
                    "df_ads_views":df_ads_views}
    )

    TopCTR = PythonOperator(
        task_id='TopCTR',
        python_callable=TopCTR, #función definida arriba
        op_kwargs = {"df_ads_views_filt" : df_ads_views_filt}
    )

    TopProduct = PythonOperator(
        task_id='TopProduct',
        python_callable=TopProduct, #función definida arriba
        op_kwargs = {"df_product_views_filt" : df_product_views_filt}
    )

    DBWritting = PythonOperator(
        task_id='DBWritting',
        python_callable=DBWritting, #función definida arriba
        op_kwargs = {"df_topCTR" : df_topCTR,
                     "df_topProduct" : df_topProduct}
    )


#Dependencias
FiltrarDatos >> TopCTR
FiltrarDatos >> TopProduct
[TopCTR, TopProduct] >> DBWritting