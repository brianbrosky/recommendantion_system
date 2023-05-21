from fastapi import FastAPI, HTTPException
from datetime import datetime, timedelta
import statistics
import psycopg2

# Credenciales para conectarse con la base de datos
host = "database.crv8bjyoa2v8.us-east-1.rds.amazonaws.com" 
port = 5432
dbname = "recomendaciones"
user = "modelos"
password = "Chavoloco23"

#host = "localhost" 
#port = 5432
#dbname = "postgres"
#user = "postgres"
#password = "pass"


def run_query(query):
    conn = psycopg2.connect(
        host = host,
        port = port,
        dbname = dbname,
        user = user,
        password = password
    )
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return cursor, results

app = FastAPI()

@app.get("/recommendations/{adv_id}/{tabla}")
async def run_query_diaria(adv_id: str, tabla: str):
    # Definimos fecha para las excepciones
    fecha = (datetime.now()).strftime('%Y-%m-%d')
    
    # Verificamos si {tabla} es un nombre de tabla válido
    valid_tables = ["top_20", "top_20_ctr"]
    if tabla not in valid_tables:
        message = f"El modelo {tabla} no es un válido."
        raise HTTPException(status_code=404, detail=message)
    
    # Construimos query
    sql_query = f"SELECT * FROM {tabla} WHERE adv_id = '{adv_id}' AND CAST(fecha_recom AS DATE) = current_date"
    
    # Corremos query y devolvemos resultados
    cursor, results = run_query(sql_query)
    
    # Verificamos si no se encontraron resultados
    if len(results) == 0:
        message = f"No se encontraron resultados para el advertiser {adv_id}, en el modelo {tabla} del día {fecha}"
        raise HTTPException(status_code=404, detail=message)
    
    # Convertimos tuplas en diccionarios
    columns = [desc[0] for desc in cursor.description]
    rows = [dict(zip(columns, row)) for row in results]
    
    # Extraemos product ids
    top_products = [row["product_id"] for row in rows]
    
    return {
        "Advertiser": adv_id,
        "Modelo": tabla,
        "Fecha": fecha,
        "Top products": top_products
    }

	
@app.get("/history/{adv_id}")
async def run_query_7_días(adv_id: str):
    # Definimos periodo para las excepciones
    fecha_inicio = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    fecha_final = (datetime.now()).strftime('%Y-%m-%d')

    # Construimos queries
    top_20_query = f"SELECT CAST(fecha_recom AS DATE) AS fecha_recom, product_id FROM top_20 WHERE adv_id = '{adv_id}' AND CAST(fecha_recom AS DATE) BETWEEN (current_date - interval '6 days') AND current_date"
    top_20_ctr_query = f"SELECT CAST(fecha_recom AS DATE) AS fecha_recom, product_id FROM top_20_ctr WHERE adv_id = '{adv_id}' AND CAST(fecha_recom AS DATE) BETWEEN (current_date - interval '6 days') AND current_date"

    # Corremos queries y devolvemos resultados
    top_20_cursor, top_20_results = run_query(top_20_query)
    top_20_rows = [(row[0].strftime('%Y-%m-%d'), row[1]) for row in top_20_results]

    top_20_ctr_cursor, top_20_ctr_results = run_query(top_20_ctr_query)
    top_20_ctr_rows = [(row[0].strftime('%Y-%m-%d'), row[1]) for row in top_20_ctr_results]

    response = {
        "Advertiser": adv_id,
        "Modelo": {}
    }

    # Verificamos si no se encontraron resultados    
    if len(top_20_rows) == 0:
        message = f"No se encontraron resultados para el advertiser {adv_id}, en el modelo top_20 para el periodo del {fecha_final} al {fecha_inicio}"
        raise HTTPException(status_code=404, detail=message)
    else:
        response["Modelo"]["top_20"] = group_by_date(top_20_rows)

    if len(top_20_ctr_rows) == 0:
        message = f"No se encontraron resultados para el advertiser {adv_id}, en el modelo top_20_ctr para el periodo del {fecha_final} al {fecha_inicio}"
        raise HTTPException(status_code=404, detail=message)
    else:
        response["Modelo"]["top_20_ctr"] = group_by_date(top_20_ctr_rows)

    return response


def group_by_date(rows):
    grouped_data = {}
    for date, product_id in rows:
        if date not in grouped_data:
            grouped_data[date] = []
        grouped_data[date].append(product_id)
    return grouped_data

@app.get("/stats/")
async def get_stats():
    #############
    ## ADVERTISERS UNICOS	

    # Query para adv unicos top_20
    top_20_query = "SELECT DISTINCT adv_id FROM top_20"

    # Query para adv unicos top_20_ctr
    top_20_ctr_query = "SELECT DISTINCT adv_id FROM top_20_ctr"

    # Traemos resultados
    top_20_cursor, top_20_results = run_query(top_20_query)
    top_20_ctr_cursor, top_20_ctr_results = run_query(top_20_ctr_query)

    # Advertiser ids
    unique_adv_id = set([row[0] for row in top_20_results] + [row[0] for row in top_20_ctr_results])

    # Contamos adv_id
    count_unique_adv_id = len(unique_adv_id)

    #############		
    ### MAS CAMBIO DE PRODUCTO 

    # Query para traer los top 5 advertisers en cambios de productos
    top_mas_cambios_query = """
        SELECT adv_id, COUNT(DISTINCT product_id) AS change_count
        FROM (
            SELECT adv_id, product_id
            FROM top_20
            WHERE CAST(fecha_recom AS DATE) BETWEEN (current_date - interval '6 days') AND current_date
            UNION ALL
            SELECT adv_id, product_id
            FROM top_20_ctr
            WHERE CAST(fecha_recom AS DATE) BETWEEN (current_date - interval '6 days') AND current_date
        ) AS combined
        GROUP BY adv_id
        ORDER BY change_count DESC
        LIMIT 3
    """

    # Ejecutamos query
    top_mas_cambios_cursor, top_mas_cambios_results = run_query(top_mas_cambios_query)

    # Extraemos adv ids y counts
    top_mas_cambios = [{"Advertiser ID": row[0], "Cantidad de productos únicos en los últimos 7 días": row[1]} for row in top_mas_cambios_results]


    #############		
    ### MENOS CAMBIO DE PRODUCTO 

    # Query para traer los top 5 advertisers en cambios de productos
    top_menos_cambios_query = """
        SELECT adv_id, COUNT(DISTINCT product_id) AS change_count
        FROM (
            SELECT adv_id, product_id
            FROM top_20
            WHERE CAST(fecha_recom AS DATE) BETWEEN (current_date - interval '6 days') AND current_date
            UNION ALL
            SELECT adv_id, product_id
            FROM top_20_ctr
            WHERE CAST(fecha_recom AS DATE) BETWEEN (current_date - interval '6 days') AND current_date
        ) AS combined
        GROUP BY adv_id
        ORDER BY change_count ASC
        LIMIT 3
    """

    # Ejecutamos query
    top_menos_cambios_cursor, top_menos_cambios_results = run_query(top_menos_cambios_query)

    # Extraemos adv ids y counts
    top_menos_cambios = [{"Advertiser ID": row[0], "Cantidad de productos únicos en los últimos 7 días": row[1]} for row in top_menos_cambios_results]
    
    

    #############		
    ### COINCIDENCIA DE MODELOS POR ADVERTISER 	
     
    coincidencia_query = """
        WITH top_20_tmp AS (
            SELECT
            adv_id,
            product_id,
            CAST(fecha_recom AS DATE) as fecha_recom
            FROM top_20
            WHERE CAST(fecha_recom AS DATE) BETWEEN (current_date - interval '6 days') AND current_date
            ),
            top_ctr_tmp AS (
            SELECT
            adv_id,
            product_id,
            CAST(fecha_recom AS DATE) as fecha_recom
            FROM top_20_ctr
            WHERE CAST(fecha_recom AS DATE) BETWEEN (current_date - interval '6 days') AND current_date
            )
        SELECT
            a.fecha_recom,
            a.adv_id,
            COUNT(*) AS cant_coincidencias
        FROM top_20_tmp AS a
            INNER JOIN top_ctr_tmp AS b
                ON a.adv_id = b.adv_id
                AND a.product_id = b.product_id
                AND a.fecha_recom = b.fecha_recom
        GROUP BY 1, 2
    """

    # Ejecutamos query
    coincidencia_query_cursor, coincidencia_query_results = run_query(coincidencia_query)

    # Creamos un diccionario para almacenar los resultados agrupados por fecha
    coincidencias = {}

    # Procesamos los resultados y los agrupamos por fecha
    for row in coincidencia_query_results:
        fecha_recom = row[0]
        adv_id = row[1]
        cant_coincidencias = row[2]
    
        # Si la fecha no existe en el diccionario, la agregamos con una lista vacía
        if fecha_recom not in coincidencias:
            coincidencias[fecha_recom] = []
        
        # Agregamos los datos a la lista correspondiente a la fecha
        coincidencias[fecha_recom].append({"Advertiser ID": adv_id, "Cantidad de coincidencias": cant_coincidencias})


    #############		
    ### PRODUCTOS MAS RECOMENDADOS POR ADVERTISER
    
    # Query para traer los top 5 de productos mas recomendados pora advertiser
    top_product_query = """
        SELECT adv_id, product_id, COUNT(*) AS recommendation_count
        FROM (
            SELECT adv_id, product_id
            FROM top_20
            WHERE CAST(fecha_recom AS DATE) BETWEEN (current_date - interval '6 days') AND current_date
            UNION ALL
            SELECT adv_id, product_id
            FROM top_20_ctr
            WHERE CAST(fecha_recom AS DATE) BETWEEN (current_date - interval '6 days') AND current_date
        ) AS combined
        GROUP BY adv_id, product_id
        ORDER BY adv_id, recommendation_count DESC
    """

    # Ejecutamos query
    top_product_cursor, top_product_results = run_query(top_product_query)

    # Creamos un diccionario para almacenar los resultados
    top_products_por_advertiser = {}

    # Procesamos los resultados y los agrupamos por advertiser
    for row in top_product_results:
        adv_id = row[0]
        product_id = row[1]
        cantidad_recomendaciones = row[2]

        # si el advertiser no existe agregamos con una lista vacia
        if adv_id not in top_products_por_advertiser:
            top_products_por_advertiser[adv_id] = []

        # Agregamos detalles
        top_products_por_advertiser[adv_id].append({"Product ID": product_id, "Cantidad de recomendaciones": cantidad_recomendaciones})

    # Ordenamos
    for top_products in top_products_por_advertiser.values():
        top_products.sort(key=lambda x: x["Cantidad de recomendaciones"], reverse=True)

        # Filtramos los 5 primeros
        top_products[:] = top_products[:3]
        
    
    return {
        "Cantidad de advertisers": count_unique_adv_id,
        "Advertisers activos": unique_adv_id,
        "Top 3 advertisers con más productos únicos recomendados en los últimos 7 días": top_mas_cambios,
        "Top 3 advertisers con menos productos únicos recomendados en los últimos 7 días": top_menos_cambios,
        "Top 3 de productos más recomendados por advertiser en los últimos 7 días": top_products_por_advertiser,
        "Cantidad de coincidencias de ambos modelos en los últimos 7 días": coincidencias
    }
