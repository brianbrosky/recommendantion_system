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
    # Definimos fecha
    fecha = (datetime.now()).strftime('%Y-%m-%d')
    
    # Verificamos si {tabla} es un nombre de tabla válido
    valid_tables = ["top_20", "top_20_ctr"]
    if tabla not in valid_tables:
        message = f"El modelo {tabla} no es un válido."
        raise HTTPException(status_code=404, detail=message)
    
    # Construimos query
    sql_query = f"SELECT * FROM {tabla} WHERE adv_id = '{adv_id}' AND fecha_recom = '{fecha}'"
    active_adv_query = f"SELECT adv_id FROM top_20 UNION SELECT adv_id FROM top_20_ctr" #NUEVO

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
    # Definimos periodo
    fecha_inicio = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    fecha_final = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # Construimos queries
    top_20_query = f"SELECT fecha_recom, product_id FROM top_20 WHERE adv_id = '{adv_id}' AND fecha_recom BETWEEN '{fecha_inicio}' AND '{fecha_final}'"
    top_20_ctr_query = f"SELECT fecha_recom, product_id FROM top_20_ctr WHERE adv_id = '{adv_id}' AND fecha_recom  BETWEEN '{fecha_inicio}' AND '{fecha_final}'"

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
    # Query para adv unicos top_20
    top_20_query = "SELECT DISTINCT adv_id FROM top_20"

    # Query para adv unicos top_20_ctr
    top_20_ctr_query = "SELECT DISTINCT adv_id FROM top_20_ctr"

    # Traemos resultados
    top_20_cursor, top_20_results = run_query(top_20_query)
    top_20_ctr_cursor, top_20_ctr_results = run_query(top_20_ctr_query)

    # Advertiser ids
    adv_id = set([row[0] for row in top_20_results] + [row[0] for row in top_20_ctr_results])

    # Contamos adv_id
    count = len(adv_id)

    # Query para traer los top 3 advertisers en cambios de productos
    top_cambios_query = """
        SELECT adv_id, COUNT(DISTINCT product_id) AS change_count
        FROM (
            SELECT adv_id, product_id
            FROM top_20
            WHERE fecha_recom BETWEEN (current_date - interval '7 days') AND current_date
            UNION ALL
            SELECT adv_id, product_id
            FROM top_20_ctr
            WHERE fecha_recom BETWEEN (current_date - interval '7 days') AND current_date
        ) AS combined
        GROUP BY adv_id
        ORDER BY change_count DESC
        LIMIT 3
    """

    # Ejecutamos query
    top_cambios_cursor, top_cambios_results = run_query(top_cambios_query)

    # Extraemos adv ids y counts
    top_cambios = [{"Advertiser ID": row[0], "Cantidad de productos únicos en los últimos días": row[1]} for row in top_cambios_results]

    # Porcentajes de coincidencia
    coincidencia_query = """
        SELECT t20.adv_id, t20.fecha_recom,
            (COUNT(DISTINCT t20.product_id) * 100.0 / COUNT(DISTINCT t20_ctr.product_id)) AS coincidence_percentage
        FROM top_20 t20
        INNER JOIN top_20_ctr t20_ctr ON t20.fecha_recom = t20_ctr.fecha_recom AND t20.adv_id = t20_ctr.adv_id
        GROUP BY t20.adv_id, t20.fecha_recom
    """

    # Execute query and fetch results
    coincidencia_cursor, coincidencia_results = run_query(coincidencia_query)

    # Group the percentages by adv_id and calculate the median for each adv_id
    coincidencia_por_adv_id = {}
    for row in coincidencia_results:
        adv_id = row[0]
        fecha = row[1]
        porcentaje = row[2]

        if adv_id not in coincidencia_por_adv_id:
            coincidencia_por_adv_id[adv_id] = []

        coincidencia_por_adv_id[adv_id].append((fecha, porcentaje))

    # Media por adv
    coincidencia_media_por_adv_id = {
        adv_id: statistics.mean([porcentaje for _, porcentaje in data])
        for adv_id, data in coincidencia_por_adv_id.items()
    }

    # Media global
    coincidencia_media_global = statistics.mean(coincidencia_media_por_adv_id.values())

    return {
	"Cantidad de advertisers": count,
        "Advertisers activos": adv_id,
        "Top 3 advertisers con más cambios en sus productos en los últimos 7 días)": top_cambios,
        "Coincidencia promedio por advertiser": coincidencia_media_por_adv_id,
        "Coincidencia global promedio": coincidencia_media_global
    }


    
#from fastapi.responses import JSONResponse
#from starlette.exceptions import HTTPException as StarletteHTTPException    
#@app.exception_handler(StarletteHTTPException)
#async def custom_exception_handler(request, exc):
#    if exc.status_code == 404:
#        message = {"Error": "Esta URL no existe"}
#        return JSONResponse(status_code=404, content=message)
#    return JSONResponse(status_code=exc.status_code, content={"message": str(exc)})
