from fastapi import FastAPI, HTTPException
from datetime import datetime, timedelta
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
    # Definimos fecha
    fecha = (datetime.now()).strftime('%Y-%m-%d')
    
    # Construimos query
    sql_query = f"SELECT * FROM {tabla} WHERE adv_id = '{adv_id}' AND fecha_recom = '{fecha}'"
    active_adv_query = f"SELECT adv_id FROM top_20 UNION SELECT adv_id FROM top_20_ctr" #NUEVO

    # Corremos query y devolvemos resultados
    cursor, results = run_query(sql_query)
    adv_cursor, adv_results = run_query(active_adv_query) #NUEVO

    # Verificamos si no se encontraron resultados
    # if len(results) == 0:
    #     message = f"No se encontraron resultados para el advertiser {adv_id}, en el modelo {tabla} del día {fecha}"
    #     raise HTTPException(status_code=404, detail=message)
    
    #NUEVO
    if len(results) == 0:
        if {tabla} not in ["top_20", "top_20_ctr"]:
            message = f"El modelo {tabla} no es un modelo válido."
            raise HTTPException(status_code=404, detail=message)
        elif {adv_id} not in adv_results:
            message = f"No existe el advertiser {adv_id} en nuestra base."
            raise HTTPException(status_code=404, detail=message)
        else:
            message = f"No se encontraron resultados para el advertiser {adv_id}, en el modelo {tabla} del día {fecha}"
            raise HTTPException(status_code=202, detail=message)


    columns = [desc[0] for desc in cursor.description]
    rows = [dict(zip(columns, row)) for row in results]
    
    return {tabla: rows}



@app.get("/history/{adv_id}")
async def run_query_7_días(adv_id: str):
    # Definimos periodo
    fecha_inicio = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    fecha_final = (datetime.now()).strftime('%Y-%m-%d')

    # Construimos queries
    top_20_query = f"SELECT * FROM top_20 WHERE adv_id = '{adv_id}' AND fecha_recom BETWEEN '{fecha_inicio}' AND '{fecha_final}'"
    top_20_ctr_query = f"SELECT * FROM top_20_ctr WHERE adv_id = '{adv_id}' AND fecha_recom  BETWEEN '{fecha_inicio}' AND '{fecha_final}'"
    active_adv_query = f"SELECT adv_id FROM top_20 UNION SELECT adv_id FROM top_20_ctr" #NUEVO

    # Corremos queries y devolvemos resultados
    top_20_cursor, top_20_results = run_query(top_20_query)
    top_20_columns = [desc[0] for desc in top_20_cursor.description]
    top_20_rows = [dict(zip(top_20_columns, row)) for row in top_20_results]

    top_20_ctr_cursor, top_20_ctr_results = run_query(top_20_ctr_query)
    top_20_ctr_columns = [desc[0] for desc in top_20_ctr_cursor.description]
    top_20_ctr_rows = [dict(zip(top_20_ctr_columns, row)) for row in top_20_ctr_results]

    adv_cursor, adv_results = run_query(active_adv_query) #NUEVO

    response = {}

    # Verificamos si no se encontraron resultados	
    if len(top_20_rows) == 0:
        if {adv_id} not in adv_results:
            message = f"No existe el advertiser {adv_id} en nuestra base." #NUEVO
            raise HTTPException(status_code=404, detail=message)
        else:
            message =  f"No se encontraron resultados para el advertiser {adv_id}, en el modelo top_20 para el periodo del {fecha_inicio} al {fecha_final}"
            raise HTTPException(status_code=202, detail=message)
    else:
        response["top_20"] = top_20_rows

    if len(top_20_ctr_rows) == 0:
        if {adv_id} not in adv_results:
            message = f"No existe el advertiser {adv_id} en nuestra base." #NUEVO
            raise HTTPException(status_code=404, detail=message)
        else:
            message =  f"No se encontraron resultados para el advertiser {adv_id}, en el modelo top_20_ctr para el periodo del {fecha_inicio} al {fecha_final}"
            raise HTTPException(status_code=202, detail=message)
    else:
        response["top_20_ctr"] = top_20_ctr_rows

    return response

@app.get("/stats/")
async def run_stats():
    # Adv totales
    total_adv = "SELECT COUNT(DISTINCT adv_id) FROM (SELECT adv_id FROM top_20 UNION SELECT adv_id FROM top_20_ctr)" #NUEVO
    cursor, results = run_query(total_adv)
    if len(results) == 0:
        raise HTTPException(status_code=404, detail="No se encontraron resultados para calcular los advertisers totales")
    total_advertisers = results[0][0]


    # Adv con mayor variacion diaria
    variacion = """
        SELECT adv_id, COUNT(DISTINCT product_id) AS variacion
        FROM (
            SELECT adv_id, product_id
            FROM top_20
            UNION ALL
            SELECT adv_id, product_id
            FROM top_20_ctr
        ) AS combined
        GROUP BY adv_id
        ORDER BY variacion DESC
        LIMIT 5
    """
    cursor, results = run_query(variacion)
    if len(results) == 0:
        raise HTTPException(status_code=404, detail="No se encontraron resultados para calcular la variación por día de los advertisers")
    variacion_por_dia = {adv_id: variacion for adv_id, variacion in results}


    # Porcentaje de coincidencia
    coincidencia = """
        SELECT adv_id, AVG(clickthroughrate) AS coincidencia_promedio
        FROM top_20_ctr
        GROUP BY adv_id
    """
    cursor, results = run_query(coincidencia)
    if len(results) == 0:
        raise HTTPException(status_code=404, detail="No se encontraron resultados para calcular el porcentaje de coincidencia entre modelos para cada advertiser")
    porcentaje_coincidencia = {adv_id: round(coincidencia_promedio * 100, 2) for adv_id, coincidencia_promedio in results}


    return {
        "Advertisers totales": total_advertisers,
        "Variación por día": variacion_por_dia,
        "Porcentaje de coincidencia": porcentaje_coincidencia
    }

