from fastapi import FastAPI, HTTPException
from datetime import datetime, timedelta
import psycopg2

# Credenciales para conectarse con la base de datos
host = "database.crv8bjyoa2v8.us-east-1.rds.amazonaws.com" 
port = 5432
dbname = "recomendaciones"
user = "modelo"
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

@app.get("/recommendations/{adv_id}/{table_name}")
async def run_query_diaria(adv_id: str, table_name: str):
    # Definimos fecha
    fecha = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Construimos query
    sql_query = f"SELECT * FROM {table_name} WHERE adv_id = '{adv_id}' AND fecha_recom = '{fecha}'"
    
    # Corremos query y devolvemos resultados
    cursor, results = run_query(sql_query)
    columns = [desc[0] for desc in cursor.description]
    rows = [dict(zip(columns, row)) for row in results]
    return {"data": rows}

@app.get("/history/{adv_id}")
async def run_query_7_días(adv_id: str):
    # Definimos periodo
    fecha_inicio = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    fecha_final = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Construimos queries
    top_20_query = f"SELECT * FROM top_20 WHERE adv_id = '{adv_id}' AND fecha_recom BETWEEN '{fecha_inicio}' AND '{fecha_final}'"
    top_20_ctr_query = f"SELECT * FROM top_20_ctr WHERE adv_id = '{adv_id}' AND fecha_recom  BETWEEN '{fecha_inicio}' AND '{fecha_final}'"
    
    # Corremos queries y devolvemos resultados
    top_20_cursor, top_20_results = run_query(top_20_query)
    top_20_columns = [desc[0] for desc in top_20_cursor.description]
    top_20_rows = [dict(zip(top_20_columns, row)) for row in top_20_results]
    
    top_20_ctr_cursor, top_20_ctr_results = run_query(top_20_ctr_query)
    top_20_ctr_columns = [desc[0] for desc in top_20_ctr_cursor.description]
    top_20_ctr_rows = [dict(zip(top_20_ctr_columns, row)) for row in top_20_ctr_results]
    
    return {"top_20": top_20_rows, "top_20_ctr": top_20_ctr_rows}

