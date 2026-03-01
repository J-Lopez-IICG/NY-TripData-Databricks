import os
from databricks import sql
from dotenv import load_dotenv


def create_gold_fact_trips() -> str:
    """
    Nodo Gold: Genera la tabla de hechos final con todos los registros
    limpios y validados según el EDA.
    """
    load_dotenv()
    host = os.getenv("DATABRICKS_HOST", "").replace("https://", "")
    token = os.getenv("DATABRICKS_TOKEN")
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")

    # Esta consulta genera la tabla completa, no un resumen
    query = """
    CREATE OR REPLACE TABLE main.gestion_gold.gold_fact_trips AS
    SELECT 
        * FROM main.gestion_silver.silver_yellow_trips
    WHERE tpep_pickup_datetime IS NOT NULL              -- Hallazgo EDA: 1 nulo
      AND trip_distance > 0 AND trip_distance < 500      -- Hallazgo EDA: 71k con 0 y outliers de 1.9M
      AND total_amount > 0                              -- Hallazgo EDA: 4.5k negativos
      AND passenger_count > 0                           -- Hallazgo EDA: 608 con 0
      AND payment_type IS NOT NULL;                     -- Hallazgo EDA: 8 nulos
    """

    try:
        connection = sql.connect(
            server_hostname=host,
            http_path=f"/sql/1.0/warehouses/{warehouse_id}",
            access_token=token,
        )
        cursor = connection.cursor()
        cursor.execute(query)
        cursor.close()
        connection.close()
        return "Tabla Gold de Hechos (12M registros filtrados) creada con éxito."
    except Exception as e:
        raise Exception(f"Error creando Fact Table en Gold: {e}")
