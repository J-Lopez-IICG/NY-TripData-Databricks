import os
from databricks import sql
from dotenv import load_dotenv


def create_gold_vendor_efficiency() -> str:
    """
    Nodo Analytics: Calcula la rentabilidad (ingresos por milla)
    de cada proveedor usando los datos limpios de Silver.
    """
    load_dotenv()
    host = os.getenv("DATABRICKS_HOST", "").replace("https://", "")
    token = os.getenv("DATABRICKS_TOKEN")
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")

    query = """
    CREATE OR REPLACE TABLE main.gestion_gold.gold_vendor_efficiency AS
    SELECT 
        VendorID,
        count(*) as total_viajes,
        round(sum(total_amount), 2) as ingresos_totales,
        round(sum(trip_distance), 2) as distancia_total,
        -- Métrica clave: Ingreso por Milla
        round(sum(total_amount) / sum(trip_distance), 2) as ingreso_por_milla
    FROM main.gestion_silver.silver_yellow_trips
    WHERE trip_distance > 0 AND total_amount > 0
    GROUP BY 1
    ORDER BY ingreso_por_milla DESC;
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
        return "Tabla de Eficiencia de Proveedores creada en Gold."
    except Exception as e:
        raise Exception(f"Error en Analytics Vendor: {e}")
