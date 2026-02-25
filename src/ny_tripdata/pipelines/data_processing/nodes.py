import os
from databricks import sql
from dotenv import load_dotenv


def create_silver_table() -> str:
    load_dotenv()
    host = os.getenv("DATABRICKS_HOST", "").replace("https://", "")
    token = os.getenv("DATABRICKS_TOKEN")
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")

    # Definimos el query para pasar TODO a Silver con tipos correctos
    query = """
        CREATE OR REPLACE TABLE main.gestion_silver.silver_yellow_trips AS
        SELECT 
            VendorID,q
            try_cast(tpep_pickup_datetime AS TIMESTAMP) AS tpep_pickup_datetime,
            try_cast(tpep_dropoff_datetime AS TIMESTAMP) AS tpep_dropoff_datetime,
            passenger_count,
            trip_distance,
            pickup_longitude,
            pickup_latitude,
            RatecodeID,
            store_and_fwd_flag,
            dropoff_longitude,
            dropoff_latitude,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount
        FROM read_files(
            '/Volumes/main/gestion_raw/raw_files_volume/yellow_trip_part_*',
            format => 'csv',
            header => 'true',
            schema => 'VendorID string, tpep_pickup_datetime string, tpep_dropoff_datetime string, passenger_count double, trip_distance double, pickup_longitude double, pickup_latitude string, RatecodeID string, store_and_fwd_flag string, dropoff_longitude double, dropoff_latitude double, payment_type double, fare_amount double, extra double, mta_tax double, tip_amount double, tolls_amount double, improvement_surcharge double, total_amount double'
        )
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
        return "Tabla Silver (Completa) creada con éxito"
    except Exception as e:
        raise Exception(f"Error en Capa Silver: {e}")
