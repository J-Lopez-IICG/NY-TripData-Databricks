import os
from dotenv import load_dotenv
from databricks import sql

# 1. Cargamos tu archivo .env (que sigue intacto)
load_dotenv()

host = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_TOKEN")
warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")

try:
    print("Conectando al SQL Warehouse Serverless...")

    # Limpiamos el host por si tiene el "https://"
    if host and host.startswith("https://"):
        host = host.replace("https://", "")

    # 2. Conexión nativa con el conector SQL de Databricks
    connection = sql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        access_token=token,
    )

    cursor = connection.cursor()
    print("¡CONEXIÓN ESTABLECIDA CON ÉXITO!")

    # 3. Le pedimos al servidor que procese una consulta mínima
    cursor.execute("SELECT 1 AS prueba_de_vida")
    result = cursor.fetchall()

    print(f"El servidor respondió: {result}")

    # Cerramos la puerta al salir
    cursor.close()
    connection.close()

except Exception as e:
    print(f"\nError de conexión:\n{e}")
