from pymongo import MongoClient, errors
import os
from logs_bi import Logs
from datetime import datetime
import pyodbc
import pandas as pd

# ===========================================================
# 🔹 Clase DatabaseMongo
# Maneja la conexión, extracción y cierre de datos desde MongoDB.
# ===========================================================
class DatabaseMongo:
    def __init__(self, uri="mongodb://localhost:27017/"):
        """
        Constructor de la clase DatabaseMongo.
        Inicializa la conexión con MongoDB y prepara el sistema de logs.
        """
        self.uri = uri
        self.client = None

        # Crear marca de tiempo (timestamp) para el archivo de logs
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Construir la ruta del archivo de log (../logs/logs_YYYYMMDD_HHMMSS.txt)
        logs_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
        log_filename = f"logs_{timestamp}.txt"
        log_path = os.path.join(logs_dir, log_filename)

        # Inicializar el sistema de logs
        self.log = Logs(log_path)
        self.log.info(f"[INIT] - Inicializando clase DatabaseMongo con URI: {self.uri}")

    def connect(self):
        """Establece la conexión con MongoDB."""
        try:
            self.log.info("[CONNECT] - Intentando conectar con MongoDB...")
            # Crear cliente de conexión con un tiempo de espera máximo de 3 segundos
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=3000)
            # Probar la conexión enviando un comando 'ping'
            self.client.admin.command("ping")
            self.log.info("[CONNECT] - Conexión exitosa con MongoDB.")
        except Exception as e:
            # Captura y registra cualquier error durante la conexión
            self.log.error(f"[CONNECT] - Error al conectar con MongoDB: {type(e).__name__} - {e}")

    def get_all(self, db_name, collection_name):
        """
        Obtiene todos los documentos de una colección específica.
        Parámetros:
        -----------
        db_name : str -> Nombre de la base de datos.
        collection_name : str -> Nombre de la colección.
        Retorna:
        --------
        list -> Lista con todos los documentos.
        """
        # Validar que exista una conexión activa
        if self.client is None:
            error_msg = "[GET_ALL] - No hay conexión activa. Llama primero a connect()."
            self.log.error(error_msg)
            raise Exception(error_msg)

        self.log.info(f"[GET_ALL] - Iniciando extracción de datos desde {db_name}.{collection_name}...")
        try:
            db = self.client[db_name]
            collection = db[collection_name]
            # Extraer todos los documentos de la colección
            data = list(collection.find())
            count = collection.count_documents({})
            self.log.info(f"[GET_ALL] - Se extrajeron {count} documentos de {db_name}.{collection_name} correctamente.")
            return data
        except Exception as e:
            # Registrar errores en caso de fallo
            self.log.error(f"[GET_ALL] - Error durante la extracción en {db_name}.{collection_name}: {type(e).__name__} - {e}")
            return []

    def get_range(self, db_name, collection_name, fecha_inicio, fecha_fin):
        """
        Obtiene documentos filtrando por un rango de fechas.
        Los documentos deben tener un campo 'date' en formato datetime.
        Parámetros:
        -----------
        db_name : str -> Nombre de la base de datos.
        collection_name : str -> Nombre de la colección.
        fecha_inicio : str -> Fecha inicial (YYYY-MM-DD).
        fecha_fin : str -> Fecha final (YYYY-MM-DD).
        Retorna:
        --------
        list -> Lista de documentos dentro del rango especificado.
        """
        # Verificar conexión activa
        if self.client is None:
            error_msg = "[GET_RANGE] - No hay conexión activa. Llama primero a connect()."
            self.log.error(error_msg)
            raise Exception(error_msg)

        try:
            self.log.info(f"[GET_RANGE] - Extrayendo datos desde {db_name}.{collection_name} entre {fecha_inicio} y {fecha_fin}...")
            db = self.client[db_name]
            collection = db[collection_name]

            # Convertir cadenas a objetos datetime
            fecha_inicio_dt = pd.to_datetime(fecha_inicio)
            fecha_fin_dt = pd.to_datetime(fecha_fin)

            # Construir la query MongoDB para el rango de fechas
            query = {
                "date": {
                    "$gte": fecha_inicio_dt,
                    "$lte": fecha_fin_dt
                }
            }

            # Ejecutar la consulta y transformar el cursor a lista
            data = list(collection.find(query))
            count = len(data)
            self.log.info(f"[GET_RANGE] - Documentos recuperados: {count}")

            # Validar si no se encontró información
            if not data:
                self.log.error(f"[GET_RANGE] - No hay datos para el rango indicado.")
                return []

            return data

        except Exception as e:
            # Manejo y registro de errores
            self.log.error(f"[GET_RANGE] - Error al extraer datos: {type(e).__name__} - {e}")
            return []

    def close(self):
        """Cierra la conexión con MongoDB."""
        if self.client:
            try:
                self.client.close()
                self.log.info("[CLOSE] - Conexión con MongoDB cerrada correctamente.")
            except Exception as e:
                self.log.error(f"[CLOSE] - Error al cerrar la conexión: {type(e).__name__} - {e}")
        else:
            self.log.info("[CLOSE] - No había conexión activa para cerrar.")


# ===========================================================
# 🔹 Clase DatabaseSQL
# Administra la conexión y carga de datos hacia Azure SQL Database.
# ===========================================================
class DatabaseSQL:
    def __init__(self, server, database, username, password, driver="{ODBC Driver 18 for SQL Server}"):
        """
        Constructor de la clase DatabaseSQL.
        Configura los parámetros necesarios para conectar a Azure SQL.
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        self.conn = None

        # Crear logs con timestamp único
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        logs_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
        os.makedirs(logs_dir, exist_ok=True)
        log_filename = f"logs_{timestamp}.txt"
        log_path = os.path.join(logs_dir, log_filename)
        self.log = Logs(log_path)

    def connect(self):
        """Establece la conexión con Azure SQL utilizando pyodbc."""
        try:
            # Construir cadena de conexión segura a Azure SQL
            conn_str = (
                f"DRIVER={self.driver};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                "Encrypt=yes;"
                "TrustServerCertificate=no;"
                "Connection Timeout=30;"
            )
            self.conn = pyodbc.connect(conn_str)
            self.log.info(f"|SQL AZURE| - Conexión exitosa a la base de datos '{self.database}' en el servidor '{self.server}'.")
        except Exception as e:
            self.log.error(f"|SQL AZURE| - Error al conectar a Azure SQL: {repr(e)}")

    def _table_exists(self, table_name, schema="dbo"):
        """Verifica si una tabla existe dentro del esquema especificado."""
        try:
            cursor = self.conn.cursor()
            query = f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema}' 
                AND TABLE_NAME = '{table_name}'
            """
            cursor.execute(query)
            exists = cursor.fetchone()[0] > 0
            cursor.close()
            return exists
        except Exception as e:
            self.log.error(f"|SQL AZURE| - Error al verificar existencia de tabla '{schema}.{table_name}': {repr(e)}")
            return False

    def _create_table_from_df(self, df, table_name, schema="dbo"):
        """
        Crea una tabla nueva basada en las columnas del DataFrame.
        Por defecto, todas las columnas se crean como NVARCHAR(MAX).
        """
        try:
            cursor = self.conn.cursor()
            cols = []
            for col, dtype in df.dtypes.items():
                # Se define el tipo NVARCHAR(MAX) para mayor flexibilidad
                sql_type = "NVARCHAR(MAX)"
                cols.append(f"[{col}] {sql_type}")

            create_query = f"CREATE TABLE {schema}.{table_name} ({', '.join(cols)})"
            cursor.execute(create_query)
            self.conn.commit()
            cursor.close()

            self.log.info(f"|SQL AZURE| - Tabla '{schema}.{table_name}' creada correctamente con {len(df.columns)} columnas.")
        except Exception as e:
            self.log.error(f"|SQL AZURE| - Error al crear la tabla '{schema}.{table_name}': {repr(e)}")

    def overwrite_table(self, df, table_name, schema="dbo"):
        """
        Sobrescribe completamente una tabla SQL:
        - Si no existe, la crea (usa estructura especial si es 'reviews').
        - Si existe, elimina los registros existentes.
        - Luego inserta todos los datos del DataFrame.
        """
        if self.conn is None:
            self.log.error("|SQL AZURE| - No hay conexión activa. Usa connect() primero.")
            return

        cursor = self.conn.cursor()
        try:
            # Verificar existencia de tabla
            if not self._table_exists(table_name, schema):
                self.log.info(f"|SQL AZURE| - La tabla '{schema}.{table_name}' no existe. Se procederá a crearla.")
                
                # Usar método optimizado si la tabla es 'reviews'
                if table_name == "reviews":
                    self.log.info(f"|SQL AZURE| - Usando esquema optimizado (Reviews) para '{table_name}'.")
                    self._create_table_from_df_review(df, table_name, schema)
                else:
                    self.log.info(f"|SQL AZURE| - Usando esquema genérico para '{table_name}'.")
                    self._create_table_from_df(df, table_name, schema)
                    
            else:
                # Vaciar tabla existente
                self.log.info(f"|SQL AZURE| - La tabla '{schema}.{table_name}' ya existe. Eliminando registros...")
                cursor.execute(f"DELETE FROM {schema}.{table_name}")
                self.conn.commit()

            # Insertar nuevos datos en la tabla
            self.log.info(f"|SQL AZURE| - Insertando {len(df)} registros en {schema}.{table_name}...")
            columns = ", ".join([f"[{c}]" for c in df.columns])
            placeholders = ", ".join(["?"] * len(df.columns))
            insert_query = f"INSERT INTO {schema}.{table_name} ({columns}) VALUES ({placeholders})"

            # Optimización para carga masiva
            cursor.fast_executemany = True
            cursor.executemany(insert_query, df.astype(str).values.tolist()) 
            self.conn.commit()

            self.log.info(f"|SQL AZURE| - Tabla '{schema}.{table_name}' sobrescrita correctamente con {len(df)} registros.")

        except Exception as e:
            # Registrar y mostrar errores
            self.log.error(f"|SQL AZURE| - Error al sobrescribir la tabla '{schema}.{table_name}': {repr(e)}")
            print(f"Error al sobrescribir '{schema}.{table_name}'. Ver logs para más detalles.")

        finally:
            cursor.close()

    def _create_table_from_df_review(self, df, table_name, schema="dbo"):
        """
        Crea una tabla 'reviews' con tipos de datos optimizados
        (clave primaria, tipos INT/FLOAT/DATE/NVARCHAR según el contenido).
        """
        try:
            cursor = self.conn.cursor()
            cols = []
            
            # Asignar tipos de datos según el nombre o tipo de la columna
            for col, dtype in df.dtypes.items():
                col_name = col.replace('.', '_')
                sql_type = "NVARCHAR(50)"  # Valor por defecto

                # Definiciones específicas por columna
                if col_name == 'id':
                    sql_type = "NVARCHAR(50) NOT NULL PRIMARY KEY"
                elif col_name in ['listing_id', 'reviewer_id']:
                    sql_type = "NVARCHAR(50) NOT NULL"
                elif 'year' in col_name or 'month' in col_name:
                    sql_type = "INT"
                elif pd.api.types.is_float_dtype(dtype):
                    sql_type = "FLOAT"
                elif 'date' in col_name:
                    sql_type = "DATE" 
                elif col_name == 'Sentimiento':
                    sql_type = "NVARCHAR(20)"
                
                cols.append(f"[{col_name}] {sql_type}")

            create_query = f"CREATE TABLE {schema}.{table_name} ({', '.join(cols)})"
            cursor.execute(create_query)
            self.conn.commit()
            cursor.close()

            self.log.info(f"|SQL AZURE| - Tabla '{schema}.{table_name}' creada correctamente con tipos de datos optimizados y clave primaria.")
        except Exception as e:
            self.log.error(f"|SQL AZURE| - Error al crear la tabla '{schema}.{table_name}': {repr(e)}")

    def close(self):
        """Cierra la conexión activa con Azure SQL."""
        if self.conn:
            self.conn.close()
            self.log.info("|SQL AZURE| - Conexión cerrada correctamente.")
