import os
from database import DatabaseSQL
from datetime import datetime
from logs_bi import Logs
import pandas as pd

class Cargas:
    def __init__(self):
        """
        Constructor de la clase Cargas.
        Inicializa un objeto para manejar cargas de datos y crea un archivo de log 
        único basado en la fecha y hora de ejecución.
        """
        # Generar marca de tiempo para el nombre del log (formato: AAAAMMDD_HHMMSS)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Definir el directorio donde se guardarán los logs (../logs)
        logs_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
        os.makedirs(logs_dir, exist_ok=True)  # Crear carpeta si no existe
        
        # Crear nombre de archivo de log con la marca de tiempo
        log_filename = f"logs_{timestamp}.txt"
        
        # Inicializar el objeto Logs, indicando la ruta completa del archivo
        self.log = Logs(os.path.join(logs_dir, log_filename))

        # Registrar mensaje informativo en el log
        self.log.info("[INIT] - Clase Cargas inicializada correctamente.")
    
    def cargar_silver(self, df, file_name):
        """
        Guarda un DataFrame en formato .xlsx dentro de la carpeta 'silver'.
        
        Parámetros:
        df (pd.DataFrame): DataFrame a guardar.
        file_name (str): Nombre del archivo (sin extensión).
        """
        try:
            # Log de inicio del proceso de carga
            self.log.info(f"[LOAD] - Iniciando carga del archivo {file_name}.xlsx en carpeta silver...")

            # Obtener ruta base del script actual
            base_dir = os.path.dirname(__file__)
            # Obtener el directorio padre (nivel superior al script actual)
            parent_dir = os.path.dirname(base_dir)
            # Construir la ruta hacia la carpeta 'silver'
            silver_dir = os.path.join(parent_dir, "data", "silver")
            # Definir la ruta completa del archivo a guardar
            xlsx_path = os.path.join(silver_dir, f"{file_name}.xlsx")

            # Guardar el DataFrame como archivo Excel (.xlsx)
            df.to_excel(xlsx_path, index=False, engine='openpyxl')

            # Log de éxito
            self.log.info(f"[LOAD] - Archivo {file_name}.xlsx guardado exitosamente en {xlsx_path}.")
        
        except Exception as e:
            # En caso de error, registrar el mensaje en el log
            self.log.error(f"[ERROR] - Error al guardar el archivo {file_name}.xlsx: {str(e)}")
        
    def cargar_sql(self, df, name, schema, instance):
        """
        Carga un DataFrame a una base de datos SQL, sobrescribiendo la tabla destino.
        
        Parámetros:
        df (pd.DataFrame): DataFrame a insertar.
        name (str): Nombre de la tabla destino.
        schema (str): Esquema de base de datos.
        instance (DatabaseSQL): Instancia de conexión a base de datos (clase DatabaseSQL).
        """
        # Abrir conexión a la base de datos
        instance.connect()

        # Sobrescribir la tabla con los nuevos datos del DataFrame
        instance.overwrite_table(df, table_name=name, schema=schema)

        # Cerrar la conexión a la base de datos
        instance.close()

