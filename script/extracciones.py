import os
import pandas as pd
from datetime import datetime
from logs_bi import Logs


class Extracciones:
    """
    Clase responsable de la extracción de datos desde una base de datos MongoDB
    y su posterior exportación a archivos CSV dentro del entorno del proyecto.

    Permite realizar extracciones completas de colecciones o dentro de rangos de fechas.
    Incluye registro de eventos (logs) para seguimiento de procesos.
    """

    def __init__(self, mongo_instance):
        """
        Constructor de la clase Extracciones.

        Parámetros:
        -----------
        mongo_instance : DatabaseMongo
            Instancia de conexión a MongoDB ya establecida.

        Acciones:
        ---------
        - Inicializa el logger con un archivo único por ejecución.
        - Guarda la referencia a la conexión de MongoDB.
        """
        self.mongo = mongo_instance

        # Crear archivo de log con timestamp único
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        logs_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
        os.makedirs(logs_dir, exist_ok=True)
        log_filename = f"logs_{timestamp}.txt"
        self.log = Logs(os.path.join(logs_dir, log_filename))

        self.log.info("[INIT] - Clase Extracciones inicializada correctamente.")

    # ----------------------------------------------------------
    # MÉTODO 1: Extracción completa de una colección MongoDB
    # ----------------------------------------------------------
    def extraer_coleccion(self, db_name, collection_name):
        """
        Extrae todos los documentos de una colección MongoDB y los convierte en un DataFrame.
        Luego los exporta como archivo CSV en la carpeta `data/raw`.

        Parámetros:
        -----------
        db_name : str
            Nombre de la base de datos de MongoDB.
        collection_name : str
            Nombre de la colección a extraer.

        Retorna:
        --------
        pd.DataFrame o None
            DataFrame con los datos extraídos o None si la colección está vacía.
        """
        self.log.info(f"[EXTRACT] - Iniciando extracción para {db_name}.{collection_name}...")

        try:
            # Recuperar todos los documentos de la colección
            data = self.mongo.get_all(db_name, collection_name)
            self.log.info(f"[EXTRACT] - Datos obtenidos desde MongoDB: {len(data)} registros recuperados.")

            # Si no hay datos, se interrumpe el proceso
            if not data:
                self.log.info(f"[EXTRACT] - La colección '{db_name}.{collection_name}' está vacía. No se generará CSV.")
                return None

            # Convertir los datos en un DataFrame de pandas
            df = pd.DataFrame(data)
            self.log.info(f"[TRANSFORM] - Conversión a DataFrame completada ({df.shape[0]} filas, {df.shape[1]} columnas).")
            
            # Asegurar que todas las columnas no numéricas sean tipo string
            for col in df.columns:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].astype(str)

            # Crear carpeta de salida si no existe
            csv_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
            os.makedirs(csv_dir, exist_ok=True)

            # Definir nombre y ruta del archivo CSV
            csv_filename = f"{collection_name}.csv"
            csv_path = os.path.join(csv_dir, csv_filename)
            
            # Guardar el DataFrame como CSV
            df.to_csv(csv_path, index=False)

            # Registrar éxito del proceso (no guarda el CSV en este método)
            self.log.info(f"[LOAD] - CSV generado exitosamente en: {csv_path}")
            self.log.info(f"[LOAD] - Total de registros exportados: {len(df)}")

            return df

        except Exception as e:
            # Captura y registro de errores durante la extracción
            self.log.error(f"[EXTRACT] - Error al procesar '{db_name}.{collection_name}': {type(e).__name__} - {e}")
            return None

    # ----------------------------------------------------------
    # MÉTODO 2: Extracción por rango de fechas desde MongoDB
    # ----------------------------------------------------------
    def extraer_calendar_rango_mongo(self, db_name, collection_name, fecha_inicio, fecha_fin):
        """
        Extrae documentos de MongoDB dentro de un rango de fechas específico,
        los convierte en un DataFrame y los exporta a un archivo CSV.

        Parámetros:
        -----------
        db_name : str
            Nombre de la base de datos en MongoDB.
        collection_name : str
            Nombre de la colección a consultar.
        fecha_inicio : str
            Fecha inicial en formato 'YYYY-MM-DD'.
        fecha_fin : str
            Fecha final en formato 'YYYY-MM-DD'.

        Retorna:
        --------
        pd.DataFrame o None
            DataFrame con los datos extraídos o None si no se encontraron registros.
        """
        self.log.info(f"[EXTRACT] - Iniciando extracción de {db_name}.{collection_name} entre {fecha_inicio} y {fecha_fin}...")

        try:
            # Obtener documentos que se encuentren dentro del rango de fechas
            data = self.mongo.get_range(db_name, collection_name, fecha_inicio, fecha_fin)
            if not data:
                self.log.info(f"[EXTRACT] - No se encontraron datos en '{db_name}.{collection_name}' para el rango indicado.")
                return None
            self.log.info(f"[EXTRACT] - {len(data)} registros recuperados desde MongoDB.")

            # Convertir los documentos a un DataFrame de pandas
            df = pd.DataFrame(data)
            self.log.info(f"[TRANSFORM] - DataFrame generado ({df.shape[0]} filas x {df.shape[1]} columnas).")
            
            # Convertir las columnas no numéricas a tipo string
            for col in df.columns:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].astype(str)

            # Crear la carpeta de salida si no existe
            csv_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
            os.makedirs(csv_dir, exist_ok=True)

            # Definir la ruta del archivo CSV a generar
            csv_path = os.path.join(csv_dir, f"{collection_name}.csv")

            # Exportar el DataFrame como archivo CSV
            df.to_csv(csv_path, index=False)

            # Registrar información de éxito
            self.log.info(f"[LOAD] - CSV generado exitosamente: {csv_path}")
            self.log.info(f"[LOAD] - Total de registros exportados: {len(df)}")

            return df

        except Exception as e:
            # Captura de excepciones y registro de errores
            self.log.error(f"[EXTRACT] - Error al procesar '{db_name}.{collection_name}': {type(e).__name__} - {e}")
            return None

           
