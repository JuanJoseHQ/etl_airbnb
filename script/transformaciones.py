import os
import pandas as pd
from datetime import datetime
from logs_bi import Logs
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import ast
import unicodedata
import numpy as np
import nltk
from collections import Counter


class Transformaciones:
    def __init__(self):
        """
        Inicializa la clase de transformaciones con un sistema de logs.
        """
        # Crear log con fecha y hora
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        logs_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
        os.makedirs(logs_dir, exist_ok=True)
        log_filename = f"logs_{timestamp}.txt"
        self.log = Logs(os.path.join(logs_dir, log_filename))
        
        try:
            SentimentIntensityAnalyzer()
        except LookupError:
            nltk.download('vader_lexicon')

        self.log.info("[INIT] - Clase Transformaciones inicializada correctamente.")

    def transformaciones_listings(self, df):
        """
        Aplica transformaciones específicas a los datos de listings.
        """
        self.log.info("[START] - Iniciando proceso de transformaciones para 'listings'.")
        df_transformado = df.copy()
        self.log.info(f"[INFO] - Copia inicial creada. Total registros: {len(df_transformado)}.")

        # ==========================================================
        # Eliminación de duplicados
        # ==========================================================
        registros_iniciales = len(df_transformado)
        df_transformado = df_transformado.drop_duplicates()
        registros_finales = len(df_transformado)
        duplicados_eliminados = registros_iniciales - registros_finales
        self.log.info(
            f"[CLEAN] - Duplicados eliminados: {duplicados_eliminados}. "
            f"Total registros después de limpieza: {registros_finales}."
        )

        # ==========================================================
        # Limpieza de columnas de tasa ('%')
        # ==========================================================
        cols_rate = ['host_acceptance_rate', 'host_response_rate']
        for col in cols_rate:
            if col in df_transformado.columns:
                try:
                    df_transformado[col] = (
                        df_transformado[col]
                        .astype(str)
                        .str.replace('%', '', regex=False)
                        .astype(float)
                    )
                    self.log.info(
                        f"[CLEAN] - Columna '{col}' transformada: "
                        f"se eliminaron '%' y se convirtió a numérico. "
                        f"Valores ejemplo: {df_transformado[col].dropna().head(5).tolist()}"
                    )
                except Exception as e:
                    self.log.error(f"[CLEAN] - Error al limpiar columna '{col}': {type(e).__name__} - {e}")

        # ==========================================================
        # Categorización de host_response_time
        # ==========================================================
        response_time_map = {
            'within an hour': 'Fast',
            'within a few hours': 'Fast',
            'within a day': 'Moderate',
            'a few days or more': 'Slow'
        }

        if 'host_response_time' in df_transformado.columns:
            df_transformado['host_response_category'] = df_transformado['host_response_time'].map(response_time_map)
            df_transformado = df_transformado.drop(columns=['host_response_time'])
            self.log.info(
                "[TRANSFORM] - 'host_response_time' categorizado en 'host_response_category'. "
                "Se simplificó a categorías Fast, Moderate, Slow. "
                f"Valores únicos finales: {df_transformado['host_response_category'].unique().tolist()}"
            )

        # ==========================================================
        # Columnas binarias en 'host_verifications'
        # ==========================================================
        if 'host_verifications' in df_transformado.columns:
            df_transformado['host_verifications'] = df_transformado['host_verifications'].apply(
                lambda x: ast.literal_eval(x) if isinstance(x, str) else x
            )
            df_transformado['host_verifications'] = df_transformado['host_verifications'].apply(
                lambda x: x if isinstance(x, list) else []
            )
            df_verifications = df_transformado['host_verifications'].apply(
                lambda x: pd.Series(1, index=x)
            ).fillna(0).astype(int)
            df_verifications.columns = [f"verif_{col}" for col in df_verifications.columns]
            df_transformado = pd.concat([df_transformado, df_verifications], axis=1)
            df_transformado = df_transformado.drop(columns=['host_verifications'])
            self.log.info(
                "[TRANSFORM] - 'host_verifications' convertida en columnas binarias. "
                f"Columnas creadas: {df_verifications.columns.tolist()}. "
                f"Primeros registros transformados: \n{df_verifications.head(3)}"
            )

        # ==========================================================
        # Limpieza de columna 'price'
        # ==========================================================
        if 'price' in df_transformado.columns:
            df_transformado['price'] = (
                df_transformado['price']
                    .astype(str)
                    .str.replace('$', '', regex=False)
                    .str.replace(',', '', regex=False)
                    .str.replace('.', '', regex=False)
                    .astype(float)
            )
            self.log.info(
                "[CLEAN] - Columna 'price' limpiada: símbolos eliminados y convertida a numérico. "
                f"Valores ejemplo: {df_transformado['price'].head(5).tolist()}"
            )

        # ==========================================================
        # Limpieza de neighbourhood
        # ==========================================================
        if 'neighbourhood' in df_transformado.columns:
            def normalize_text(text):
                if pd.isnull(text):
                    return text
                text = text.lower()
                text = unicodedata.normalize('NFKD', text).encode('ascii', errors='ignore').decode('utf-8')
                return text.strip()
            df_transformado['neighbourhood'] = df_transformado['neighbourhood'].apply(normalize_text)
            self.log.info(
                "[TRANSFORM] - Columna 'neighbourhood' normalizada: "
                "minúsculas, sin acentos y sin espacios extra. "
                f"Valores ejemplo: {df_transformado['neighbourhood'].dropna().unique()[:5]}"
            )

        # ==========================================================
        # Normalización columna bathrooms
        # ==========================================================
        if 'bathrooms' in df_transformado.columns:
            df_transformado['bathrooms'] = np.ceil(df_transformado['bathrooms']).astype('Int64')
            self.log.info(
                "[TRANSFORM] - 'bathrooms' redondeada hacia arriba y convertida a Int64. "
                f"Valores ejemplo: {df_transformado['bathrooms'].head(5).tolist()}"
            )

        # ==========================================================
        # Eliminación de columnas irrelevantes
        # ==========================================================
        drop_cols = ['host_neighbourhood', 'neighborhood_overview', 'neighbourhood']
        cols_existentes = [c for c in drop_cols if c in df_transformado.columns]
        df_transformado = df_transformado.drop(columns=cols_existentes)
        self.log.info(
            f"[CLEAN] - Columnas eliminadas: {cols_existentes}. "
            f"Columnas restantes: {df_transformado.columns.tolist()}"
        )

        # ==========================================================
        # Eliminación de outliers
        # ==========================================================
        cols_outliers = ['bathrooms', 'bedrooms', 'beds']
        n_reg_before = len(df_transformado)
        filtro = (df_transformado[cols_outliers] > 15).any(axis=1)
        df_transformado = df_transformado[~filtro].copy()
        df_transformado = df_transformado[df_transformado['price'] < 400000000]
        n_reg_after = len(df_transformado)
        self.log.info(
            "[OUTLIERS] - Eliminación de valores extremos en 'bathrooms', 'bedrooms', 'beds' y 'price'. "
            f"Registros antes: {n_reg_before}, después: {n_reg_after}, eliminados: {n_reg_before - n_reg_after}."
        )
        
        # ==========================================================
        # Categorizacion de amentities
        # ==========================================================
        # Asegurar que la columna amenities sea tipo string
        df_transformado['amenities'] = df_transformado['amenities'].astype(str)

        # Dividir los amenities en listas
        df_transformado['amenities_list'] = df_transformado['amenities'].str.split(',')

        # Contar la frecuencia de cada amenity
        all_amenities = df_transformado['amenities_list'].explode()
        top_10_amenities = [amen.strip() for amen, _ in Counter(all_amenities).most_common(10)]

        # Crear columnas binarias para los 10 amenities más comunes
        for amen in top_10_amenities:
            col_name = f"amen_{amen.replace(' ', '_').lower()}"
            df_transformado[col_name] = df_transformado['amenities_list'].apply(lambda x: int(amen in [a.strip() for a in x]))
        
        self.log.info(
            "[TRANSFORM] - Columnas binarias creadas para los 10 amenities más comunes: "
            f"{[f'amen_{amen.replace(' ', '_').lower()}' for amen in top_10_amenities]}"
        )

        # Crear columna con el número total de amenities
        df_transformado['amenities_count'] = df_transformado['amenities_list'].apply(lambda x: len(x))
        self.log.info(
            "[TRANSFORM] - Columna 'amenities_count' creada con el conteo total de amenities por listing."
        )

        # ==========================================================
        # Finalización
        # ==========================================================
        self.log.info(f"[END] - Transformaciones completadas. Total final de registros: {len(df_transformado)}. Total columnas: {len(df_transformado.columns)}.")
        return df_transformado

    def transformaciones_calendar(self, df):
        """
        Aplica transformaciones específicas a los datos de listings.
        """
        self.log.info("[START] - Iniciando proceso de transformaciones para 'calendar'.")
        df_transformado = df.copy()
        self.log.info(f"[INFO] - Copia inicial creada. Total registros: {len(df_transformado)}.")
        
        # ==========================================================
        # Desagregación de fechas
        # ==========================================================
        # Convertir a datetime de forma segura antes de desagregar
        df_transformado['date'] = pd.to_datetime(df_transformado['date'], errors='coerce')

        df_transformado['year'] = df_transformado['date'].dt.year
        df_transformado['month'] = df_transformado['date'].dt.month
        df_transformado['day'] = df_transformado['date'].dt.day
        self.log.info(
            "[TRANSFORM] - Columna 'date' desagregada correctamente en componentes 'year', 'month' y 'day'"
            f"Ejemplo de valores: {df_transformado[['date', 'year', 'month', 'day']].head(3).to_dict(orient='records')}"
        )
        
        # ==========================================================
        # Eliminación de columnas irrelevantes
        # ==========================================================
        df_transformado = df_transformado.drop(columns=['minimum_nights', 'maximum_nights'])
        self.log.info(
            "[CLEAN] - Columnas 'minimum_nights' y 'maximum_nights' eliminadas por irrelevancia."
        )
        
        # ==========================================================
        # Finalización
        # ==========================================================
        self.log.info(f"[END] - Transformaciones completadas. Total final de registros: {len(df_transformado)}.Total columnas: {len(df_transformado.columns)}.")
        return df_transformado
    
    def transformaciones_reviews(self, df):
        """
        Aplica análisis de sentimiento y prepara los datos de reviews para la tabla de hechos.
        """
        self.log.info("[START] - Iniciando proceso de transformaciones para 'reviews'.")
        df_transformado = df.copy()
        
        # 1. Selección y limpieza inicial de columnas
        cols_needed = ['id', 'listing_id', 'date', 'reviewer_id', 'comments']
        df_transformado = df_transformado[cols_needed].copy()
        
        # Llenar nulos en 'comments' para evitar errores de análisis
        df_transformado['comments'] = df_transformado['comments'].fillna('')
        self.log.info(f"[CLEAN] - Nulos en 'comments' rellenados con cadena vacía. Total registros: {len(df_transformado)}.")
        
        # 2. Análisis de Sentimiento con VADER
        sia = SentimentIntensityAnalyzer()

        def get_sentiment(text):
            score = sia.polarity_scores(text)
            comp = score['compound']
            if comp >= 0.05:
                return 'Positivo'
            elif comp <= -0.05:
                return 'Negativo'
            else:
                return 'Neutral', comp

        # Aplicar el análisis de sentimiento y obtener la puntuación compuesta
        df_transformado['Sentimiento'] = df_transformado['comments'].apply(
            lambda x: 'Positivo' if sia.polarity_scores(x)['compound'] >= 0.05 else 
                      ('Negativo' if sia.polarity_scores(x)['compound'] <= -0.05 else 'Neutral')
        )
        df_transformado['Puntuacion_Compuesta'] = df_transformado['comments'].apply(
            lambda x: sia.polarity_scores(x)['compound']
        )
        self.log.info("[TRANSFORM] - Análisis de Sentimiento (VADER) completado.")
        
        # 3. Desagregación de Fechas (Requisito para BI)
        df_transformado['date'] = pd.to_datetime(df_transformado['date'], errors='coerce')
        
        # Eliminar filas con fechas no válidas si las hay (aunque no es común en reviews)
        registros_antes = len(df_transformado)
        df_transformado.dropna(subset=['date'], inplace=True)
        registros_despues = len(df_transformado)
        
        if registros_antes != registros_despues:
             self.log.info(f"[CLEAN] - Se eliminaron {registros_antes - registros_despues} registros con fecha nula después de la conversión.")
        
        df_transformado['review_year'] = df_transformado['date'].dt.year
        df_transformado['review_month'] = df_transformado['date'].dt.month
        
        self.log.info("[TRANSFORM] - Fechas desagregadas en 'review_year' y 'review_month'.")

        # 4. Eliminación de columna de texto para la carga SQL
        df_transformado = df_transformado.drop(columns=['comments'])
        
        # 5. Selección final de columnas (ordenación)
        final_cols = ['id', 'listing_id', 'reviewer_id', 'date', 
                      'review_year', 'review_month', 
                      'Sentimiento', 'Puntuacion_Compuesta']
        
        df_transformado = df_transformado[final_cols].copy()
        
        self.log.info(f"[END] - Transformaciones completadas. Total final de registros: {len(df_transformado)}. Total columnas: {len(df_transformado.columns)}.")
        
        return df_transformado