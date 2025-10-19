# =============================================================================
# Script principal de ETL (Extracción, Transformación y Carga)
# =============================================================================

from database import DatabaseMongo, DatabaseSQL
from extracciones import Extracciones
from carga import Cargas
from transformaciones import Transformaciones
import pandas as pd
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Obtener variables de entorno
MONGO_URI = os.getenv("URL")
SQL_DATABASE = os.getenv("SQLDATABASE")
SQL_USER = os.getenv("USER")
SQL_PASSWORD = os.getenv("PASSWORD")
SERVER = os.getenv("SERVER")

if __name__ == "__main__":

    # =========================================================================
    # EXTRACCIONES
    # =========================================================================
    mongo = DatabaseMongo(uri=MONGO_URI)
    mongo.connect()
    extr = Extracciones(mongo)
    df = extr.extraer_coleccion("bi_mx", "listings")
    df = extr.extraer_calendar_rango_mongo("bi_mx", "calendar", "2025-06-26", "2025-06-26")
    df_reviews = extr.extraer_coleccion("bi_mx", "reviews")
    mongo.close()

    # =========================================================================
    # TRANSFORMACIONES
    # =========================================================================
    transf = Transformaciones()

    base_dir = os.path.dirname(__file__) 
    parent_dir = os.path.dirname(base_dir)  
    raw_dir = os.path.join(parent_dir, "data", "raw") 

    # Listings
    df_listings = pd.read_csv(os.path.join(raw_dir, "listings.csv"), sep=",", encoding="utf-8-sig")
    df_listings_transf = transf.transformaciones_listings(df_listings)

    # Calendar
    df_calendar = pd.read_csv(os.path.join(raw_dir, "calendar.csv"), sep=",", encoding="utf-8-sig")
    df_calendar_transf = transf.transformaciones_calendar(df_calendar)

    # Reviews
    df_reviews = pd.read_csv(os.path.join(raw_dir, "reviews.csv"), sep=",", encoding="utf-8-sig")
    df_reviews['date'] = pd.to_datetime(df_reviews['date'], errors='coerce')
    df_reviews_filtrado = df_reviews[
        (df_reviews['date'] >= "2016-01-01") & (df_reviews['date'] <= "2016-05-30")
    ]
    df_reviews_transf = transf.transformaciones_reviews(df_reviews_filtrado)

    # =========================================================================
    # CARGAS
    # =========================================================================
    carg = Cargas()
    carg.cargar_silver(df_listings_transf, "listings")
    carg.cargar_silver(df_calendar_transf, "calendar")
    carg.cargar_silver(df_reviews_transf, "reviews")

    # SQL Server
    sql = DatabaseSQL(
        server=SERVER,
        database=SQL_DATABASE,
        username=SQL_USER,
        password=SQL_PASSWORD
    )
    carg.cargar_sql(df_listings_transf, "silver_listings", "dbo", sql)
    carg.cargar_sql(df_calendar_transf, "silver_calendar", "dbo", sql)
    carg.cargar_sql(df_reviews_transf, "silver_reviews", "dbo", sql)

