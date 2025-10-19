# ETL Airbnb

## Integrantes y Responsabilidades

- Juan José Herrera Quinchia – Encargado del análisis de cada dataset y la orquestación de todas las clases, además de documentación del proyecto.

- Sebastián Rodríguez Echeverri – Desarrollo de scripts de extracción, transformación y carga (ETL) y documentación del proyecto.

Nota: Este proyecto fue un esfuerzo colaborativo constante entre ambos. Las tareas indicadas reflejan una mayor responsabilidad puntual, pero ambos participaron activamente en cada decisión y desarrollo del proyecto.



## Descripción del proyecto
Este proyecto consiste en un proceso de **ETL (Extracción, Transformación y Carga)** aplicado a datos de Airbnb.  
Se extraen colecciones desde una base de datos **MongoDB local**, se realizan transformaciones y limpieza de los datos, y finalmente se cargan en una base de datos **SQL Server en Azure**.  

### Objetivo
El objetivo del proyecto es generar una **sábana de datos consolidada** que permita realizar análisis descriptivos, predictivos y de comportamiento, incluyendo:  
- Limpieza y normalización de datos de listings, calendar y reviews.  
- Análisis de sentimientos en los comentarios de los usuarios.  
- Creación de columnas binarias y conteos para atributos como amenities y verificaciones de anfitriones.  
- Preparación de la información para análisis de BI y posibles modelos de forecasting o segmentación.

---

## Instrucciones de instalación

### 1. Clonar el repositorio
```bash
git clone https://github.com/usuario/etl_airbnb.git
cd etl_airbnb
```
### 2. Crear Entorno Virutal
```bash
python -m venv venv
```
### 3. Activar Entorno Virutal
```bash
venv\Scripts\activate
```
### 4. Instalar Dependencias
```bash
pip install -r requirements.txt
```
### Dependencias principales

- pandas
- numpy
- nltk
- textblob
- langdetect
- pymongo
- pyodbc
- python-dotenv
- matplotlib
- seaborn

### 5.Configurar variables de entorno 
Crear un archivo .env en la carpeta de scrips con las siguientes variables:
```dotenc
URL="mongodb://localhost:27017/" -->Url Conexion a mongo
SQLDATABASE=""
USER=""
PASSWORD=""
```
### 6.Ejecutar ETL
Crear un archivo .env en la carpeta de scrips con las siguientes variables:
Antes de ejecutarlo debe intanciar su mongo propia y crear un schema llamado bi_mx y 3 colecciones con el nombre de:
- listings: https://data.insideairbnb.com/mexico/df/mexico-city/2025-06-25/data/listings.csv.gz
- calendar: https://data.insideairbnb.com/mexico/df/mexico-city/2025-06-25/data/calendar.csv.gz
- reviews: https://data.insideairbnb.com/mexico/df/mexico-city/2025-06-25/data/reviews.csv.gz
```bash
cd etl_airbnb/script
python main.py
```

## Ejemplo de Ejecución del ETL

Al ejecutar el script principal main.py, se realizan los siguientes pasos:
### 1. Extracción de datos

- Conexión a MongoDB local y extracción de las colecciones listings, calendar y reviews.

- Los datasets extraídos se guardan inicialmente en formato .csv en la carpeta data/raw.

### 2. Transformación de los datasets

Normalización de columnas y tipos de datos.

- Limpieza de valores faltantes, inconsistentes o duplicados.

- Creación de columnas adicionales útiles para análisis de BI, como:

- Conteo de amenities por listing.

- Análisis de sentimiento de los comentarios de los usuarios (reviews).

### 3. Guardado de datos transformados

- Los datasets procesados se exportan a formato .xlsx local para su revisión o uso posterior.

### 3.1. Carga en SQL Server (opcional)

- Los datasets transformados pueden cargarse en SQL Server Azure para análisis y reporting.

- Esta parte puede fallar si no se tienen las credenciales o permisos necesarios para acceder a la base de datos.

### 4. Generación de logs

- Durante todo el proceso se genera un registro detallado de cada paso del ETL en la carpeta logs, permitiendo auditoría y seguimiento del flujo de datos.
