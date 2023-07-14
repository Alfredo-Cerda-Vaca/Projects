"""TMS_ETL - Database Manager for Source Data."""
# Built-in modules
#from datetime import datetime
#import unicodedata

# External dependencies
import pyodbc # DB Connection

from dotenv import dotenv_values # Read .env files

DB_SOURCE_DB_DRIVER = dotenv_values().get('DB_SOURCE_DB_DRIVER')
DB_SOURCE_SERVER = dotenv_values().get('DB_SOURCE_SERVER')
DB_SOURCE_NAME = dotenv_values().get('DB_SOURCE_NAME')
DB_SOURCE_USERNAME = dotenv_values().get('DB_SOURCE_USERNAME')
DB_SOURCE_PASSWORD = dotenv_values().get('DB_SOURCE_PASSWORD')
DB_SOURCE_VIEW = dotenv_values().get('DB_SOURCE_VIEW')

conn = pyodbc.connect(
	f'DRIVER={DB_SOURCE_DB_DRIVER};SERVER={DB_SOURCE_SERVER};'
	f'DATABASE={DB_SOURCE_NAME};UID={DB_SOURCE_USERNAME};'
	f'PWD={DB_SOURCE_PASSWORD};Encrypt=no',
	autocommit=False
)

cursor = conn.cursor()

def close_db_connection():
	"""Close the connection at the end of the script."""
	conn.close()

def get_view_data():
	"""Get all the records from the Employees view."""
	query = f"""
		SELECT
			[CLAVE]
      ,[NOMBRE]
      ,[APELLIDO_P]
      ,[APELLIDO_M]
      ,[NOMBRE_COMPLETO]
      ,[ID_PUESTO]
      ,[DESCRIPCION_PUESTO]
      ,[ID_AREA]
      ,[AREA]
      ,[BU]
      ,[PERSONAL_DL1]
      ,[PE]
      ,[FECHA_INGRESO]
      ,[TURNO_ID]
      ,[TURNO_DESCR]
      ,[ESCOLARIDAD_ID]
      ,[ESCOLARIDAD_DESCR]
      ,[CURP]
      ,[SEXO]
      ,[FECHA_NACIMIENTO]
      ,[FECHA_PLANTA]
      ,[ESCUELA]
      ,[VIGENCIA]
      ,[FECHA_BAJA]
      ,[GRADO]
      ,[PROFESSIONAL_CAT_CODE]
      ,[COST_CENTER]
      ,[SUPERVISOR_NOMBRE]
      ,[SUPERVISOR_CLAVE]
		FROM {DB_SOURCE_NAME}.{DB_SOURCE_VIEW};"""
	cursor.execute(query)
	results = cursor.fetchall()

	return results
