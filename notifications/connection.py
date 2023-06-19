"""Connection with DB"""
# Built-in modules
import pyodbc #Driver per SQL
from dotenv import dotenv_values, load_dotenv # Read .env filest

load_dotenv() ## Information FROM SQL .env

DB_DRIVER = dotenv_values().get('DB_DRIVER')
DB_SERVER = dotenv_values().get('DB_SERVER')
DB_NAME = dotenv_values().get('DB_NAME')
DB_USERNAME = dotenv_values().get('DB_USERNAME')
DB_PASSWORD = dotenv_values().get('DB_PASSWORD')

##Table
DB_TABLE_TEMPLATE_EMAIL = dotenv_values().get('DB_TABLE_TEMPLATE_EMAIL')

## Config SQL

conn = pyodbc.connect(
	f'DRIVER={DB_DRIVER};SERVER={DB_SERVER};'
	f'DATABASE={DB_NAME};UID={DB_USERNAME};'
	f'PWD={DB_PASSWORD};Encrypt=no',
	autocommit=False
)
cursor = conn.cursor()

def close_db_connection():
	"""Close the connection at the end of the script."""
	cursor.close()
def choose_name_template(information_email: list) -> str:
	"""get the body FROM SQL server"""
	try:
		result = cursor.execute(f'''
		SELECT Body FROM [{DB_NAME}].{DB_TABLE_TEMPLATE_EMAIL} WHERE Name = '{information_email['template_name']}'
		''').fetchone()
		for key, value in information_email['body'].items():
			result[0] = result[0].replace('{' + key + '}', value)
	except ConnectionError as ex:
		print(f'Error en la conexion: {ex}')
	except KeyError as ex:
		print(f'Error en la conexion: {ex}')

	return result[0]
