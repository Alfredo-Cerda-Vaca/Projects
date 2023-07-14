"""TMS_ETL - Gets info from Active Directory API"""
# Built-in dependencies
import pprint # Better visualization for JSON

# External dependencies
from dotenv import dotenv_values, load_dotenv # Read .env files
import requests # HTTP Requests

load_dotenv() # Load environment variables from .env file

ACTIVE_DIRECTORY_BASE_URL = dotenv_values().get('ACTIVE_DIRECTORY_BASE_URL')
TIMEOUT_SECS = 60

def get_user_data(username: str):
	"""Returns a dict with user data, retrieved from the Active Directory API"""
	url = f'{ACTIVE_DIRECTORY_BASE_URL}/values/GetUsersGlobalByAccountName/{username}'
	response = requests.get(url, timeout=TIMEOUT_SECS)

	if response.status_code == 200: # If OK response
		data = response.json()
		p_p = pprint.PrettyPrinter(indent=2)
		p_p.pprint(data)
		return data

	print(f"Error: {response.status_code}")
	return 'Not found'

def get_all_users_in_domain() -> dict:
	"""Returns a dict with the code as a key (Like 'MX000000'),
	and values being the mail, retrieved from the Active Directory API"""
	# Note: The data source have the following examples
	# MX0|control_tower@usiglobal.com
	# MX026623, G5|eduardo_galvan@usiglobal.com
	# MX026681|juan_montiel@usiglobal.com
	# MX2200-0000000409|Rubinder_Chadda@usiglobal.com
	# EXTERNAL|E-Emmanuel_Parada@usiglobal.com

	# Remove the part after the ', '

	# Remove commas and spaces, and separated by |

	url = f'{ACTIVE_DIRECTORY_BASE_URL}/values/GetUsersGlobalToInsert'
	response = requests.get(url, timeout=TIMEOUT_SECS)

	if response.status_code == 200: # If OK response
		users_list = response.json()
		users_dict = {} # Store the users here
		#users_list = json.loads(data) # The json is a list []
		for user_str in users_list:
			# Remove spaces and commas
			str_parts = user_str.split('|')

			if len(str_parts) >= 2:
				part_1 = str_parts[0].split(', ')
				if len(part_1) >= 1:
					user_code = part_1[0]

					user_mail = str_parts[1]
					users_dict[user_code] = user_mail

		users_dict['status'] = 'OK'
		return users_dict
		#p_p = pprint.PrettyPrinter(indent=2)
		#p_p.pprint(data)

	print(f"Error: {response.status_code}")
	return {'status': 'Not available'}
