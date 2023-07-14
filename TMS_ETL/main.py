"""TMS_ETL"""
# Dev by E-Emmanuel_Parada@usiglobal.com

# Built-in modules
import os.path # File paths
import time # To get execution time
from datetime import datetime # Time management
import sys # System functions
import pprint # Better visualization for JSON

# External dependencies
import shutil # Tools for file management
import pandas as pd # Read Excel files
from dotenv import dotenv_values # Read .env files

# Project modules
import db
from logger import log, get_current_log, clean_current_log
from active_directory_api import get_all_users_in_domain
from mailing import send_email_notification
from full_name_splitter import split_name
from models.employee import Employee
from BOK.courses import validation_new_employees_or_update
import employee_changes

# ROW NAMES
ROW_CLAVE = 'CLAVE'
ROW_DEPARTMENT_DESCRIPTION = 'AREA'
ROW_PROJECT_NAME = 'BU'

# Start the timer
start_time = time.time()

def get_active_directory_code(code: str) -> str:
	"""Converts an int code to a str code to compare
	on the Active Directory."""
	prefix = 'MX'
	# Note: Pandas convert numbers to int, that's why we convert it to str
	return prefix + str(code).zfill(6)

def insert_in_db(row_as_dict: dict):
	'''Process a row from the XLSX file, and compare it to the DB'''
	code_with_prefix = get_active_directory_code(row_as_dict[ROW_CLAVE])

	# Process email
	row_as_dict['EMAIL'] = None
	# Check if they have an email
	if code_with_prefix in active_directory_users:
		email_address = active_directory_users[code_with_prefix]
		row_as_dict['EMAIL'] = email_address
		#print(f'Found user in AD: {code_with_prefix} - {email_address}')

	# Get full name
	first_name, last_name = split_name(row_as_dict['NOMBRE COMPLETO'])
	row_as_dict['FIRST_NAME'] = first_name
	row_as_dict['LAST_NAME'] = last_name

	# Get JobPositionId
	job_position_id = db.get_job_position_from_name(row_as_dict['DESC. PUESTO'])

	if job_position_id is None:
		log(f"WARNING - Job Position ID {row_as_dict['DESC. PUESTO']} doesn't exist")
		return

	row_as_dict['JOB_POSITION_ID'] = job_position_id
	#print(f"{row_as_dict['JOB_POSITION_ID']} - {row_as_dict['DESC. PUESTO']}")}

	row_as_dict['SUPERVISOR'], row_as_dict['SUPERVISOR_ID'] = \
		Employee.get_supervisor_id_from_str(row_as_dict['SUPERVISOR'])

	# If exist in the XLSX, but not in the DB. Create on the DB
	# If exist in the XLSX and the DB. Enable it
	# If doesn't exist in the XLSX and does in the DB. Disable it
	# (Look for registers not in the Excel)

	log(f"Employee code {row_as_dict['CLAVE']} doesn't exist. Creating it.")
	employee_id = db.insert_employee(row_as_dict)[0] # First cell from the row
	#print(f'Employee ID = {employee_id}')

	employee_obj = Employee()
	employee_obj.id = employee_id
	employee_obj.department_description = row_as_dict[ROW_DEPARTMENT_DESCRIPTION]
	employee_obj.project_name = row_as_dict[ROW_PROJECT_NAME]
	employee_obj.personal_dl1 = row_as_dict['PERSONAL_DL1']
	employee_obj.p_e = row_as_dict['P/E']
	employee_obj.education = row_as_dict['ESC.']
	employee_obj.gender = row_as_dict['SEXO']
	employee_obj.dob = row_as_dict['FECHA NAC.']
	employee_obj.degree = row_as_dict['GRADO']
	employee_obj.professional_cat_code = row_as_dict['PROFESSIONAL CAT CODE']
	employee_obj.cost_center = row_as_dict['COST CENTER']

	db.insert_employee_properties(employee_obj)

	return employee_id

def update_employee(employee: Employee):
	"""Received an Employee object, and updates all it's properties
	in the DB."""
	db.update_employee(employee)

def set_employee_enable_status(employee_codes_in_file: set):
	"""Check which employees are in the DB,
	and not in the Excel to	disable them."""
	employee_codes_in_db = db.get_all_employee_codes()

	employees_not_in_file = employee_codes_in_db - employee_codes_in_file
	employees_in_file_and_in_db = employee_codes_in_db & employee_codes_in_file
	#print('Employees in the DB, not in the XLSX')
	#print(employees_not_in_file)
	#print('Employees in the DB and in the XLSX')
	#print(employees_in_file_and_in_db)

	if len(employees_not_in_file) > 0:
		log(f'Disabling {len(employees_not_in_file)} employees')
		db.set_employee_enabled(tuple(employees_not_in_file), '0')

	if len(employees_in_file_and_in_db) > 0:
		log(f'Enabling {len(employees_in_file_and_in_db)} employees')
		db.set_employee_enabled(tuple(employees_in_file_and_in_db), '1')

if __name__ == '__main__':
	clean_current_log()
	log('Starting ETL script...')

	# Load environment variables from .env file
	#load_dotenv()

	SOURCE_FILE_PATH = dotenv_values().get('SOURCE_FILE_PATH')
	SOURCE_FILE_NAME = dotenv_values().get('SOURCE_FILE_NAME')
	PROCESSED_FILES_PATH = dotenv_values().get('PROCESSED_FILES_PATH')
	source_file_full_path = os.path.join(SOURCE_FILE_PATH, SOURCE_FILE_NAME)

	log(f'Loading source file: {source_file_full_path}')

	if not os.path.isfile(source_file_full_path):
		log("File doesn't exist - Exiting")
		sys.exit()

	# Add the date before the extension
	# HC.xlsx -> HC_2023_05_01-09_25_15.xlsx
	formatted_date = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")

	# Split the file name into its components (name and extension)
	source_file_path, ext = os.path.splitext(SOURCE_FILE_NAME)

	# Get the path for the copied file on the local storage
	processed_file_name = f'{source_file_path}_{formatted_date}'
	processed_file_name_with_ext = processed_file_name + ext
	processed_file_full_path = os.path.join(PROCESSED_FILES_PATH, processed_file_name_with_ext)
	#print(f'processed_file_full_path: {processed_file_full_path}')

	if not os.path.exists(PROCESSED_FILES_PATH):
		log(f"Processed files directory doesn't exist. Creating it on: {PROCESSED_FILES_PATH}")
		os.makedirs(PROCESSED_FILES_PATH)

	# Copy the file to the local storage
	shutil.copy2(source_file_full_path, processed_file_full_path)
	log(f'Source file copied to: {processed_file_full_path}')

	# Read the Excel source file
	log(f'Loading file: {processed_file_full_path}')
	df = pd.read_excel(processed_file_full_path)
	#print(df)
	p_p = pprint.PrettyPrinter(indent=2)

	# Get all users from Active Directory
	log('Loading all users in the Domain from Active Directory')
	active_directory_users = {}
	active_directory_users = get_all_users_in_domain()
	#p_p.pprint(active_directory_users)
	#if active_directory_users['status'] != 'ok':
	#	log('Error retreiving info from Active Directory API')

	log('Finished loading users from Active Directory')
	log('Getting a snapshot of users in the DB')

	expected_columns = [
		ROW_CLAVE, 'NOMBRE COMPLETO', 'DESC. PUESTO', ROW_DEPARTMENT_DESCRIPTION,
		ROW_PROJECT_NAME, 'PERSONAL_DL1', 'P/E', 'F. INGRESO', 'TURNO',
		'ESC.', 'CURP', 'SEXO', 'FECHA NAC.', 'ESCUELA', 'GRADO',
		'PROFESSIONAL CAT CODE', 'COST CENTER', 'SUPERVISOR',
	]
	column_names = list(df.columns)

	if expected_columns != column_names:
		log("WARNING: The source XLS doesn't have the right structure. - Exiting")
		sys.exit()

	# Get all current employees to determine the following scenarios
	# - Exists in XLSX but not on DB -> Insert as enabled
	# - Doesn't exist in XLSX but does in DB -> Disable it
	# - Are different in the XLSX and DB -> Update (refresh date)

	# Get all employees to define which registers to insert
	employees = db.get_all_employees_with_job_position()
	# for key, value in employees.items():
	# 	print(f'Employee Code: {key}\n{value}\n')

	# Get all departments from DB
	departments = db.get_departments_dict()

	# Get all job_descriptions from DB
	job_positions = db.get_job_positions_dict()

	log(f'Found {len(employees)} employees in the DB')
	log('Deciding what to insert, to disable and to update.')

	# Set enable status
	employee_codes_in_xlsx = set(df[ROW_CLAVE])
	set_employee_enable_status(employee_codes_in_xlsx)

	# Iterate through the XLSX rows
	for index, row in df.iterrows():
		row_dict = row.to_dict()
		for key in row_dict:
			if isinstance(row_dict[key], str):
				# Trim left and right white space
				row_dict[key] = row_dict[key].strip()

		EMPLOYEE_CODE = str(row_dict[ROW_CLAVE])
		#log(f'Debug: Checking employee code: {EMPLOYEE_CODE}')

		#changed_job_position = []
		#new_employees = []

		# Doesn't exist? -> Insert it
		if EMPLOYEE_CODE not in employees:
			new_employee_id = insert_in_db(row_dict)
			employee_changes.save_new_employee(new_employee_id)

		# Create an object without properties
		current_employee = Employee.from_row(row, departments, job_positions)
		employee_code_with_prefix = get_active_directory_code(current_employee.code)
		if employee_code_with_prefix in active_directory_users:
			current_employee.email = active_directory_users[employee_code_with_prefix]

		# The employee code (XLSX) is in the DB
		# If the employee is different, update it!
		if EMPLOYEE_CODE in employees:
			are_different = current_employee != employees[EMPLOYEE_CODE]

			if are_different:
				#print(current_employee)
				print('Employee in XLSX != Employee in DB')
				current_employee.enabled = 1
				current_employee.id = employees[EMPLOYEE_CODE].id
				log(current_employee.get_different_attributes(employees[EMPLOYEE_CODE]))
				log(f'Employee Id: {current_employee.id} Code: {EMPLOYEE_CODE} '
				'in XLSX is different than in the DB. Updated.')

				update_employee(current_employee)

				if current_employee.job_position_id != employees[EMPLOYEE_CODE].job_position_id:
					log(f'Employee Id: {current_employee.id} changed Job position')
					employee_changes.save_updated_employee(current_employee.id)

	# Send the updated and new employees to BOK
	log('Updating courses for new and employees changing position.')
	new_employees = employee_changes.get_new_employees()
	changed_job_position = employee_changes.get_employees_with_new_job()
	validation_new_employees_or_update(new_employees, changed_job_position)
	employee_changes.clean_employee_list()

	# Send a mail to inform the changes
	log('Sending a notification email')
	send_email_notification(get_current_log())
	clean_current_log()

	db.close_db_connection()

	end_time = time.time()
	execution_time = end_time - start_time
	log('ETL process finished')
	log(f'Process time: {execution_time:.2f} seconds')
	log('') # Save an empty line on the log
