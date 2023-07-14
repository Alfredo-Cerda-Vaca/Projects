"""Stores lists of new and updated employees."""
import os
from dotenv import dotenv_values

EMPLOYEES_PATH = dotenv_values().get('EMPLOYEES_PATH')
NEW_EMPLOYEES_FILE_NAME = dotenv_values().get('NEW_EMPLOYEES_FILE_NAME')
EMPLOYEES_WITH_NEW_JOB_FILE_NAME = dotenv_values().get('EMPLOYEES_WITH_NEW_JOB_FILE_NAME')

new_employees_path = os.path.join(EMPLOYEES_PATH, NEW_EMPLOYEES_FILE_NAME)
employees_with_new_job_path = os.path.join(EMPLOYEES_PATH, EMPLOYEES_WITH_NEW_JOB_FILE_NAME)

def save_new_employee(employee_code: int):
	"""Creates a new entry in the log file."""
	with open(new_employees_path, 'a', encoding='UTF-8') as file:
		file.write(str(employee_code) + "\n")

def save_updated_employee(employee_code: int):
	"""Creates a new entry in the log file."""
	with open(employees_with_new_job_path, 'a', encoding='UTF-8') as file:
		file.write(str(employee_code) + "\n")

def get_new_employees() -> list[int]:
	"""Reads the current log from a file and return it."""
	if os.path.isfile(new_employees_path):
		with open(new_employees_path, 'r', encoding='UTF-8') as file:
			return [int(line.rstrip()) for line in file]

	return []

def get_employees_with_new_job() -> list[int]:
	"""Reads the current log from a file and return it."""
	if os.path.isfile(employees_with_new_job_path):
		with open(employees_with_new_job_path, 'r', encoding='UTF-8') as file:
			return [int(line.rstrip()) for line in file]

	return []

def clean_employee_list():
	"""Removes the files to start over!."""
	if os.path.isfile(new_employees_path):
		os.remove(new_employees_path)
	if os.path.isfile(employees_with_new_job_path):
		os.remove(employees_with_new_job_path)
