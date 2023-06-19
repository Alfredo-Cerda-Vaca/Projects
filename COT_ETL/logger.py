"""Simple logger"""
import os
from datetime import datetime
from dotenv import dotenv_values, load_dotenv

load_dotenv()
LOG_PATH = dotenv_values().get('LOG_PATH')
LOG_ARCHIVE_FILE_NAME = dotenv_values().get('LOG_ARCHIVE_FILE_NAME')
LOG_CURRENT_PROCESS_FILE_NAME = dotenv_values().get('LOG_CURRENT_PROCESS_FILE_NAME')

full_log_path = os.path.join(LOG_PATH, LOG_ARCHIVE_FILE_NAME)
current_log_path = os.path.join(LOG_PATH, LOG_CURRENT_PROCESS_FILE_NAME)

def log(string_to_log: str = ''):
	"""Creates a new entry in the log file."""
	# TODO: Process when the directory doesn't exist
	# or the log file is not writeable

	with open(full_log_path, 'a', encoding='UTF-8') as file:
		# Example: 2023-05-05 11:53:10
		formatted_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

		string_to_log = f'{formatted_date} - {string_to_log}\n'
		file.write(string_to_log)
		print(string_to_log[:-1]) # Remove the trailing \n

	with open(current_log_path, 'a', encoding='UTF-8') as file:
		file.write(string_to_log)

def clean_current_log():
	"""Clean a previous log for current run."""
	if os.path.isfile(current_log_path):
		os.remove(current_log_path)

def get_current_log() -> str:
	"""Reads the current log from a file and return it."""
	with open(current_log_path, 'r', encoding='UTF-8') as file:
		content = file.read() # Why as a variable? Wasn't returning alone
		return content
