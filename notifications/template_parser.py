"""Microservices Notification Module - Mail template parser."""
import os
from datetime import datetime

# Get an example path
template_path = os.path.join(os.path.dirname(__file__), 'templates', 'task.html')
file = open(template_path, 'r', encoding='UTF-8')

# Read the file content and store it in a variable
template_str = file.read()
file.close()

test_data = {
	'TITLE': 'Hello world',
	'OPEN_ACTION': '',
	# datetime(year, month, day, hour, minute, second, microsecond)
	'TARGET_DATE': datetime(2023, 6, 13, 11, 35, 0, 0),
  'OPEN_ACTION_DESC': 'Fix this soon!',
  'RESPONSABLE': 'Alfredo'
}

for key, value in test_data.items():
	value_to_replace = value

	# If it's a datetime, format it first
	if isinstance(value_to_replace, datetime):
		# To get: 2021-07-20 16:26:24
		value_to_replace = value.strftime("%Y-%m-%d %H:%M:%S")

	template_str = template_str.replace('{' + key + '}', value_to_replace)

print(template_str)
