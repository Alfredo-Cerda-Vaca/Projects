"""Employee object for ORM."""
# Built-in modules
import re # Regular expressions
import sys
from datetime import datetime
# Project modules
import db
from full_name_splitter import split_name

# Import script outside the scope
sys.path.append('..')

class Employee:
	"""Class representing an employee."""
	def __init__(self):
		# pylint: disable=C0103
		self.id: int = None
		self.code: str = None
		self.sap_id: str = None
		self.first_name: str = None
		self.last_name: str = None
		self.email: str = None
		self.phone: str = None
		self.curp: str = None
		self.social_security_number: str = None
		self.source: str = None
		self.shift: str = None
		self.supervisor_str: str = None
		self.supervisor_id: int = None
		self.start_day: datetime = None
		self.end_date: datetime  = None
		self.enabled: int = None
		self.job_position_id: int = None
		self.job_position_code: str = None
		self.job_position_name: str = None

		self.department_id: int = None
		self.department_description: str = None

		self.project_name: str = None
		self.personal_dl1: str = None
		self.p_e: str = None
		self.education: str = None
		self.gender: str = None
		self.dob: datetime = None
		self.degree: str = None
		self.professional_cat_code: str = None
		self.cost_center: str = None

	def __str__(self):
		properties = []
		for attr, value in self.__dict__.items():
			properties.append(f"{attr}: {value}")
		return '\n'.join(properties)

	ATTRIBUTES_TO_COMPARE = {
			'code', 'sap_id', 'first_name', 'last_name', 'email', 'curp',
			'shift', 'supervisor_str', 'start_day', 'end_date',
			'project_name', 'personal_dl1', 'p_e', 'education',
			'gender', 'dob', 'degree', 'professional_cat_code', 'cost_center',
			'job_position_id', 'department_id', 'enabled', 'source',
		}

	def get_different_attributes(self, other):
		"""Compares two Employee objects and returns a string with all the
		found differences."""

		differences = ''
		if isinstance(other, Employee):
			for attr_name in self.ATTRIBUTES_TO_COMPARE:
				self_value = getattr(self, attr_name)
				other_value = getattr(other, attr_name)

				if self_value != other_value:
					self_type = str(type(self_value)).split("'")[1]
					other_type = str(type(other_value)).split("'")[1]
					differences += f"Attribute '{attr_name}' is different: {self_value} " \
						+ f"({self_type}) !== {other_value} ({other_type})\n"

		return differences

	def __eq__(self, other):
		"""Compares two objects, going to the relevant fields.
		For example creation date and ID on DB are not compared.
		Returns True if both Employees are similar.

		# Example of utilization:
		# e_1 = Employee()
		# e_2 = Employee()
		# e_1.last_name = 'Ea'
		# e_2.last_name = 'Ea1'

		# print(f'Are equal? {e_1 == e_2}')
		"""

		if isinstance(other, Employee):
			for attr_name in self.ATTRIBUTES_TO_COMPARE:
				self_value = getattr(self, attr_name)
				other_value = getattr(other, attr_name)

				if self_value != other_value:
					return False

			return True # Is every attribute equal? -> Same objects!

		return False # Not an instance of Employee? -> Are different!

	@staticmethod
	def from_row(row, departments_dict, job_positions_dict):
		"""Returns an new Object from an XLSX row."""
		employee = Employee()
		# DB Row has the following values in the tuple
		# 0-ROW_CLAVE, 1-'NOMBRE COMPLETO', 2-'DESC. PUESTO',
		# 3-ROW_DEPARTMENT_DESCRIPTION, 4-ROW_PROJECT_NAME,
		# 5-'PERSONAL_DL1', 6-'P/E', 7-'F. INGRESO', 8-'TURNO',
		# 9-'ESC.', 10-'CURP', 11-'SEXO', 12-'FECHA NAC.', 13-'ESCUELA',
		# 14-'GRADO', 15-'PROFESSIONAL CAT CODE', 16-'COST CENTER',
		# 17-'SUPERVISOR',
		employee.code = str(row[0])
		employee.sap_id = employee.code
		employee.first_name, employee.last_name = split_name(db.cleanup_string(row[1]))

		employee.job_position_code = db.cleanup_string(row[2])
		employee.job_position_name = employee.job_position_code

		# TODO: When updating an employee, the department is not created
		employee.department_description = db.cleanup_string(row[3])
		if employee.department_description in departments_dict:
			employee.department_id = departments_dict[employee.department_description]
		else:
			employee.department_id = db.get_department_id_from_description(employee.department_description)

		if employee.job_position_code in job_positions_dict:
			employee.job_position_id = job_positions_dict[employee.job_position_code]
		else:
			# TODO: How are we inserting this job_position code ?
			pass

		employee.project_name = db.cleanup_string(row[4])
		employee.personal_dl1 = db.cleanup_string(row[5])
		employee.p_e = row[6].strip()

		employee.start_day = row[7] # datetime

		employee.shift = str(row[8])
		employee.education = db.cleanup_string(row[9])
		employee.curp = db.cleanup_string(row[10])
		employee.gender = db.cleanup_string(row[11])

		employee.dob = row[12] # datetime

		employee.degree = db.cleanup_string(row[14])
		employee.professional_cat_code = db.cleanup_string(row[15])
		employee.cost_center = db.cleanup_string(row[16])
		employee.supervisor_str, employee.supervisor_id = \
			Employee.get_supervisor_id_from_str(db.cleanup_string(row[17]))

		employee.enabled = 1 # True
		employee.source = db.SOURCE

		return employee

	@staticmethod
	def get_supervisor_id_from_str(supervisor_str: str) -> str:
		"""Split the supervisor string into name and code.
		Example: 'JUAN (123)', returns
		supervisor_name: 'JUAN'
		supervisor_code: '123'."""

		supervisor_re = re.search(r'\((.*?)\)', supervisor_str)
		if supervisor_re:
			supervisor_id = supervisor_re.group(1)
			supervisor_name = re.sub(r'\(.*\)', '', supervisor_str)
			supervisor_name = db.cleanup_string(supervisor_name)
			return supervisor_name, supervisor_id

		return supervisor_str, ''
