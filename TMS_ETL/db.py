"""TMS_ETL - Database Manager"""
# Built-in modules
from datetime import datetime
import unicodedata

# External dependencies
import pyodbc # DB Connection

from dotenv import dotenv_values, load_dotenv # Read .env files

# Project modules
from models.employee import Employee

load_dotenv()

DB_DRIVER = dotenv_values().get('DB_DRIVER')
DB_SERVER = dotenv_values().get('DB_SERVER')
DB_NAME = dotenv_values().get('DB_NAME')
DB_USERNAME = dotenv_values().get('DB_USERNAME')
DB_PASSWORD = dotenv_values().get('DB_PASSWORD')
DB_TABLE_EMPLOYEES = dotenv_values().get('DB_TABLE_EMPLOYEES')
DB_TABLE_EMPLOYEE_PROPERTIES = dotenv_values().get('DB_TABLE_EMPLOYEE_PROPERTIES')
DB_TABLE_JOB_POSITIONS = dotenv_values().get('DB_TABLE_JOB_POSITIONS')
DB_TABLE_DEPARTMENTS = dotenv_values().get('DB_TABLE_DEPARTMENTS')

# https://learn.microsoft.com/en-us/sql/sql-server/maximum-capacity-specifications-for-sql-server?view=sql-server-ver16
MAX_PARAMETERS_BY_QUERY = 2000

# TODO: Get a System user to avoid the Hard coded number
UPDATED_BY = 1 # Why ? Default user
SOURCE = 'System'
PROPERTY_GROUP = 'GENERAL'

# String constants - To avoid mistyping
PROJECT_NAME = 'PROJECT_NAME'
PERSONAL_DL1 = 'PERSONAL_DL1'
P_E = 'P_E'
EDUCATION = 'EDUCATION'
GENDER = 'GENDER'
DOB = 'DOB'
DEGREE = 'DEGREE'
PROFESSIONAL_CAT_CODE = 'PROFESSIONAL_CAT_CODE'
COST_CENTER = 'COST_CENTER'
DEPARTMENT_ID = 'DEPARTMENT_ID'

conn = pyodbc.connect(
	f'DRIVER={DB_DRIVER};SERVER={DB_SERVER};'
	f'DATABASE={DB_NAME};UID={DB_USERNAME};'
	f'PWD={DB_PASSWORD};Encrypt=no',
	autocommit=False
)

cursor = conn.cursor()

def close_db_connection():
	"""Close the connection at the end of the script."""
	conn.close()

def get_all_employee_codes() -> set:
	"""Returns a set of all employes codes."""
	employee_codes = set()

	query = f"SELECT [Code] FROM {DB_NAME}.{DB_TABLE_EMPLOYEES};"
	cursor.execute(query)
	results = cursor.fetchall()

	for row in results:
		employee_codes.add(int(row[0])) # First column = Code

	return employee_codes

def get_all_employees_with_job_position():
	"""Returns lists of rows from employes in DB to check what to insert,
	disable or update."""
	employees_rows = cursor.execute(
		f'''
		SELECT
			-- Employees
			e.[Id], e.[Code], e.[SAP_ID], e.[FirstName], e.[LastName], e.[Email],
			e.[Phone], e.[CURP], e.[SocialSecurityNumber], e.[Source],
			e.[JobPositionId], e.[Shift], e.[Supervisor], e.[SupervisorId],
			e.[StartDay], e.[EndDate], e.[Enabled],

			-- Job Positions
			j_p.[Id], j_p.[Code], j_p.[Name], j_p.[Enabled]
		FROM {DB_NAME}.{DB_TABLE_EMPLOYEES} AS e

		LEFT JOIN {DB_NAME}.{DB_TABLE_JOB_POSITIONS} as j_p
		ON e.[JobPositionId] = j_p.[Id]
		'''
	).fetchall()

	employees_dict = {}
	map_id_to_code = {}

	for row in employees_rows:
		employee = Employee()
		employee.id = int(row[0]) if row[0] is not None else None
		employee.code = str(row[1])
		employee.sap_id = row[2]
		employee.first_name = row[3]
		employee.last_name = row[4]
		employee.email = row[5]
		employee.phone = row[6]
		employee.curp = row[7]
		employee.social_security_number = row[8]
		employee.source = row[9]
		employee.job_position_id = row[10]
		employee.shift = row[11]
		employee.supervisor_str = row[12]
		employee.supervisor_id = row[13]
		employee.start_day = row[14] # datetime
		employee.end_date = row[15] # datetime

		employee.enabled = int(row[16]) if row[16] is not None else None
		employee.job_position_id = int(row[17]) if row[17] is not None else None
		employee.job_position_code = row[18]
		employee.job_position_name = row[19]

		employees_dict[employee.code] = employee

		map_id_to_code[employee.id] = employee.code

	employee_properties_rows = cursor.execute(
		f'''
		SELECT [EmployeeId], [PropertyGroup], [Tag], [Value], [Enabled]

		FROM {DB_NAME}.{DB_TABLE_EMPLOYEE_PROPERTIES}
		'''
	).fetchall()

	for row in employee_properties_rows:
		employee_id = int(row[0])
		if employee_id in map_id_to_code:
			employee_code = map_id_to_code[employee_id] # EmployeeId

			if employee_code in employees_dict:
				employee = employees_dict[employee_code]
				tag = row[2]
				value = row[3]

				employee.department_id = int(value) \
					if tag == DEPARTMENT_ID else employee.department_id

				employee.project_name = value if tag == PROJECT_NAME else employee.project_name
				employee.personal_dl1 = value if tag == PERSONAL_DL1 else employee.personal_dl1
				employee.p_e = value if tag == P_E else employee.p_e
				employee.education = value if tag == EDUCATION else employee.education
				employee.gender = value if tag == GENDER else employee.gender

				if tag == DOB:
					#print(f'{type(value)}: {value}')
					# In SQL Server microseconds are stored with 7 zeroes, and Python uses 6 =O
					# Strip the 7th zero
					#value = value[:-1]
					employee.dob = datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')

				employee.degree = value if tag == DEGREE else employee.degree
				employee.professional_cat_code = value \
					if tag == PROFESSIONAL_CAT_CODE else employee.professional_cat_code
				employee.cost_center = value if tag == COST_CENTER else employee.cost_center

	return employees_dict

def get_employee_properties(employee: Employee):
	"""Inserts employee properties into a employee object."""
	employee_properties_rows = cursor.execute(
		f'''
		SELECT [EmployeeId], [PropertyGroup], [Tag], [Value], [Enabled]
		FROM {DB_NAME}.{DB_TABLE_EMPLOYEE_PROPERTIES}
		WHERE [EmployeeId] = ?
		''',
		(employee.id,)
	).fetchall()

	for row in employee_properties_rows:
		tag = row[2]
		value = row[3]

		employee.department_id = int(value) \
			if tag == DEPARTMENT_ID else employee.department_id

		employee.project_name = value if tag == PROJECT_NAME else employee.project_name
		employee.personal_dl1 = value if tag == PERSONAL_DL1 else employee.personal_dl1
		employee.p_e = value if tag == P_E else employee.p_e
		employee.education = value if tag == EDUCATION else employee.education
		employee.gender = value if tag == GENDER else employee.gender

		if tag == DOB:
			# In SQL Server microseconds are stored with 7 zeroes, and Python uses 6 =O
			# Strip the 7th zero
			value = value[:-1]
			employee.dob = datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')

		employee.degree = value if tag == DEGREE else employee.degree
		employee.professional_cat_code = value \
			if tag == PROFESSIONAL_CAT_CODE else employee.professional_cat_code
		employee.cost_center = value if tag == COST_CENTER else employee.cost_center

def insert_employee(employe_dict: dict):
	'''Insert a new employee in the DB. Returns EmployeeId.'''
	is_enabled = 1 # True, is enabled on creation

	params = (
		employe_dict['CLAVE'], # int
		employe_dict['CLAVE'], # SAP_ID is the same than CLAVE
		cleanup_string(employe_dict['FIRST_NAME']),
		cleanup_string(employe_dict['LAST_NAME']),
		employe_dict['EMAIL'],
		cleanup_string(employe_dict['CURP']),
		SOURCE,
		employe_dict['JOB_POSITION_ID'],
		str(employe_dict['TURNO']), # int
		cleanup_string(employe_dict['SUPERVISOR']),
		cleanup_string(employe_dict['SUPERVISOR_ID']),
		employe_dict['F. INGRESO'],
		is_enabled,
		UPDATED_BY,
	)

	query = f"INSERT INTO [{DB_NAME}].{DB_TABLE_EMPLOYEES} " + \
		"([Code], [SAP_ID], [FirstName], [LastName], [Email], " + \
		"[CURP], [Source], [JobPositionId], [Shift], " + \
		"[Supervisor], [SupervisorId], [StartDay], " + \
		"[Enabled], [Created], [Updated], [UpdatedBy]" + \
		") " + \
		"OUTPUT INSERTED.Id " + \
		"VALUES " + \
		"(?, ?, ?, ?, ?, " + \
		"?, ?, ?, ?, " + \
		"?, ?, ?, " + \
		"?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?);"

	# [Code] [varchar](50) NOT NULL,
	# [SAP_ID] [varchar](50) NULL,
	# [FirstName] [varchar](50) NOT NULL,
	# [LastName] [varchar](50) NOT NULL,
	# [Email] [varchar](50) NULL,
	# [Phone] [varchar](50) NULL,
	# [CURP] [varchar](50) NULL,
	# [SocialSecurityNumber] [varchar](50) NULL,
	# [Source] [varchar](50) NULL,
	# [JobPositionId] [bigint] NULL,
	# [Shift] [varchar](50) NULL,
	# [Supervisor] [varchar](150) NULL,
	# [SupervisorId] [varchar](50) NULL,
	# [StartDay] [datetime] NULL,
	# [EndDate] [datetime] NULL,
	# [Enabled] [bit] NULL,
	# [Created] [datetime] NULL,
	# [Updated] [datetime] NULL,
	# [UpdatedBy] [int] NULL,

	result = cursor.execute(query, params).fetchone()
	cursor.commit()
	return result

def update_employee(employee: Employee):
	"""Receives an Employee object, and updates the relational data.
	It's enabled by default."""
	# Update table Employees
	params = (
		employee.code,
		employee.sap_id,
		employee.first_name,
		employee.last_name,
		employee.email,
		employee.curp,
		SOURCE,
		employee.job_position_id,
		employee.shift,
		employee.supervisor_str,
		employee.supervisor_id,
		employee.start_day,
		employee.enabled,
		UPDATED_BY,
		employee.code, # For Where
	)

	cursor.execute(
		f'''
		UPDATE {DB_NAME}.{DB_TABLE_EMPLOYEES}
		SET
			[Code] = ?,
			[SAP_ID] = ?,
			[FirstName] = ?,
			[LastName] = ?,
			[Email] = ?,
			[CURP] = ?,
			[Source] = ?,
			[JobPositionId] = ?,
			[Shift] = ?,
			[Supervisor] = ?,
			[SupervisorId] = ?,
			[StartDay] = ?,
			[Enabled] = ?,
			[Updated] = CURRENT_TIMESTAMP,
			[UpdatedBy] = ?
		WHERE [Code]  = ?
		''',
		params)
	cursor.commit()

	# Update table EmployeeProperties
	update_employee_properties(employee)

def get_employee_by_code(employee_code: str):
	"""Return True if the employee exists in the DB."""
	query = f"SELECT TOP 1 [Id], [Code] FROM {DB_NAME}.{DB_TABLE_EMPLOYEES} " \
		+ "WHERE [Code] = ?;"

	params = (employee_code, )

	return cursor.execute(query, params).fetchone()

def split_tuple(tuple_to_split, max_elements):
	"""
	Splits a tuple into smaller tuples of maximum 'max_elements' elements.

	Args:
			tuple_to_split (tuple): The tuple to be split.
			max_elements (int): The maximum number of elements per tuple.

	Returns:
			list: A list of tuples, where each tuple contains a maximum of 'max_elements' elements.

	Example:
			>>> my_tuple = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
			>>> max_elements = 3
			>>> split_tuple(my_tuple, max_elements)
			[(1, 2, 3), (4, 5, 6), (7, 8, 9), (10,)]
		"""
	return [tuple_to_split[i:i+max_elements] for i in range(0, len(tuple_to_split), max_elements)]

def set_employee_enabled(employee_codes: tuple, status: str):
	"""Receives a list of employee codes, and sets
	the column 'Enabled' to the status value, on the DB."""

	# A list of many tuples to insert
	employee_codes_list = [employee_codes]

	if len(employee_codes) > MAX_PARAMETERS_BY_QUERY:
		employee_codes_list = split_tuple(employee_codes, MAX_PARAMETERS_BY_QUERY)

	for current_employee_codes_tuple in employee_codes_list:
		query = f"""
			UPDATE {DB_NAME}.{DB_TABLE_EMPLOYEES}
			SET [Enabled] = '{status}'
			WHERE [Code] IN
				({', '.join('?' for _ in current_employee_codes_tuple)});
		"""

		cursor.execute(query, current_employee_codes_tuple)
		conn.commit()

def get_job_position_from_name(job_position_name: str) -> int:
	"""Returns the Job Position from the DB, by Name."""
	params = (job_position_name, )

	job_position = cursor.execute(
		f'''
		SELECT [Id], [Code]
		FROM {DB_NAME}.{DB_TABLE_JOB_POSITIONS}
		WHERE [Name] = ?
		''',
		params
	).fetchone()

	if job_position:
		return job_position[0]

	return insert_job_position(job_position_name)

def insert_job_position(job_position_name: str) -> int:
	'''Inserts a job position into the DB. Returns JobPosition.Id'''
	is_enabled = 1

	# Retreive the lastest code from the DB
	latest_code = cursor.execute(
		f'''
		SELECT Max([Code])
		FROM {DB_NAME}.{DB_TABLE_JOB_POSITIONS}
		'''
	).fetchone()

	# If there are no rows, start with CJP-0000
	job_position_code = 'CJP-0000'
	if latest_code[0] is not None:
		# If contains 'CJP-' remove it
		prefix = 'CJP-'
		if latest_code[0].find(prefix) != -1: # Contains the prefix
			code = int(latest_code[0].replace(prefix, ''))
			code += 1
			job_position_code = f'{prefix}{code:04d}'

		# TODO: In case the latest found code doesn't have the prefix...
		# We're screwed! Propose an alternative

	params = (job_position_code, job_position_name, is_enabled, UPDATED_BY)

	job_position = cursor.execute(
		f'''
		INSERT INTO {DB_NAME}.{DB_TABLE_JOB_POSITIONS}
		([Code], [Name], [Enabled], [Created], [Updated], [UpdatedBy])
		OUTPUT INSERTED.[Id]
		VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?)
		''',
		params
	).fetchone()

	return job_position[0] # Id

def get_job_positions_dict() -> dict:
	"""
	Returns a dict of all the JobPositions in the DB
	key=name, value=id
	"""
	job_positions_rows = cursor.execute(
		f'''
		SELECT [Id], [Name]
		FROM {DB_NAME}.{DB_TABLE_JOB_POSITIONS}
		'''
	).fetchall()

	job_positions_dict = {row[1]: row[0] for row in job_positions_rows}

	return job_positions_dict

def get_department_id_from_description(dept_description: str) -> int:
	"""Returns the DepartmentId from a DepartmentDescription.
	Also creates the Department if it doesn't exist."""

	department = cursor.execute(
		f'''
		SELECT [Id],[Code],[Alias],[Name],[Enabled],[Created],[Updated],[UpdatedBy]
		FROM {DB_NAME}.{DB_TABLE_DEPARTMENTS}
		WHERE [Code] = ?
		''',
		(dept_description,)
	).fetchone()

	if department: # If exists
		return int(department[0]) # Department.Id

	# Doesn't exist -> Create it
	return insert_department(dept_description)

def get_departments_dict() -> dict:
	"""
	Returns a dict of all the departments in the DB
	key=description, value=id
	"""
	department_rows = cursor.execute(
		f'''
		SELECT [Id],[Code],[Alias],[Name],[Enabled],[Created],[Updated],[UpdatedBy]
		FROM {DB_NAME}.{DB_TABLE_DEPARTMENTS}
		'''
	).fetchall()

	departments_dict = {row[1]: row[0] for row in department_rows}

	return departments_dict

def insert_department(dept_description: str) -> int:
	"""Based on the description insert the Department
	in the DB."""
	is_enabled = 1
	params = (dept_description, dept_description, dept_description,
		 is_enabled, UPDATED_BY)

	# Insert the row and return the Id
	result = cursor.execute(
		f'''
		INSERT INTO {DB_NAME}.{DB_TABLE_DEPARTMENTS}
			([Code],[Alias],[Name],[Enabled],[Created],[Updated],[UpdatedBy])
		OUTPUT INSERTED.[Id]
		VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?)
		;''',
		params).fetchone()
	cursor.commit()
	return result[0]

def insert_employee_properties(employee: Employee):
	"""Gets an object Employee and inserts each property into the DB."""
	# For each one of the fields, insert it in the DB
	insert_employee_property(employee.id, PROJECT_NAME, employee.project_name)
	insert_employee_property(employee.id, PERSONAL_DL1, employee.personal_dl1)
	insert_employee_property(employee.id, P_E, employee.p_e)
	insert_employee_property(employee.id, EDUCATION, employee.education)
	insert_employee_property(employee.id, GENDER, employee.gender)
	insert_employee_property(employee.id, DOB, employee.dob)
	insert_employee_property(employee.id, DEGREE, employee.degree)
	insert_employee_property(employee.id, PROFESSIONAL_CAT_CODE, employee.professional_cat_code)
	insert_employee_property(employee.id, COST_CENTER, employee.cost_center)

	# Get the department ID
	department_id = get_department_id_from_description(employee.department_description)
	insert_employee_property(employee.id, DEPARTMENT_ID, department_id)

	conn.commit()

def insert_employee_property(employee_id: int, property_name: str, value: str):
	"""Insert an employee property into the DB."""
	is_enabled = 1 # True

	if isinstance(value, datetime):
		value = value.strftime('%Y-%m-%d %H:%M:%S.%f')

	cursor.execute(
		f'INSERT INTO {DB_NAME}.{DB_TABLE_EMPLOYEE_PROPERTIES} '
		'('
		'[EmployeeId],[PropertyGroup],[Tag],[Value],'
		'[Enabled],[Created],[Updated],[UpdatedBy]) '
		'VALUES (?,?,?,?,'
		f"?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?);",
		(employee_id, PROPERTY_GROUP, property_name, value,
		is_enabled, UPDATED_BY)
	)

def update_employee_properties(employee: Employee):
	"""Updates all the properties for an employee."""
	# For each one of the fields, update it in the DB
	update_employee_property(employee.id, PROJECT_NAME, employee.project_name)
	update_employee_property(employee.id, PERSONAL_DL1, employee.personal_dl1)
	update_employee_property(employee.id, P_E, employee.p_e)
	update_employee_property(employee.id, EDUCATION, employee.education)
	update_employee_property(employee.id, GENDER, employee.gender)
	update_employee_property(employee.id, DOB, employee.dob)
	update_employee_property(employee.id, DEGREE, employee.degree)
	update_employee_property(employee.id, PROFESSIONAL_CAT_CODE, employee.professional_cat_code)
	update_employee_property(employee.id, COST_CENTER, employee.cost_center)
	update_employee_property(employee.id, DEPARTMENT_ID, employee.department_id)

	conn.commit()

def update_employee_property(employee_id: int, property_name: str, value: str):
	"""Insert an employee property into the DB."""
	is_enabled = 1 # True

	if isinstance(value, datetime):
		value = value.strftime('%Y-%m-%d %H:%M:%S.%f')

	# If exist -> Update
	# Doesn't exist -> Insert
	# Try this approach
	cursor.execute(
		f'''
		UPDATE {DB_NAME}.{DB_TABLE_EMPLOYEE_PROPERTIES}
			WITH (UPDLOCK, SERIALIZABLE)
		SET
			[Value] = ?,
			[Enabled] = ?,
			[Updated] = CURRENT_TIMESTAMP,
			[UpdatedBy] = ?
		WHERE [EmployeeId] = ? AND [Tag] = ?;

		IF @@ROWCOUNT = 0
		BEGIN
			INSERT INTO {DB_NAME}.{DB_TABLE_EMPLOYEE_PROPERTIES}
			(
				[EmployeeId],[PropertyGroup],[Tag],[Value],
				[Enabled],[Created],[Updated],[UpdatedBy]
			)
			VALUES
			(
				?,?,?,?,
				?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?
			);
		END
		''',
		(
			value, is_enabled, UPDATED_BY,
			employee_id, property_name,

			employee_id, PROPERTY_GROUP, property_name, value,
			is_enabled, UPDATED_BY
		)
	)
	cursor.commit()

def cleanup_string(input_str: str) -> str:
	"""Remove Unicode control characters and
	trailing whitespace."""
	# Remove control characters
	output_str = "".join(
		char for char in input_str if unicodedata.category(char) != 'Cc'
	)
	# Remove new lines and carriage return
	output_str = \
		''.join(line.strip() for line in output_str.split('\n'))
	output_str = \
		''.join(line.strip() for line in output_str.split('\r'))
	# Remove duplicated whitespacing
	output_str = ' '.join(output_str.split())
	output_str = output_str.strip()
	return output_str
