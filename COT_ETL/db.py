"""
Dependencies External for connect with data bases
"""
# Built-in modules
from datetime import datetime # Time managemen
#External dependencies
import oracledb #Driver per Oracle
import pyodbc #Driver per SQL
from dotenv import dotenv_values, load_dotenv # Read .env filest
from logger import log

load_dotenv() #Load datas from .env

#!Information from ORACLE .env
DB_USERNAMEORC = dotenv_values().get('DB_USERNAMEORC')
DB_PASSWORDORC = dotenv_values().get('DB_PASSWORDORC')
DB_DSN = dotenv_values().get('DB_DSN')

	## Information FROM SQL .env

DB_DRIVER = dotenv_values().get('DB_DRIVER')
DB_SERVER = dotenv_values().get('DB_SERVER')
DB_NAME = dotenv_values().get('DB_NAME')
DB_USERNAME = dotenv_values().get('DB_USERNAME')
DB_PASSWORD = dotenv_values().get('DB_PASSWORD')

##Tables
DB_TABLE_GENERAL_SCHEDULED_HISTORY = dotenv_values().get('DB_TABLE_GENERAL_SCHEDULED_HISTORY')
DB_TABLE_GENERAL_SCHEDULED_CONFIG = dotenv_values().get('DB_TABLE_GENERAL_SCHEDULED_CONFIG')

DB_TABLE_zDEVGENERAL_HISTORY = dotenv_values().get('DB_TABLE_zDEVGENERAL_SCHEDULED_HISTORY')
DB_TABLE_zDEVGENERAL_CONFIG = dotenv_values().get('DB_TABLE_zDEVGENERAL_SCHEDULED_CONFIG')
DB_TABLE_zDEVTmpCOT = dotenv_values().get('DB_TABLE_zDEVTmpCOT')
DB_TABLE_zDEVCOT = dotenv_values().get('DB_TABLE_zDEVCOT')



def get_result_from_oracle(query: str) -> list:
	"""
	Obtein results with query and relized a config with data bases from Oracle
	"""
	dataorc = []
	## Mode Thunk
	oracledb.init_oracle_client()
	## config Oracle
	connection=oracledb.connect(
	user = DB_USERNAMEORC, password = DB_PASSWORDORC,dsn = DB_DSN, encoding = 'UTF-8'
	)
	cursor1 = connection.cursor()
	cursor1.execute(query)

	for row in cursor1:
		dataorc.append(row)
	connection.close()
	#print("Connection close from oracle")
	return dataorc

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

def get_range_dates() -> list:
	"""
	Obtein range date with first register from data SQL
	"""
	data_edashboard_init = []
	query =f'''
	SELECT top 1 sh.datestart as PrevStart, sh.dateend as DateStart, DATEADD(HOUR,sc.[HoursToAdd],dateend) as DateEnd,
	CASE WHEN sh.datestart <= DATEADD(HOUR,-24,getdate()) THEN '1' else '0' end as Run, DATEADD(HOUR,-24,getdate())
	FROM [{DB_NAME}].{DB_TABLE_GENERAL_SCHEDULED_HISTORY} sh (NOLOCK) 
	INNER JOIN [{DB_NAME}].{DB_TABLE_GENERAL_SCHEDULED_CONFIG} sc (NOLOCK) ON sh.ScheduledTaskName = sc.ScheduledTaskName
	WHERE sh.ScheduledTaskName ='USI_COT_ETL' AND Enabled = 1 AND [Status] = 'Success'
	ORDER BY sh.id DESC
	'''
	cursor.execute(query)
	for row in cursor:
		data_edashboard_init.append(row)
	return data_edashboard_init

def insert_history_scheduled(startdate: datetime, enddate: datetime, msg: str):
	"""
	Relized a insert on zDEVGeneralScheduledHistory depend of the messenge registered (No RUN/ SUCCESS)
	"""
	query_string=f'''INSERT INTO {DB_TABLE_zDEVGENERAL_HISTORY}
	([ScheduledTaskName],[LastExecution],[DateStart],[DateEnd],[Status])
	VALUES ('USI_COT_ETL',GETDATE(),'{startdate}','{enddate}','{msg}')'''
	cursor.execute(query_string)
	cursor.commit()

def create_table_tempcot():
	"""
	Create table with same values than table definitive COT
	"""
	query_create_table = f'''
	CREATE TABLE [{DB_NAME}].{DB_TABLE_zDEVTmpCOT}(
	[RECORD_ID] [varchar](100) NULL,
	[BU] [varchar](100) NULL,
	[DEPT] [varchar](100) NULL,
	[LINE_NAME] [varchar](100) NULL,
	[MODEL_NAME] [varchar](100) NULL,
	[CUSTOMER] [varchar](100) NULL,
	[PROJECT_NAME] [varchar](100) NULL,
	[SHIFT_ID] [varchar](100) NULL,
	[EXCEPT_ID] [varchar](100) NULL,
	[ACTIVATE_TYPE] [varchar](100) NULL,
	[ACTIVATE_EMP_NO] [varchar](100) NULL,
	[CONFIRM_EMP_NO] [varchar](100) NULL,
	[START_TIME] [datetime] NULL,
	[END_TIME] [datetime] NULL,
	[STATUS] [varchar](100) NULL,
	[TOTAL_SUPPORT] [varchar](100) NULL,
	[TOTAL_TIME] [varchar](100) NULL,
	[TOTAL_DAY] [int] NULL,
	[REMARK] [varchar](500) NULL,
	[MACHINE] [varchar](100) NULL,
	[BD_SIDE] [varchar](100) NULL,
	[SUPPORT_DEPT] [varchar](100) NULL,
	[MO_NUMBER] [varchar](100) NULL,
	[EXCEPT_NAME] [varchar](500) NULL,
	[EXCEPT_DESC] [varchar](500) NULL,
	[EXCEPT_OWNER] [varchar](100) NULL)
	'''
	cursor.execute(query_create_table)
	cursor.commit()

def choose_select(start_datetime: datetime, end_datetime: datetime) ->str:
	"""
	Choose the query with comparation about range date
	"""
	statement1 = ''
	if start_datetime != end_datetime:
		statement1 = f'''
		SELECT art.RECORD_ID,art.BU,art.DEPT,art.LINE_NAME,art.MODEL_NAME, 
		CASE WHEN m2.CUSTOMER IS NULL THEN 'NULL' ELSE m2.CUSTOMER END AS CUSTOMER, 
		CASE WHEN m2.PROJECT_NAME IS NULL THEN 'NULL' ELSE m2.PROJECT_NAME END AS PROJECT_NAME,
		art.SHIFT_ID,art.EXCEPT_ID,art.ACTIVATE_TYPE,art.ACTIVATE_EMP_NO,art.CONFIRM_EMP_NO,art.START_TIME,art.END_TIME,art.STATUS,
		TO_CHAR(art.TOTAL_SUPPORT) AS TOTAL_SUPPORT,
		TO_CHAR(art.TOTAL_TIME) AS TOTAL_TIME,
		TO_CHAR(art.TOTAL_DAY) AS TOTAL_DAY,
		art.REMARK,art.MACHINE,art.BD_SIDE,art.SUPPORT_DEPT,art.MO_NUMBER,cect.EXCEPT_NAME,cect.EXCEPT_DESC,cect.EXCEPT_OWNER
		FROM SMTB.R_ACTIVATION_RECORD_T art
		LEFT JOIN smt.C_EXCEPT_CODE_T cect ON (art.EXCEPT_ID =CECT.EXCEPT_ID)
		LEFT JOIN SFIS1.C_MODEL_DESC2_t m2 ON (art.MODEL_NAME = m2.MODEL_NAME)
		WHERE START_TIME BETWEEN TO_DATE ('{start_datetime}', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE ('{end_datetime}', 'YYYY-MM-DD HH24:MI:SS') 
		AND art.STATUS = 'CLOSE' 
		'''
	else:
		statement1 = f'''
		SELECT art.RECORD_ID,art.BU,art.DEPT,art.LINE_NAME,art.MODEL_NAME, m2.CUSTOMER, m2.PROJECT_NAME,art.SHIFT_ID,art.EXCEPT_ID,art.ACTIVATE_TYPE,art.ACTIVATE_EMP_NO,art.CONFIRM_EMP_NO,art.START_TIME,art.END_TIME,art.STATUS,art.TOTAL_SUPPORT,art.TOTAL_TIME,art.TOTAL_DAY,art.REMARK,art.MACHINE,art.BD_SIDE,art.SUPPORT_DEPT,art.MO_NUMBER,cect.EXCEPT_NAME,cect.EXCEPT_DESC,cect.EXCEPT_OWNER
		FROM SMTB.R_ACTIVATION_RECORD_T art
		LEFT JOIN smt.C_EXCEPT_CODE_T cect ON (art.EXCEPT_ID =CECT.EXCEPT_ID)
		LEFT JOIN SFIS1.C_MODEL_DESC2_t m2 ON (art.MODEL_NAME = m2.MODEL_NAME)
		WHERE START_TIME BETWEEN TO_DATE ('{start_datetime}', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE ('${end_datetime}', 'YYYY-MM-DD HH24:MI:SS') 
		'''
	print("QUERY SELECT: ",statement1)

	return statement1

def insert_step_5(data: list):
	"""
	realize a transaction in case error with insert values on table temp COT
	"""
	tempdata =[]
	for row in data:
		queryinsert=f'''
		declare @Error int
		begin tran
		INSERT INTO [{DB_NAME}].{DB_TABLE_zDEVTmpCOT}
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		set @Error = @@Error
		if(@Error <> 0)
		begin
		rollback tran
		end
		else
		commit
		'''
		cursor.execute(queryinsert, row[0],row[1],row[2],row[3],row[4],row[5],
		row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],
		row[18],row[19],row[20],row[21],row[22],row[23],row[24],row[25])
		cursor.commit()

def insert_segment(data: list, start_datetime: datetime, end_datetime: datetime):
	"""
	relize a copy, all the information on temp COT is insert on table COT definitive
	"""
	log('-----4.- starting insertion from COT......')
	## Create table temporal COT
	#create_table_tempcot()

	log("-----5.- Inserting TEM COT------")
	insert_step_5(data)
	try:
		log("-----6.- Insert TMP COT completed ")
		query_clone_table = f'''
		declare @Error int
		begin tran
		INSERT INTO {DB_TABLE_zDEVCOT}
				([RECORD_ID]
					,[BU]
					,[DEPT]
					,[LINE_NAME]
					,[MODEL_NAME]
					,[CUSTOMER]
					,[PROJECT_NAME]
					,[SHIFT_ID]
					,[EXCEPT_ID]
					,[ACTIVATE_TYPE]
					,[ACTIVATE_EMP_NO]
					,[CONFIRM_EMP_NO]
					,[START_TIME]
					,[END_TIME]
					,[STATUS]
					,[TOTAL_SUPPORT]
					,[TOTAL_TIME]
					,[TOTAL_DAY]
					,[REMARK]
					,[MACHINE]
					,[BD_SIDE]
					,[SUPPORT_DEPT]
					,[MO_NUMBER]
					,[EXCEPT_NAME]
					,[EXCEPT_DESC]
					,[EXCEPT_OWNER]
					,[CREATE_DATE])
			SELECT
			t.[RECORD_ID]
			,t.[BU]
			,t.[DEPT]
			,t.[LINE_NAME]
			,t.[MODEL_NAME]
			,t.[CUSTOMER]
			,t.[PROJECT_NAME]
			,t.[SHIFT_ID]
			,t.[EXCEPT_ID]
			,t.[ACTIVATE_TYPE]
			,t.[ACTIVATE_EMP_NO]
			,t.[CONFIRM_EMP_NO]
			,t.[START_TIME]
			,t.[END_TIME]
			,t.[STATUS]
			,t.[TOTAL_SUPPORT]
			,t.[TOTAL_TIME]
			,t.[TOTAL_DAY]
			,t.[REMARK]
			,t.[MACHINE]
			,t.[BD_SIDE]
			,t.[SUPPORT_DEPT]
			,t.[MO_NUMBER]
			,t.[EXCEPT_NAME]
			,t.[EXCEPT_DESC]
			,t.[EXCEPT_OWNER]
			,GETDATE()
			FROM {DB_TABLE_zDEVTmpCOT} t
		set @Error = @@Error
		if(@Error <> 0)
		begin
		rollback tran
		end
		else
		commit
		'''
		#Clone data table from zDEVTmpCOT to zDEVCOT
		cursor.execute(query_clone_table)
		cursor.commit()
		log(f"-----7.- COT inserted on table definitive Row Affected : {len(data)}")

		cursor.execute(f'''
		declare @Error int
		begin tran
		DELETE FROM {DB_TABLE_zDEVTmpCOT}
		set @Error = @@Error
		if(@Error <> 0)
		begin
		rollback tran
		end
		else
		commit
		''')

		cursor.commit()

		log(f'-----8.- COT datas temporaly deleted: {len(data)}')
		insert_history_scheduled(start_datetime, end_datetime, 'Success')
	except (RuntimeError, TypeError, NameError):
		insert_history_scheduled(start_datetime, end_datetime, 'Error Nulls')
	