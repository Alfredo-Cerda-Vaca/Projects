"""COT_ETL"""
# Dev by E-Alfredo_Cerda@usiglobal.com

# Built-in modules
import time # To get execution time
from datetime import datetime # Time management
from logger import log, get_current_log, clean_current_log #Send mail
from mailing import send_email_notification

#External dependencies
import db

# Start the timer
start_time = time.time()

def validation_lenght_origin(data: list, sdatetime: datetime, edatetime: datetime):
	"""
	Condition for leght data in case the string is less 0
	insert a success in history schedule
	"""
	if len(data) > 0:
		log(f'-----3.- Found data from COT: {len(data)}')
		db.insert_segment(data, sdatetime, edatetime)
	else:
		db.insert_history_scheduled(sdatetime, edatetime, 'Success')
		print('---NO COT Info---')

def insert_success(sdatetime: datetime, edatetime: datetime, messenge: str):
	"""
	Depend of action realized is the entry messenge (SUCCESS/NO RUN) 
	"""
	db.insert_history_scheduled(sdatetime, edatetime, messenge)
	print('---NO COT Info---')

def bring_results(origin1 ,job_run,sdatetime, edatetime):
	"""
	Comparation with job run in different case job run 0 or job run 1
	and origin1 can't be None or send an entry No Run
	"""
	if int(job_run) == 1:
		if origin1 is not None:
			validation_lenght_origin(origin1, sdatetime, edatetime)
			return
		insert_success(sdatetime, edatetime,'Success')
		return
	insert_success(sdatetime, edatetime,'No Run')
	return

if __name__ == '__main__':
	log('Starting ETL script...')

	def etl_run():
		"""
		Obtein date range for realized a filter with data information from Oracle
		continues with comparation for send data entry To SQL
		"""
		try:
			scheduled_trigger = db.get_range_dates()
			if scheduled_trigger is not None:
				if len(scheduled_trigger) > 0:
					log("Start")
					start_datetime = scheduled_trigger[0][1]
					end_datetime = scheduled_trigger[0][2]
					job_run = scheduled_trigger[0][3]

					if scheduled_trigger[0] is None:
						print("DATA NOT FOUND!")
						return
					db.choose_select(start_datetime,end_datetime)
					result_statement1 = db.choose_select(start_datetime,end_datetime)
					log("-----1.- Connect to Oracle COT (origin 1)......")
					origin1 = db.get_result_from_oracle(result_statement1)
					log("-----2.- catch results COT")
					bring_results(origin1, job_run, start_datetime, end_datetime)

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
					return
				print(">>NOT CONFIGURATION FOR LAST EXECUTION<<")
				return
			print(">>NOT DATA FOUND!<<")
			return
		except (RuntimeError, TypeError, NameError) as ex:
			print("ERROR IN ANY PROCCESS",ex)
	etl_run()
