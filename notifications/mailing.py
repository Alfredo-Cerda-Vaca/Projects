"""Sends notification emails"""
# External dependencies
import smtplib # Send email
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from dotenv import dotenv_values, load_dotenv # Read .env files

load_dotenv()

MAIL_HOST = dotenv_values().get('MAIL_HOST')
MAIL_PORT = dotenv_values().get('MAIL_PORT')
MAIL_PORT = dotenv_values().get('587')
MAIL_SECURE = dotenv_values().get('MAIL_SECURE')

MAIL_USER = dotenv_values().get('MAIL_USER')
MAIL_PASS = dotenv_values().get('MAIL_PASS')

MAIL_FROM = dotenv_values().get('MAIL_FROM')
MAIL_TO = dotenv_values().get('MAIL_TO')
MAIL_SUBJECT = dotenv_values().get('MAIL_SUBJECT')

NOTIFICAION_SUBJECT = "ETL Notificacion Service"


def send_email_notification(information_email: dict, data_opcional: str):
	"""."""
	message = MIMEMultipart()
	message['From'] = MAIL_FROM
	message['To'] = ','.join(information_email['to'])
	message['Subject'] = NOTIFICAION_SUBJECT

	try:
		if data_opcional != "":
			mail_body = '\n\n' + data_opcional
			message.attach(MIMEText(mail_body, 'html'))

		elif information_email['body'] != "":
			mail_body = '\n\n ' + information_email['body']
			message.attach(MIMEText(mail_body, 'html' or 'plain'))

		# TODO: Catch exceptions on sending the mail
		# and implement a retrying or connect to a logger
		with smtplib.SMTP(MAIL_HOST, MAIL_PORT) as smtp:
			smtp.starttls()
			smtp.login(MAIL_USER, MAIL_PASS)

			text = message.as_string()
			smtp.sendmail(MAIL_FROM, information_email['to'], text)
			return True
	except KeyError as ex:
		print(f'Not found the key: {ex}')
