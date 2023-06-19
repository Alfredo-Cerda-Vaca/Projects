"""COT_ETL - Sends notification emails"""
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

MAIL_BODY = "ETL Notificacion"

def send_email_notification(text_to_send: str):
	"""Sends a notification with a fixed message."""
	# Create the email message
	message = MIMEMultipart()
	message['From'] = MAIL_FROM
	message['To'] = MAIL_TO
	message['Subject'] = MAIL_SUBJECT
	mail_body = MAIL_BODY + '\n\n' + text_to_send
	message.attach(MIMEText(mail_body, 'plain'))

	# TODO: Catch exceptions on sending the mail
	# and implement a retrying or connect to a logger
	with smtplib.SMTP(MAIL_HOST, MAIL_PORT) as smtp:
		smtp.starttls()
		smtp.login(MAIL_USER, MAIL_PASS)

		text = message.as_string()
		smtp.sendmail(MAIL_FROM, MAIL_TO, text)
		return True
