"""NOTIFICATIONS_SERVICE_ETL"""
# Dev by e-alfredo_cerda@usiglobal.com
import json
import mailing
import connection as db
import consumer


def get_messenge_callback(ch, method, properties, body):
	"""get messenge with RabbitMQ"""
	try:
		my_bytes_value = body.decode()
		result_json = json.loads(my_bytes_value)
		if result_json['notification_type'] == "EMAIL_WITHOUT_TEMPLATE":
			mailing.send_email_notification(result_json, "")

		elif result_json['notification_type'] == "EMAIL_WITH_TEMPLATE":
			result_template_str = db.choose_name_template(result_json)
			mailing.send_email_notification(result_json, result_template_str)
	except UnboundLocalError as error:
		print(f'Json con formato invalido: {error}')
	except ValueError as error:
		print(f'Error en algun valor de Json: {error}')
	return result_json

if __name__ == "__main__":

	consumer.config_consumer()
