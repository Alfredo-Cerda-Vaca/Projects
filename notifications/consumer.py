import pika as pk
from dotenv import dotenv_values, load_dotenv # Read .env filest
import main

load_dotenv()

USER = dotenv_values().get('USER')
PASS = dotenv_values().get('PASS')

def config_consumer():
	"""."""
	connection_parameters = pk.ConnectionParameters(
			host ="192.168.10.160",
			port = 5672,
			credentials = pk.PlainCredentials(
				username= USER,
				password= PASS
			)
		)
	channel = pk.BlockingConnection(connection_parameters).channel()
	channel.queue_declare(
			queue = "emailNotity.queue",
			durable = True
		)

	channel.basic_consume(
			queue = "emailNotity.queue",
			auto_ack = True,
			on_message_callback = main.get_messenge_callback
		)
	try:
		print('Lister RabbitMQ on port 5672')
		channel.start_consuming()
	except KeyboardInterrupt as ex:
		print(f'Error de interrupcion: {ex}')
	