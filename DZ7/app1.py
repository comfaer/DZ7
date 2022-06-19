import json
import datetime
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['194.61.2.84:32769'], api_version=(0,10,1))   

while 1:
	send_json = None
	try:
		send_type_message = int(input("\nВведите 1, если хотите отправить сообщение или введите 2, если хотите отправить сообщение об ошибке\n"))  
	except ValueError:
		continue

	if send_type_message == 1:  
		print(datetime.datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S"),"\tСообщение типа *message*")
		send_json = (json.dumps({"type": "message"})).encode('utf-8')

	elif send_type_message == 2:  
			print(datetime.datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S"),"\tСообщение типа *error*")
			send_json = (json.dumps({"type": "error"})).encode('utf-8')

	producer.send('main_topic', send_json)

