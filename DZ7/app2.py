import json
import datetime
from kafka import KafkaConsumer
from kafka import KafkaProducer

consumer = KafkaConsumer('main_topic', 	bootstrap_servers=['194.61.2.84:32769'], api_version=(0,10,1), auto_offset_reset='earliest') 
producer = KafkaProducer(bootstrap_servers=['194.61.2.84:32769'], api_version=(0,10,1)) 

last_offset = 0

while 1: 
    for msg in consumer:  
        if msg.offset > last_offset: 
            type_message = (json.loads(msg.value.decode('utf-8')))['type'] 
            if type_message == 'message':  
                print(datetime.datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S"),"\tDone") 
            else:
                print(datetime.datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S"),"\t Сообщение отправленно в dead_letter") 
                send_json = (json.dumps({"type": type_message})).encode('utf-8')
                producer.send('dead_letter', send_json) 

            last_offset = msg.offset