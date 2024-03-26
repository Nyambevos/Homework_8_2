import pika

import time
import json

import connect
from models import Contact

credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
channel = connection.channel()

channel.queue_declare(queue='contact_queue', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')

def send_email(email: str, message: str):
    print(f" [x] Sended message: '{message}' to email: '{email}'")

def callback(ch, method, properties, body):
    message = json.loads(body.decode())
    print(f" [x] Received {message}")
    time.sleep(1)
    print(f" [x] Done: {method.delivery_tag}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

    contact = Contact.objects(id = message).first()
    send_email(email=contact.email, message=f"Hello {contact.fullname}")
    contact.update(sended=True)
    print("--------------------------------------")


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='contact_queue', on_message_callback=callback)


if __name__ == '__main__':
    channel.start_consuming()
