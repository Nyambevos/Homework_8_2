import json

import pika
from pika.adapters.blocking_connection import BlockingChannel
from faker import Faker
from mongoengine.fields import ObjectId

import connect
from models import Contact

def gen_fake_contacts(number: str = 5) -> list:
    faker = Faker('en_US')
    contacts = []
    for _ in range(number):
        contacts.append(
            {'fullname': faker.name(),
             'email': faker.email()}
            )
    return contacts

def save_contact_to_db(contact: dict) -> ObjectId:
    contact = Contact(**contact).save()
    return contact.id

def send_messages(message: dict, channel: BlockingChannel):
    channel.basic_publish(
            exchange='contact_mock',
            routing_key='contact_queue',
            body=json.dumps(message).encode(),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ))
    print(" [x] Sent %r" % message)

def main() -> None:
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
    channel = connection.channel()

    channel.exchange_declare(exchange='contact_mock', exchange_type='direct')
    channel.queue_declare(queue='contact_queue', durable=True)
    channel.queue_bind(exchange='contact_mock', queue='contact_queue')

    # Generation fake contacts
    contacts = gen_fake_contacts()

    # Save contacts to database
    for contact in contacts:
        save_contact_to_db(contact)

    # Get contacts from database
    contacts = Contact.objects(sended=False)

    for contact in contacts:
        message = str(contact.id)
        send_messages(message=message, channel=channel)

    connection.close()

    
if __name__ == "__main__":
    main()