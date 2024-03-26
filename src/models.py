from mongoengine import Document
from mongoengine.fields import StringField, EmailField, BooleanField

class Contact(Document):
    fullname = StringField(unique=True)
    email = EmailField()
    sended = BooleanField(default=False)