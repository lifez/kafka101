import json

from confluent_kafka import Consumer

from send_mail import send_mail


def main():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'lifez',
        'default.topic.config': {
            'auto.offset.reset': 'earliest'
        }
    })
    consumer.subscribe(['send-mail'])
    while(True):
        message = consumer.poll(1.0)
        if message:
            data = json.loads(message.value())
            send_mail(
                mail_to=data['email'],
                subject=data['subject'],
                message=data['text']
            )


if __name__ == '__main__':
    main()
