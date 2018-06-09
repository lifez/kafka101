import json
import asyncio
from confluent_kafka import Producer


producer = Producer({
    'bootstrap.servers': 'localhost:9092'
})


def delivery_callback(future):
    def __delivery_callback(err, msg):
        if err:
            future.set_exception(err)
        elif msg:
            future.set_result(msg)
    return __delivery_callback


async def produce(email: str, subject: str, text: str):
    future = asyncio.Future()
    value = {'email': email, 'subject': subject, 'text': text}
    producer.produce('send-mail', json.dumps(value), key=email, callback=delivery_callback(future))
    data = await future
