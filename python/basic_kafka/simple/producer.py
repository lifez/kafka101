from confluent_kafka import Producer


def main():
    producer = Producer({
        'bootstrap.servers': 'localhost:9092',
        'queue.buffering.max.messages': 500000
    })
    for i in range(10):
        producer.produce('test-topic', (f'Hello {i}'))
        producer.poll(0)
    producer.flush()


if __name__ == '__main__':
    main()
    print('Completed.')
