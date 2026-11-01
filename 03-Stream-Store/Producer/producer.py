from kafka import KafkaProducer
import json
import time

# create kafka prodcuer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'], 

    # convert python dict to json bytes
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),

    # convert message key into bytes
    key_serializer=lambda k: str(k).encode('utf-8'),

    acks='all'
)

# Sample order events
orders = [
    {
        "order_id": 101,
        "customer": "John",
        "product": "Laptop",
        "amount": 75000
    },
    {
        "order_id": 102,
        "customer": "Alice",
        "product": "Mobile",
        "amount": 25000
    },
    {
        "order_id": 103,
        "customer": "Bob",
        "product": "Headphones",
        "amount": 3000
    }
]

# send messages
for order in orders:

    future = producer.send(
        topic='orders',
        key=order['order_id'],
        value=order
    )

    metadata = future.get(timeout=10)

    print(
        f"Sent: {order} | "
        f"Partition: {metadata.partition} | "
        f"Offset: {metadata.offset}"
    )

# ensure all message delivered
producer.flush()
producer.close()

print("All message sent succesfully!!")