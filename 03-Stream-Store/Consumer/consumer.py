from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers=['localhost:9092'],

    group_id='order-processing-group',

    auto_offset_reset='earliest',

    value_deserializer=lambda v: json.loads(v.decode('utf-8')),

    enable_auto_commit=True
)

print("waiting for message....")

try:
    for message in consumer:
        order = message.value

        print("\nReceived Order:")
        print(f"Order ID: {order['order_id']}")
        print(f"Customer: {order['customer']}")
        print(f"Product: {order['product']}")
        print(f"Amount: {order['amount']}")

        print(f"Topic: {message.topic}")
        print(f"Partition: {message.partition}")
        print(f"Offset: {message.offset}")
except KeyboardInterrupt:
    print("Consumer stopped")

finally:
    consumer.close()