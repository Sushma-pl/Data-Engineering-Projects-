import json
from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata
import psycopg2

# --------------------------------
# 1. Connect to target PostgreSQL
# --------------------------------

db_connection = psycopg2.connect(
    host="localhost",
    port=5435,
    database="shop_target",
    user="postgres",
    password="postgres"
)

print("Connected to target PostgreSQL!")

# 2. Create Kafka consumer
# --------------------------------

consumer = KafkaConsumer(
    "shop.public.customers",
    bootstrap_servers=["localhost:9092"],
    group_id="customers-sink-group",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda x: json.loads(x.decode("utf-8") if x is not None else None)
)

print("Waiting for CDC events...")

try:
    for message in consumer:
        event = message.value

        if event is None:
            consumer.commit({
                TopicPartition(message.topic, message.partition):
                OffsetAndMetadata(message.offset + 1, "")
            })
            continue

        # Extract Debezium payload
        payload= event.get('payload', event)

        operation=payload.get('op')
        before=payload.get('before')
        after= payload.get('after')

        print("\n---------- CDC EVENT ----------")
        print("Operation:", operation)

        with db_connection:
            with db_connection.cursor() as cursor:
                if operation in ("r", "c", "u"):
                    customer=after
                    cursor.execute(
                        """
                        INSERT INTO customers (id, name, email, city)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (id)
                        DO UPDATE SET
                            name = EXCLUDED.name,
                            email = EXCLUDED.email,
                            city = EXCLUDED.city;
                        """,
                        (
                            customer["id"],
                            customer["name"],
                            customer["email"],
                            customer["city"]
                        )
                    )

                    print("UPSERT:", customer)
                elif operation=='d':
                    customer_id = before["id"]
                    cursor.execute(
                        "DELETE FROM customers WHERE id = %s",
                        (customer_id,)
                    )

                    print("DELETE customer ID:", customer_id)

                else:
                    print("Unknown operation:", operation)
        
        # Commit this exact Kafka message only after DB transaction succeeds
        consumer.commit({
            TopicPartition(message.topic, message.partition):
            OffsetAndMetadata(message.offset + 1, "")
        })

        print("Kafka offset committed:", message.offset + 1)

except KeyboardInterrupt:
    print("\nStopping sink...")

finally:
    consumer.close()
    db_connection.close()
    print("Connections closed.")


