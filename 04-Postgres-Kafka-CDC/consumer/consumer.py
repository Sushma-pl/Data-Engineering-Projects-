import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "shop.public.customers",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="customers-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Waiting for customer events...")

for message in consumer:
    print("Topic:", message.topic)
    print("Partition:", message.partition)
    print("Offset:", message.offset)

    event = message.value

    # Extract the actual CDC payload
    payload = event.get("payload", event)

    operation = payload.get("op")
    before = payload.get("before")
    after = payload.get("after")

    print("\n---------- CDC EVENT ----------")

    if operation == "r":
        print("SNAPSHOT:", after)

    elif operation == "c":
        print("INSERT:", after)

    elif operation == "u":
        print("UPDATE")
        print("Before:", before)
        print("After:", after)

    elif operation == "d":
        print("DELETE:", before)

    print("-------------------------------")