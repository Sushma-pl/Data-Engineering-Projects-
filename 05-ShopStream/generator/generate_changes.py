import random
import time
import psycopg2

# ---------------------------------------------------
# Connects to the SOURCE Postgres (port 5433)
# and continuously mutates data so the pipeline
# always has events to process
# ---------------------------------------------------

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    dbname="shop",
    user="postgres",
    password="postgres"
)
conn.autocommit = True
cursor = conn.cursor()

CITIES    = ["Pune", "Mumbai", "Delhi", "Chennai", "Hyderabad", "Bangalore", "Kolkata"]
STATUSES  = ["created", "paid", "shipped", "delivered", "cancelled"]

# Seed 20 customers to start with
print("Seeding customers...")
for i in range(20):
    cursor.execute(
        "INSERT INTO customers (name, email, city) VALUES (%s, %s, %s)",
        (f"Customer_{i}", f"customer{i}@example.com", random.choice(CITIES) )
    )

print("Starting change loop... Press Ctrl+C to stop.")

while True:
    action = random.choices(
        ["insert", "update", "delete"],
        weights=[6, 3, 1]
    )[0]

    try:
        if action == "insert":
            cursor.execute(
                """
                INSERT INTO orders (customer_id, amount, status)
                VALUES (
                    (SELECT id FROM customers ORDER BY random() LIMIT 1), %s, 'created'
                )
                """,(round(random.uniform(100, 5000), 2),)
            )
        elif action == "update":
            cursor.execute(
                """
                UPDATE orders
                SET    status     = %s,
                       updated_at = now()
                WHERE  id = (SELECT id FROM orders ORDER BY random() LIMIT 1)
                """,
                (random.choice(STATUSES),)
            )
        elif action == "delete":
            cursor.execute(
                """
                DELETE FROM orders
                WHERE id = (SELECT id FROM orders ORDER BY random() LIMIT 1)
                """
            )

        print(f"{action.upper()} executed")
    except Exception as e:
        print(f"Error on {action}: {e}")

    time.sleep(random.uniform(0.3, 1.2))
