-- ============================================================
-- ShopStream source schema
-- Runs automatically when the postgres container first starts
-- ============================================================

CREATE TABLE customers (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    email      TEXT,
    city       TEXT,
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE orders (
    id          SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id),
    amount      NUMERIC(10, 2),
    status      TEXT,
    created_at  TIMESTAMP DEFAULT now(),
    updated_at  TIMESTAMP DEFAULT now()
);

-- Full replica identity so Debezium captures the complete
-- "before" row on updates and deletes, not just the PK
ALTER TABLE orders  REPLICA IDENTITY FULL;
ALTER TABLE customers REPLICA IDENTITY FULL;

-- Seed data so the pipeline has something to snapshot on first run
INSERT INTO customers (name, email, city) VALUES
    ('Asha',   'asha@example.com',   'Pune'),
    ('Ravi',   'ravi@example.com',   'Delhi'),
    ('Meera',  'meera@example.com',  'Mumbai'),
    ('Arjun',  'arjun@example.com',  'Chennai'),
    ('Priya',  'priya@example.com',  'Hyderabad');

INSERT INTO orders (customer_id, amount, status) VALUES
    (1, 1500.00, 'created'),
    (2,  899.50, 'paid'),
    (3, 3200.75, 'shipped'),
    (4,  450.00, 'delivered'),
    (5, 2100.00, 'created');
