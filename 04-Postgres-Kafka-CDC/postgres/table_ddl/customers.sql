CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(150),
    city VARCHAR(100)
);

-- for target postgres
CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(150),
    city VARCHAR(100)
);

INSERT INTO customers (name, email, city)
VALUES
    ('Alice', 'alice@example.com', 'Delhi'),
    ('Bob', 'bob@example.com', 'Mumbai');