CREATE TABLE users (user_id VARCHAR(10) PRIMARY KEY, username VARCHAR(100) NOT NULL, age INTEGER NOT NULL);
CREATE TABLE products (product_id VARCHAR(20) PRIMARY KEY, productname VARCHAR(200) NOT NULL, price FLOAT(24) NOT NULL);
CREATE TABLE transactions (purchase_date DATETIME NOT NULL, customer_id VARCHAR(10) NOT NULL, product_id VARCHAR(20) NOT NULL, price FLOAT(24) NOT NULL);
