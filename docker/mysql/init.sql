-- Create Databases for Microservices
CREATE DATABASE IF NOT EXISTS order_db;
CREATE DATABASE IF NOT EXISTS inventory_db;
CREATE DATABASE IF NOT EXISTS notification_db;

-- Grant privileges
GRANT ALL PRIVILEGES ON order_db.* TO 'root'@'%';
GRANT ALL PRIVILEGES ON inventory_db.* TO 'root'@'%';
GRANT ALL PRIVILEGES ON notification_db.* TO 'root'@'%';
FLUSH PRIVILEGES;

-- Use inventory_db and seed sample products
USE inventory_db;

CREATE TABLE IF NOT EXISTS products (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    available_quantity INT NOT NULL DEFAULT 0,
    reserved_quantity INT NOT NULL DEFAULT 0,
    version BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO products (id, name, available_quantity, reserved_quantity)
VALUES 
    (55, 'Wireless Mechanical Keyboard', 50, 0),
    (56, 'Ergonomic Vertical Mouse', 25, 0),
    (57, '4K Ultra HD Gaming Monitor', 10, 0),
    (58, 'USB-C Multiport Hub (10-in-1)', 2, 0),
    (59, 'Noise Cancelling Headphones', 0, 0)
ON DUPLICATE KEY UPDATE 
    name=VALUES(name),
    available_quantity=VALUES(available_quantity),
    reserved_quantity=VALUES(reserved_quantity);

-- Use order_db and prepare schema
USE order_db;

CREATE TABLE IF NOT EXISTS orders (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INT NOT NULL,
    status VARCHAR(30) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Use notification_db and prepare schema
USE notification_db;

CREATE TABLE IF NOT EXISTS notification_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    product_id BIGINT,
    notification_type VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    message VARCHAR(1000) NOT NULL,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
