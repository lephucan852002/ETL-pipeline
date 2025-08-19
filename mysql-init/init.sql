CREATE DATABASE IF NOT EXISTS sourcedb;
USE sourcedb;

CREATE TABLE IF NOT EXISTS orders (
  id INT PRIMARY KEY AUTO_INCREMENT,
  order_date DATE,
  customer_name VARCHAR(100),
  amount DECIMAL(12,2)
);

INSERT INTO orders (order_date, customer_name, amount) VALUES
('2025-01-01','Alice',100.50),
('2025-01-02','Bob',200.00),
('2025-01-03','Charlie',150.25);
