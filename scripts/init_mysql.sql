-- MySQL source database initialization
CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT NOT NULL,
    customer VARCHAR(255),
    amount DOUBLE,
    last_modified DATETIME NOT NULL,
    PRIMARY KEY (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Seed data: 5 sample orders
INSERT INTO orders (order_id, customer, amount, last_modified)
VALUES
    (1, 'alice', 100.50, '2025-01-01 10:00:00'),
    (2, 'bob',   200.00, '2025-01-01 11:00:00'),
    (3, 'carol', 300.75, '2025-01-02 08:00:00'),
    (4, 'dan',   150.00, '2025-01-02 09:30:00'),
    (5, 'eve',   175.25, '2025-01-03 14:00:00')
ON DUPLICATE KEY UPDATE amount=VALUES(amount);
