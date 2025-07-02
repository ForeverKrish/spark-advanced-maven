/*
Design tables to store sales and customer data.
– If a customer’s current address is California and they order “Pharmacy,” flag ‘Y’.
– If any past order’s address was California, flag ‘Y’; else ‘N’.
– Provide row-level masking so only California-flagged customers are visible.
*/

-- Core tables
CREATE TABLE customers (
  customer_id        BIGINT PRIMARY KEY,
  current_address    VARCHAR(100),
  ever_in_ca_pharmacy CHAR(1)  -- 'Y' or 'N'
);

CREATE TABLE orders (
  order_id        BIGINT PRIMARY KEY,
  customer_id     BIGINT REFERENCES customers,
  product_category VARCHAR(50),
  order_date       DATE
);

CREATE TABLE customer_address_history (
  history_id     BIGINT PRIMARY KEY,
  customer_id    BIGINT REFERENCES customers,
  address        VARCHAR(100),
  effective_from DATE,
  effective_to   DATE  -- NULL if still in effect
);

-- Nightly batch to populate the flag
UPDATE customers c
SET ever_in_ca_pharmacy = CASE
  WHEN EXISTS (
    SELECT 1
    FROM orders o
    WHERE o.customer_id = c.customer_id
      AND o.product_category = 'Pharmacy'
      AND (
        c.current_address = 'California'
        OR EXISTS (
          SELECT 1
          FROM customer_address_history h
          WHERE h.customer_id = c.customer_id
            AND h.address = 'California'
            AND h.effective_from <= o.order_date
            AND (h.effective_to IS NULL OR h.effective_to >= o.order_date)
        )
      )
  ) THEN 'Y'
  ELSE 'N'
END;

-- Row-level masking via a view
CREATE VIEW ca_pharmacy_customers AS
SELECT *
FROM customers
WHERE current_address = 'California'
  AND ever_in_ca_pharmacy = 'Y';
