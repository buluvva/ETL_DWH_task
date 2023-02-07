CREATE TABLE IF NOT EXISTS oper_dim_customers(
    id BIGSERIAL PRIMARY KEY,
    "name" text ,
    "country" text
);

CREATE TABLE IF NOT EXISTS oper_dim_products(
    id BIGSERIAL PRIMARY KEY,
    "name" text,
    "group_name" text
);
DROP table oper_fact_sales
CREATE TABLE IF NOT EXISTS oper_fact_sales(
    id BIGSERIAL PRIMARY KEY,
    "customer_id" int REFERENCES oper_dim_customers (id),
    "product_id" int REFERENCES oper_dim_products (id),
    "qty" int
);


SELECT MAX(id) FROM oper_fact_sales;