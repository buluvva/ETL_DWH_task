DROP TABLE dwh_fact_sales;
DROP TABLE dwh_dim_customers;
DROP TABLE dwh_dim_products;
DROP TABLE dwh_high_water_mark;

CREATE TABLE IF NOT EXISTS dwh_dim_customers(
    id BIGSERIAL PRIMARY KEY,
    "name" text ,
    "country" text
);

CREATE TABLE IF NOT EXISTS dwh_dim_products(
    id BIGSERIAL PRIMARY KEY,
    "name" text,
    "group_name" text
);

CREATE TABLE IF NOT EXISTS dwh_fact_sales(
    id BIGSERIAL PRIMARY KEY,
    "customer_id" int REFERENCES dwh_dim_customers (id),
    "product_id" int REFERENCES dwh_dim_products (id),
    "qty" int
);

CREATE TABLE IF NOT EXISTS dwh_high_water_mark(
    id BIGSERIAL PRIMARY KEY,
    "last_update" bigint DEFAULT 0,
    "table_name" text,
    "table_cols" text
);

CREATE TABLE IF NOT EXISTS logs_table(
    dag_id text,
    task_name text,
    log_level text,
    start_date text,
    execution_time float4,
    error_text text
);

INSERT INTO dwh_high_water_mark("table_name", "table_cols") VALUES('customers', 'name, country');
INSERT INTO dwh_high_water_mark("table_name", "table_cols") VALUES('products', 'name, group_name');
INSERT INTO dwh_high_water_mark("table_name", "table_cols") VALUES('sales', 'customer_id, product_id, qty');







