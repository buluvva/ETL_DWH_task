DROP TABLE stg_fact_sales;
DROP TABLE stg_dim_customers;
DROP TABLE stg_dim_products;
DROP TABLE stg_high_water_mark;

CREATE TABLE stg_dim_customers(
    id BIGSERIAL PRIMARY KEY,
    "name" text ,
    "country" text
);

CREATE TABLE stg_dim_products(
    id BIGSERIAL PRIMARY KEY,
    "name" text,
    "group_name" text
);

CREATE TABLE stg_fact_sales(
    id BIGSERIAL PRIMARY KEY,
    "customer_id" int REFERENCES stg_dim_customers (id),
    "product_id" int REFERENCES stg_dim_products (id),
    "qty" int
);

CREATE TABLE stg_high_water_mark(
    id BIGSERIAL PRIMARY KEY,
    "last_update" bigint DEFAULT 0,
    "table_name" text,
    "table_cols" text
);

INSERT INTO stg_high_water_mark("table_name", "table_cols") VALUES('customers', 'name, country');
INSERT INTO stg_high_water_mark("table_name", "table_cols") VALUES('products', 'name, group_name');
INSERT INTO stg_high_water_mark("table_name", "table_cols") VALUES('sales', 'customer_id, product_id, qty');
