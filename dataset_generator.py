from faker import Faker
import random
import psycopg2 as ps

from dataset_settings import (COUNTRIES,
                              COUNT_CUSTOMERS,
                              COUNT_PRODUCTS,
                              CLOTHES_NAMES,
                              SHOES_NAMES,
                              GROUPS)

fake = Faker('en_US')

PG_URI = 'postgres://admin:admin@0.0.0.0:5433/oper'


def fake_dataset():

    conn = ps.connect(PG_URI)
    cur = conn.cursor()

    for i in range(15000):
        cur.execute(f"INSERT INTO oper_fact_sales(customer_id, product_id, qty) "
                    f"VALUES ({random.randint(1, COUNT_CUSTOMERS)}, "
                    f"{random.randint(1, COUNT_PRODUCTS)},"
                    f"{random.randint(1, 100)}) ")

    conn.commit()
    conn.close()


def fake_dimensions():

    conn = ps.connect(PG_URI)
    cur = conn.cursor()

    for i in range(100):
        cur.execute(f"INSERT INTO oper_dim_customers(name, country) "
                    f"VALUES ('{fake.name()}', '{random.choice(COUNTRIES)}')")

    for cloth in CLOTHES_NAMES:
        cur.execute(f"INSERT INTO oper_dim_products(name, group_name) "
                    f"VALUES ('{cloth}', '{GROUPS[0]}')")
    for shoe in SHOES_NAMES:
        cur.execute(f"INSERT INTO oper_dim_products(name, group_name) "
                    f"VALUES ('{shoe}', '{GROUPS[1]}')")

    conn.commit()
    conn.close()


# fake_dimensions()
fake_dataset()
