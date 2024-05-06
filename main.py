import os.path

import psycopg2
from src import PostgresDateConverter, CSVToDBParser

SCHEMA_NAME = 'public'
CONFIG_FILENAME = 'env'

# CSV-файлы -> словарь
customers_dict = CSVToDBParser.parse_csv('data/customers.csv')

employees_dict = CSVToDBParser.parse_csv('data/employees.csv')
for employee in employees_dict['data']:
    employee[3] = PostgresDateConverter.convert(employee[3])

orders_dict = CSVToDBParser.parse_csv('data/orders.csv')
for order in orders_dict['data']:
    order[3] = PostgresDateConverter.convert(order[3])


# БД соединение
conn_params = {}
with open(CONFIG_FILENAME, 'r') as file:
    for line in file:
        key, value = line.split(':')
        value = value.replace('\n', '')
        conn_params[key] = value
conn = psycopg2.connect(**conn_params)
cursor = conn.cursor()


# -----Создание таблиц-----
cursor.execute(f"create schema if not exists {SCHEMA_NAME};")
cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.orders")
cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.customers")
cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.employees")

cursor.execute(f"create table {SCHEMA_NAME}.customers("
               f"customer_id char(5) primary key,"
               f"company_name varchar(100) not null,"
               f"contact_name varchar(100) not null)")
cursor.execute(f"create table {SCHEMA_NAME}.employees ("
               f"employee_id serial primary key,"
               f"first_name varchar(25) not null,"
               f"last_name varchar(35) not null,"
               f"title varchar(100) not null,"
               f"birth_date date not null,"
               f"notes text default '')")
cursor.execute(f"create table {SCHEMA_NAME}.orders ("
               f"order_id serial primary key,"
               f"customer_id char(5) references customers(customer_id) not null,"
               f"employee_id int references employees(employee_id) not null,"
               f"order_date date not null,"
               f"ship_city varchar(100) not null)")
conn.commit()
cursor.close()
conn.close()

# -----вставить строки в БД-таблицу-----
CSVToDBParser.insert_into_db(conn_params, SCHEMA_NAME, 'customers', customers_dict)
CSVToDBParser.insert_into_db(conn_params, SCHEMA_NAME, 'employees', employees_dict)
CSVToDBParser.insert_into_db(conn_params, SCHEMA_NAME, 'orders', orders_dict)
