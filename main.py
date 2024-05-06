import psycopg2
from src import CSVLoader, PostgresDateConverter

SCHEMA_NAME = 'public'

# CSV-файлы
customers_dict = CSVLoader.parse('data/customers.csv')

employees_dict = CSVLoader.parse('data/employees.csv')
for employee in employees_dict['data']:
    employee[3] = PostgresDateConverter.convert(employee[3])

orders_dict = CSVLoader.parse('data/orders.csv')
for order in orders_dict['data']:
    order[3] = PostgresDateConverter.convert(order[3])


# БД соединение
conn_params = {
    'host': 'localhost',
    'port': '5432',
    'dbname': 'pydb171',
    'user': 'postgres',
    'password': 'Database_1821'
}
conn = psycopg2.connect(**conn_params)
cursor = conn.cursor()
"""Курсор"""

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


# Заполнение customers
for customer in customers_dict['data']:
    query = (f"insert into {SCHEMA_NAME}.customers ({customers_dict['header']}) "
             f"values({', '.join(['%s'] * len(customer))}) returning *")
    cursor.execute(query, customer)
conn.commit()
# Заполнение employees
for employer in employees_dict['data']:
    query = (f"insert into {SCHEMA_NAME}.employees ({employees_dict['header']}) "
             f"values({', '.join(['%s'] * len(employer))}) returning *")
    cursor.execute(query, employer)
conn.commit()
# Заполнение orders
for order in orders_dict['data']:
    query = (f"insert into {SCHEMA_NAME}.orders ({orders_dict['header']}) "
             f"values({', '.join(['%s'] * len(order))}) returning *")
    cursor.execute(query, order)
conn.commit()


cursor.close()
conn.close()