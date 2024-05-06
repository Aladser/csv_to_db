import psycopg2
from src import CSVLoader

# csv-файлы
customers_dict = CSVLoader.load('data/customers.csv')
employees_dict = CSVLoader.load('data/employees.csv')
orders_dict = CSVLoader.load('data/orders.csv')

# бд соединение
conn_params = {
    'host': 'localhost',
    'port': '5432',
    'dbname': 'pydb171',
    'user': 'postgres',
    'password': 'Database_1821'
}
conn = psycopg2.connect(**conn_params)
cursor = conn.cursor()


# Не меняйте и не удаляйте эти строки - они нужны для проверки
cursor.execute("create schema if not exists itresume4105;")
cursor.execute("DROP TABLE IF EXISTS itresume4105.orders")
cursor.execute("DROP TABLE IF EXISTS itresume4105.customers")
cursor.execute("DROP TABLE IF EXISTS itresume4105.employees")

# -----запросы для создания таблиц-----
cursor.execute(f"create table itresume4105.customers("
            f"customer_id char(5) primary key,"
            f"company_name varchar(100) not null,"
            f"contact_name varchar(100) not null)")
cursor.execute(f"create table itresume4105.employees ("
            f"employee_id serial primary key,"
            f"first_name varchar(25) not null,"
            f"last_name varchar(35) not null,"
            f"title varchar(100) not null,"
            f"birth_date date not null,"
            f"notes text default '')")
cursor.execute(f"create table itresume4105.orders ("
            f"order_id serial primary key,"
            f"customer_id char(5) references customers(customer_id) not null,"
            f"employee_id int references employees(employee_id) not null,"
            f"order_date date not null,"
            f"ship_city varchar(100) not null)")
conn.commit()

# -----Теперь приступаем к операциям вставок данных-----
# Запустите цикл по списку customers_data и выполните запрос формата 
# INSERT INTO table (column1, column2, ...) VALUES (%s, %s, ...) returning *", data)
# В конце каждого INSERT-запроса обязательно должен быть оператор returning *
for customer in customers_data:
    query = f"insert into itresume4105.customers ({', '.join(customers_header)}) values({', '.join(['%s'] * len(customer))}) returning *"
    cursor.execute(query, customer)

# Не меняйте и не удаляйте эти строки - они нужны для проверки
conn.commit()
res_customers = cursor.fetchall()

# Запустите цикл по списку employees_data и выполните запрос формата 
# INSERT INTO itresume4105.table (column1, column2, ...) VALUES (%s, %s, ...) returning *", data)
# В конце каждого INSERT-запроса обязательно должен быть оператор returning *
for employer in employees_data:
    query = f"insert into itresume4105.employees ({', '.join(employees_header)}) values({', '.join(['%s'] * len(employer))}) returning *"
    cursor.execute(query, employer)

# Не меняйте и не удаляйте эти строки - они нужны для проверки
conn.commit()
res_employees = cursor.fetchall()

# Запустите цикл по списку orders_data и выполните запрос формата
# INSERT INTO itresume4105.table (column1, column2, ...) VALUES (%s, %s, ...) returning *", data)
# В конце каждого INSERT-запроса обязательно должен быть оператор returning *
for order in orders_data:
    query = f"insert into itresume4105.orders ({', '.join(orders_header)}) values({', '.join(['%s'] * len(order))}) returning *"
    cursor.execute(query, order)

# Не меняйте и не удаляйте эти строки - они нужны для проверки
conn.commit()
res_orders = cursor.fetchall()

# Закрытие курсора
cursor.close()
