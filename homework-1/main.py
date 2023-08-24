"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import psycopg2

conn = psycopg2.connect("host=localhost dbname=north user=postgres password=12345")
cur = conn.cursor()

with open('north_data/customers_data.csv', 'r', encoding="utf-8") as f:
    next(f)
    cur.copy_from(f, 'customers', sep=',')

with open('north_data/employees_data.csv', 'r', encoding='utf-8') as csv_file:
    csv_reader = csv.reader(csv_file)
    next(csv_reader)
    for row in csv_reader:
        employee_id, first_name, last_name, title, birth_date, notes = row

        insert_query = f"INSERT INTO {'employees'} (employee_id, first_name, last_name, title, birth_date, notes) VALUES (%s, %s, %s, %s, %s, %s)"
        cur.execute(insert_query, (employee_id, first_name, last_name, title, birth_date, notes))

with open('north_data/orders_data.csv', 'r', encoding="utf-8") as f:
    next(f)
    cur.copy_from(f, 'orders', sep=',')

conn.commit()

# close cursor and connection
cur.close()
conn.close()
