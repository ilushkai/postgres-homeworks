-- SQL-команды для создания таблиц
CREATE TABLE employees
(
	employee_id int PRIMARY KEY,
	first_name varchar(15) NOT NULL,
	last_name varchar(15) NOT NULL,
	title varchar(60) NOT NULL,
	birth_date date NOT NULL,
	notes text
);


CREATE TABLE customers
(
	customer_id varchar(20) PRIMARY KEY,
	company_name varchar(40) NOT NULL, 
	contact_name varchar(50) NOT NULL
);


CREATE TABLE orders
(
	order_id int PRIMARY KEY,
	customer_id varchar(20) REFERENCES customers(customer_id),
	employee_id int REFERENCES employees(employee_id),
	order_date date NOT NULL,
    ship_city varchar(40) NOT NULL
);

