
CREATE DATABASE order_processing;
USE order_processing;

CREATE TABLE Product
(
ID int identity primary key,
Product_name nvarchar(50),
Price float,
Origin nvarchar(20)
)

CREATE TABLE Orders
(
ID int identity primary key,
Bill_id int,
Product_id int foreign key references Product(ID),
qty int,
sale float,
payment bit
)

--DROP TABLE Orders