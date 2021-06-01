# -*- coding: utf-8 -*-
"""
Created on Sat May 29 14:41:06 2021

@author: admin
"""
import os
import csv
import mysql.connector as conn
os.chdir(r'C:\Users\admin\OneDrive\web scraping')

mydb = conn.connect(
            host="localhost",
            user="root",
            password="KETHAN@852",
            auth_plugin='mysql_native_password',
            database = 'depart'
            )

c = mydb.cursor()

#this snippet code creates tables based on departments present in csv file
#it reads each and every row if any new department found it will create a new table with respective name
with open('employee.csv','r') as fh:
    columns = csv.DictReader(fh,delimiter='|')#it reads only header line and take these as keys and remaining values    
    departments = [row['Department'] for row in columns]
    departments = list(set(departments))
    print('different departments exists in csv file are', departments)
    for department in departments:
        sq_t = f"create table {department}(name varchar(20), ID varchar(5) UNIQUE, salary DECIMAL(10,2))"
        try:
            c.execute(sq_t)
            print(f'new table created with name: {department}')
        except:
            continue

#retriving all IDs in database if any
IDs_in_exist_database = []
c.execute('show tables')
tables = c.fetchall()
if tables: 
    tables = [table[0] for table in tables]
    print('tables already exist in database:', tables)
    for table in tables:
        c.execute(f'select ID from {table}')
        a = c.fetchall()
        for id in a:
            IDs_in_exist_database.append(id[0])

print('existing IDs in all tables in depart database:', IDs_in_exist_database)

            
#after creating tables it will read each row and insert that data in respective table based on department name
with open('employee.csv','r') as fh:
    reader = csv.reader(fh,delimiter='|')
    data = list(reader)#converting csv reader object to list for easy accessing data
    data = data[1:]#excluding the first line as it is header line
    for i in data:
        if i[1] not in IDs_in_exist_database:
            name = i[0]
            id = i[1]
            salary = float(i[3])
            #for inserting the varchar data type keep in single quotation(') in sql statement
            sql = f"insert into {i[2]}(name,ID,salary) values('{name}','{id}',{salary})"
            #to remove repeated entries of employees used try and except block
            #print(sql)
            c.execute(sql)
            mydb.commit()#saving to database
            print(f'one new row insertd in {i[2]} table with ID-{id}')
    
        