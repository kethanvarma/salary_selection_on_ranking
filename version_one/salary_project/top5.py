# -*- coding: utf-8 -*-
"""
Created on Sun May 30 00:03:39 2021

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
#collecting tables present in database
c.execute('show tables')
tables_list = c.fetchall()
tables = []
for i in tables_list:
    tables.append(i[0])
#retriving data from each table through looping
for table in tables:
    sql = f'select * from {table}'
    c.execute(sql)
    rows = c.fetchall()
    #collecting rows based on salary using set data type to remove duplicate
    salary = set()
    for i in rows:
        a = float(i[2])
        salary.add(a)
    
    l = list(salary)#converting set to list
    l.sort(reverse=True)#arranging the values in desc order by sort method in list
    l = l[:5]#taken top 5 greatest values
    
    #based on top 5 salary values fetching data from database and
    #inserting the data in CSV file.
    
    #creating SQL syntax and collect all rows based on salary
    data = []
    for i in l:
        sql1 = f'select * from {table} where salary = {i}'
        c.execute(sql1)
        data.extend(c.fetchall())
    
    #creating header names as it is column names present in database 
    c.execute(f'desc {table}')
    col_names = c.fetchall()
    header_names = []
    for col_name in col_names:
        header_names.append(col_name[0])
    
    #writing data into csv file
    with open(f'top5_salary_in_{table}_department','w',newline='') as fh:
        writer = csv.DictWriter(fh,delimiter='|',fieldnames=header_names)
        writer.writeheader()
        for row in data:
            insert_data = {header_names[0]:row[0], header_names[1]:row[1], header_names[2]:row[2]}
            writer.writerow(insert_data)
            





