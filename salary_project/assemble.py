# -*- coding: utf-8 -*-
"""
Created on Tue Jun  1 13:04:45 2021

@author: admin
"""

from database_file import create_table, insert_into_database, parse_csv_from_database
create_table()#it creates table in database
insert_into_database()# it takes lists or tuples of listed as argument and insert into a table if exists 
parse_csv_from_database()
