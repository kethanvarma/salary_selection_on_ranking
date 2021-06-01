# -*- coding: utf-8 -*-
"""
Created on Sun May 30 14:04:22 2021

@author: admin
"""

import commentjson
import mysql.connector as conn
import os
import csv
from detect_delimiter import detect
import sys

f = open("config.json",)
data = commentjson.load(f)
database_config = data['database_configuration']

mydb = conn.connect(
                     host=database_config['host'],
                     user=database_config['user'],
                     password=database_config['password'],
                     auth_plugin=database_config['auth_plugin'],
                     database = database_config['database']
                         )


cur = mydb.cursor(buffered=True)


#csv file details
file_path = os.path.join(data['csv_file_details']['abspath'],data['csv_file_details']['csv_file_name'])

csv_filename_format = f"""first_{data['selection_basis']['rank_number']}_{data['selection_basis']['rank_selection']}_in_{data['selection_basis']['arrange']}_order_{data['selection_basis']['group_by']} wise-\
"""
#print(csv_filename_format)

#function for creating table
def create_table(table_name=data['table_name'], colnames=data['colnames']):
    if not isinstance(table_name, str):
        sys.exit('table_name data type should be str')
    
    if not (isinstance(colnames,tuple) or isinstance(colnames,list)):
        sys.exit("colnames parameter should be a list or tuple")
    
    #checking table existence in database
    cur.execute('show tables')
    tables = cur.fetchall()
    if tables: 
        tables = [table[0] for table in tables]
    if table_name not in tables:
        sql = f"create table {table_name}({','.join(colnames)})"
        print(sql)
        cur.execute(sql)
        print(f"new table: {table_name} was created in {data['database_configuration']['database']} database")
    else:
        print(f"table with name: {table_name} already exists in {data['database_configuration']['database']} database")



def read_csv(filepath=file_path,delimiter=data['csv_file_details']['delimiter']):
    with open(filepath,'r') as fh:
        text = fh.readlines()
        text = text[3]#any one number to get delimiter
        a = detect(text,default=',', whitelist=[',', ';', ':', '|', '\t','.'], blacklist=['.'])
        #print(a)
        if a != delimiter:
            sys.exit(f"""
                     given '{delimiter}' delimiter is not matching with the delimiter '{a}' in CSV file.
                     check default delimitir in config.json and read_csv() delimiter argument.
                     
                     """)
        
        else:
            with open(filepath,'r') as fh:
                csv_data = csv.DictReader(fh,delimiter=delimiter)
                rows=[]
                for row in csv_data:
                    rows.append(list(row.values()))
                return rows
        
    
def insert_into_database(table_name=data['table_name'], values = read_csv()):
    # checking input data type
    if not isinstance(table_name, str):
        sys.exit('table_name data type should be str')
    
    if not (isinstance(values, list) or isinstance(values, tuple)):
        sys.exit('values data type should be List of (tuples or lists items) OR Tuples of (tuples or lists items)')
    
    #checking table existence in database and columns
    cur.execute('show tables')
    tables = [i[0] for i in cur.fetchall()]
    if table_name not in tables:
        sys.exit(f"""
                 given table'{table_name}' not exist in database if you want
                 to create the table call this function 'create_table' with 
                 2 parameters of table_name and colnames
                 """)
        
    else:
         cur.execute(f'desc {table_name}')
         columns = [col[0] for col in cur.fetchall()]
    
    placeholders = ','.join(len(columns)*['%s'])
    insert_sql = f"insert into {table_name} values({placeholders})"
    #print(insert_sql)
    for j in range(len(values)):
        if not (type(values[j]) == tuple or type(values[j]) == list):
            sys.exit('the values items for inserting into database shoulde be given in tuple or list format')
        if not len(columns) == len(values[j]):
            sys.exit(f"""
                     number of columns and items not matching for inserting the data 
                     in table {table_name} at '{j}'th index item of values
                     """)
        
        try:
            cur.execute(insert_sql,values[j])
            mydb.commit()
        except:
            continue 


def parse_csv_from_database(
        DBTable_name=data['table_name'], #table name in database name
        rank_selection=data['selection_basis']['rank_selection'], #column name on which selection is processed
        colwise_colname=data['selection_basis']['group_by'], # column name to retrive different departments
        number_of_rows_selection=data['selection_basis']['rank_number'], # integer to select top number numbers based on arrange_order
        arrange_order=data['selection_basis']['arrange'] #parameter to decide ascending or descending         
                       ):
    #checking datatype
    if not isinstance(DBTable_name,str):
        sys.exit("incorrect data type for parameter 'DBTable_name', it should be string")
    if not isinstance(rank_selection,str):
        sys.exit("incorrect data type for parameter 'rank_selection' it should be string")
    if not isinstance(colwise_colname,str):
        sys.exit("incorrect data type for parameter 'colwise_colname' it should be string")
    if not isinstance(colwise_colname,str):
        sys.exit("incorrect data type for parameter 'colwise_colname' it should be string")
    if not isinstance(int(number_of_rows_selection),int):
        sys.exit("incorrect data type for parameter 'number_of_rows_selection' it should be Integer")
        
    
    '''
    first checking of existence  of given table name and columns database raise error if table not exists
    '''    
    cur.execute('show tables')
    tables = [i[0] for i in cur.fetchall()]
    if DBTable_name not in tables:
        sys.exit(f"given table'{DBTable_name}' not exist in database ")
    else:
        cur.execute(f'desc {DBTable_name}')
        columns = [col[0] for col in cur.fetchall()]
        if rank_selection not in columns and colwise_colname not in columns:
            sys.exit(f"the columns '{rank_selection}', '{colwise_colname}' : not exists in '{DBTable_name}' table")
    
    """ checking of arrange_order parameter"""
    if not arrange_order.strip().upper() in ['DESC','ASC']:
        sys.exit(f"incorrect  arrange : '{arrange_order}', arrange_order it should be either string of 'DESC' or 'ASC'")
    
    
    sql = f"select {colwise_colname} from {DBTable_name} group by {colwise_colname}"
    cur.execute(sql)
    #print(sql)
    departments = [i[0] for i in cur.fetchall()]   
    #print(departments)
    
    
    for i in departments:
        sql_salary = f"""select distinct {rank_selection} 
                from {DBTable_name} 
                where {colwise_colname} = '{i}' 
                order by {data['selection_basis']['order_by']} 
                {arrange_order}
                LIMIT {number_of_rows_selection}
                """
        #print(sql_salary)
        cur.execute(sql_salary)
        high_salaries = tuple([float(r[0]) for r in cur.fetchall()])
        #print(rows,22222)
        sq1_sal_sel = f"""select * from {DBTable_name}
                   where {rank_selection} in {(high_salaries)}
                   and {colwise_colname} = '{i}'
                   order by {rank_selection} {arrange_order}
                 """
        #print(sq1_sal_sel)
        cur.execute(sq1_sal_sel)
        values = cur.fetchall()
        #print(values)    
        #parsing csv files
        filename = f"{csv_filename_format}{i}.csv"
        
        with open(f'{filename}','x',newline='') as csv_fh:
            cur.execute(f"desc {DBTable_name}")
            #print(filename)
            writer = csv.DictWriter(csv_fh,
            delimiter=data['csv_file_details']['delimiter_to_parse_csv_file'],
                                    fieldnames=columns)
            writer.writeheader()
            for value in values:
                dict_rows = {}
                for i in list(zip(columns,value)):
                    dict_rows[i[0]] = i[1]
                #print(dict_rows)
                writer.writerow(dict_rows)
                
                
        
#parse_csv_from_database()
    
#print(read_from_database(number_of_rows_selection=4,arrange_order='asc'))

#rint(10*'-----')
#testing the above functions
#a = read_csv(filepath=r'C:\Users\admin\Desktop\New folder\temp.csv',delimiter=',')
#print(a)
#colnames = ['name varchar(10)', 'id varchar(5)']
#create_table(table_name='tempora',colnames=colnames)