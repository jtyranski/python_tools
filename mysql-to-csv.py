"""
mysql-to-csv.py
summary: Dump all tables in localhost database to csv files
created by: Jim Tyranski
created: 08/06/2019
"""

import mysql.connector
import sys, os, shutil
import csv

# database variables
dbhost = 'localhost'
dbuser = ''
dbpass = ''
dbname = ''

# connect to the database
try:
    mydb = mysql.connector.connect(
        host = dbhost,
        user = dbuser,
        passwd = dbpass,
        db = dbname
    )
except:
    print('Error Connecting to database: ' + dbname)
    os._exit(1)

cursor = mydb.cursor(buffered=True)

# get master_id to use in the filename
sql = """SELECT master_id FROM master_list WHERE master_id !=1;"""
cursor.execute(sql)
result=cursor.fetchone()
masterid=result[0]

# get table name from database schema
TABLE_QUERY = """
    SELECT TABLE_NAME
    FROM information_schema.tables
    WHERE table_schema='""" + dbname + """';
    """
cursor.execute(TABLE_QUERY)
table_result=cursor.fetchall()
for t in table_result:
    table_name = t[0]

    # check for existing directory, create if not found
    if not os.path.isdir('tables'):
        os.mkdir('tables')

    # open new csv files with table as filename
    with open('tables/' + table_name + '.csv', 'w') as csvfile:
        c = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        QUERY = """
            SELECT *
            FROM """ + table_name + """;
            """
        print('Processing table ' + table_name)
        cursor.execute(QUERY)
        result=cursor.fetchall()

        # write the field names as first row, data following
        field_names = [i[0] for i in cursor.description]
        c.writerow(field_names)
        for data in result:
            c.writerow(data)

# compress the directory with csv files
zipcmd = "zip -r " + "DBtables_" + str(masterid) + ".zip tables"
print(zipcmd)
os.system(zipcmd)
# remove 'tables' directory
shutil.rmtree('tables')

print('Done.')
