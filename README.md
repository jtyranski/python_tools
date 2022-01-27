# python_tools
These are python tools I created for various projects I've worked on

---

## cExtract.py
- Created 08/08/2019
Created this tool, to extract a single contractor's data from a shared database

### Arguments
- Requires a command argument of masterid which is the contractors unique id
- Optional command argument --table to extract a single table

### Process
- Checks and removes any copied tables from previous runs (table namess appended with '_copy')
- Gets a list of all table names
- Creates empty copies of tables with appended '_copy' after table name
- Creates a list of 'INSERT SELECT' queries to insert data with unique master_id info into table copies
- Loop through query list and execute each
- Loop through original table list to execute drop
- Rename the tables with '_copy' name appended to original table name
- Create a database dump on the new tables
- Compress the database dump to gzip format

---

## mysql-to-csv.py
- Created 08/06/2019
Dump all tables in database to csv files from an single contractors extracted data (cExtract.py)

### Process
- Get a list of all table names from the database
- Creates a directory for the table
- writes all data from a table into a comma delimited file (writing field names as first row)
- Compresses the directory with csv files into a zip format
- Removes the 'tables' directory
