This python 3.x script queries a Redshift database for all the non-pg schemas and updates a table with counts of tables in each schema.

Before you run this script, create the table that stores the data:

`create table [settings.target_schema].[settings.target_table] 
(schemaname varchar(40), 
table_count int, 
ts timestamp)`
