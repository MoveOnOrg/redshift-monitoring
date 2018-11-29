#!usr/bin/python
import settings
import psycopg2

# before running this code, 
# be sure to run the create table statement described in the README

def get_current_schemas(cursor):
    query = """select nspname from pg_namespace where nspname not like 'pg_%' 
    and nspname != 'information_schema';"""
    cursor.execute(query)
    schemas = [r[0] for r in cursor.fetchall()]
    return schemas

def get_current_table_counts(cursor, schemas):
    table_counts = []
    for schema in schemas:
        query = """set search_path = '{0}';  
        select count(distinct tablename) from pg_table_def where schemaname = '{0}';
        """.format(schema)
        cursor.execute(query)
        r = cursor.fetchall()
        table_counts.append((schema, r[0][0]))
    return table_counts

def write_table_counts_to_redshift(cursor, table_counts):
    data_string_list = []
    for table in table_counts:
        data_string_list.append("('{}', {}, getdate())".format(table[0], table[1]))
    data_string = ','.join(data_string_list)
    print(data_string)
    query = "insert into {}.{} values {};".format(
        settings.target_schema, settings.target_table, data_string)
    cursor.execute(query)
    return True

def write_table_counts_to_csv(table_counts):
    #TODO
    return True

if __name__ == '__main__':
    conn = psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        user=settings.DB_USER,
        password=settings.DB_PWD,
        database=settings.DB_NAME
    )
    with conn:
        with conn.cursor() as cursor:
            schemas = get_current_schemas(cursor)
            table_counts = get_current_table_counts(cursor, schemas)
            write_table_counts_to_redshift(cursor, table_counts)
    conn.close()
