import psycopg2

creds = {
    'RDS_HOST': 'data-handling-project-readonly.cq2e8n0558s.eu-west-1.rds.amazonaws.com',
    'RDS_PASSWORD': 'AiCore2022',
    'RDS_USER': 'aicore_admin',
    'RDS_DATABASE': 'postgres',
    'RDS_PORT': 5432
}

connection = None

try:
    connection = psycopg2.connect(
        user=creds['RDS_USER'],
        password=creds['RDS_PASSWORD'],
        host=creds['RDS_HOST'],
        port=creds['RDS_PORT'],
        database=creds['RDS_DATABASE']
    )
    cursor = connection.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    tables = cursor.fetchall()
    for table in tables:
        print(table)
except Exception as e:
    print(f"Error: {e}")
finally:
    if connection:
        cursor.close()
        connection.close()