import psycopg2

def get_db_connection():
    return psycopg2.connect(
        dbname='mediation_simulator',
        user='postgres',
        password='password',
        host='localhost',
        port='5432'
    )