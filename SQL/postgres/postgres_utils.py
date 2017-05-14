import psycopg2
from psycopg2.extras import RealDictCursor


def psql_db_connect(db_name):
    """
        Aim: Connects to Postgres databases
        Input:
                db_name: database_name, type:string
        Output:
                conn: database connection, type psycopg2 connection object
    """

    if db_name == "db_name":
        conn_string = '''host=127.0.0.1 dbname=db_name user=dsl_readonly password=dsl port=5434'''
    else:
        conn_string = '''host=127.0.0.1 dbname=''' + db_name + \
            ''' user=user password=password port=5433'''
    conn = psycopg2.connect(conn_string)
    return conn


def query_over_psql(db_name, query):
    """
        Aim: Executes psql query
        Input:
                db_name: database_name, type:string
                query: psql query to be executed, type:string
        Output:
                rows: result of the query execution. type:list of dicts with
                key as colums selected and corresponding values

    """

    connection = psql_db_connect(db_name)
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows
