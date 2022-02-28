import psycopg2
from psycopg2.extensions import connection, cursor
import logging
from connection_details import HOST, DBNAME, USER, PASSWORD

# DDL_us_zipcodes = ['DROP TABLE public.us_zipcodes', 'DROP TABLE public.staging_us_zipcodes']

COPY_SQL_STATEMENT = """
    COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','
    """


def create_connection(host, dbname, user, password):
    """
    :param host: Database Host
    :param dbname: Database Name
    :param user: Database User
    :param password: Database Password
    :return: Cursor, Connection Objects
    """
    # Connect to the given database
    conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    return cur, conn


def execure_query(cur, all_queries, fetch=False):
    """
    :param cur: PG Cursor object
    :param all_queries: queries as Python List
    :param fetch: boolean to fetch results or not
    :return: None
    """
    # Execute query in a loop
    for qry in all_queries:
        logging.info("Query Execution Start, Query: {qry} ".format(qry=qry))
        try:
            print(qry)
            cur.execute(qry)
            if fetch:
                results = cur.fetchall()
                for row in results:
                    logging.info(row)
                    print(row)
        except Exception as error:
            logging.info("An exception has occured while executing SQL query:", error)
        finally:
            logging.info("Query executed Successfully")


def load_csv_data(cur: cursor, file_path, table_name):
    """
    :param cur: PG Cursor Object
    :param file_path: File Path
    :param table_name: PG Table Name
    :return: None
    """
    # copy file into the table just created
    with open(file_path, "r") as file:
        cur.copy_expert(sql=COPY_SQL_STATEMENT % table_name, file=file)


def main():

    # Create Cursor and Connection
    cur, conn = create_connection(
        host=HOST, dbname=DBNAME, user=USER, password=PASSWORD
    )

    # Create Stage Table
    with open("ddl_Scripts/public.staging_us_zipcodes.sql", "r") as file:
        sql_text = file.read()
        execure_query(cur, all_queries=[sql_text])

    # Create Target table
    with open("ddl_Scripts/public.us_zipcodes.sql", "r") as file:
        sql_text = file.read()
        execure_query(cur, all_queries=[sql_text])

    # Verify Newly created Tables
    execure_query(
        cur,
        all_queries=[
            "SELECT * FROM information_schema.tables where table_schema = 'public';"
        ],
        fetch=True,
    )

    # Truncate Stage Table
    execure_query(cur, all_queries=["TRUNCATE TABLE public.staging_us_zipcodes"])

    # Load Stage table from CSV File
    load_csv_data(
        cur,
        file_path="./sample_us_zipcodes.csv",
        table_name="public.staging_us_zipcodes",
    )

    # Run SQL Transactions to load from Stage Table to Target Table
    with open("./Transformations/public.us_zipcodes_transformations.sql", "r") as file:
        sql_text = file.read()
        execure_query(cur, all_queries=[sql_text])

    # Capture Row counts in Stage and Target Table
    execure_query(
        cur, all_queries=["select count(1) from public.staging_us_zipcodes"], fetch=True
    )
    execure_query(
        cur, all_queries=["select count(1) from public.us_zipcodes;"], fetch=True
    )

    # Run Data Validations
    with open("./sample_queries/sample_test_queries.sql", "r") as file:
        sql_text = file.read()
        execure_query(cur, all_queries=[sql_text.split(";")], fetch=True)

    # Close Database Connection
    conn.close()


if __name__ == "__main__":
    main()
