import os
from dotenv import load_dotenv
import mysql.connector
import psycopg2

load_dotenv()


def get_pgsql_config():
    return {
        "user": os.getenv("PGSQL_USER"),
        "password": os.getenv("PGSQL_PASSWORD"),
        "host": os.getenv("PGSQL_HOST"),
        "port": int(os.getenv("PGSQL_PORT")),
        "database": os.getenv("PGSQL_DATABASE"),
    }


def get_mysql_config():
    return {
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "host": os.getenv("MYSQL_HOST"),
        "port": int(os.getenv("MYSQL_PORT")),
        "database": os.getenv("MYSQL_DATABASE"),
    }


def main():
    try:
        pgsql_conn = psycopg2.connect(**get_pgsql_config())
        mysql_conn = mysql.connector.connect(**get_mysql_config())

        print(f"Connected to PostgreSQL {pgsql_conn}")
        print(f"Connected to MySQL {mysql_conn}")

    except psycopg2.OperationalError as err:
        print("Connection to PostgreSQL database failed.", err)

    except mysql.connector.errors.ProgrammingError as err:
        print("Connection to MySQL database failed.", err)


if __name__ == "__main__":
    main()
