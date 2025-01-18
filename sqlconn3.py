import os
import pymysql
import pyodbc
import pandas as pd
import logging
from sshtunnel import SSHTunnelForwarder

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Environment variables
mysql_config = {
    "host": os.environ.get("MSH"),
    "user": os.environ.get("MSU"),
    "password": os.environ.get("MSP"),
    "database": os.environ.get("MSN"),
}
ssh_host = os.environ.get("SSH")
ssh_user = os.environ.get("SSU")
ssh_key_path = "./ssh_key.pem"
db_host = os.environ.get("DBH")

azure_sql_config = {
    "server": os.environ.get("AZURE_SERVER"),
    "database": os.environ.get("AZURE_DATABASE"),
    "username": os.environ.get("AZURE_USER"),
    "password": os.environ.get("AZURE_PASSOWRD"),
    "driver": "ODBC Driver 17 for SQL Server",
}

# Debug secrets
logging.info(f"SSH_HOST: {ssh_host}")
logging.info(f"DB_HOST: {mysql_config['host']}")
logging.info(f"AZURE_SQL_SERVER: {azure_sql_config['server']}")

# Fetch max duration data from Azure SQL
def fetch_max_duration_data():
    try:
        conn_str = (
            f"DRIVER={{{azure_sql_config['driver']}}};"
            f"SERVER={azure_sql_config['server']};"
            f"DATABASE={azure_sql_config['database']};"
            f"UID={azure_sql_config['username']};"
            f"PWD={azure_sql_config['password']};"
        )
        logging.info(f"Azure SQL Connection String: {conn_str}")
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            query = """
                SELECT TOP 1 YEAR(date) AS max_year, date, time
                FROM extra_staff.notes
                ORDER BY YEAR(date) DESC, date DESC, time DESC
            """
            cursor.execute(query)
            result = cursor.fetchone()
            if result:
                return result.max_year, f"{result.date} {result.time}"
            else:
                return None, None
    except Exception as e:
        logging.error(f"Error connecting to Azure SQL: {e}")
        raise

# Fetch data from MySQL
def fetch_mysql_data(max_year, max_date_time):
    try:
        with SSHTunnelForwarder(
            (ssh_host, 22),
            ssh_username=ssh_user,
            ssh_private_key=ssh_key_path,
            remote_bind_address=(db_host, 3306),
        ) as tunnel:
            connection = pymysql.connect(
                host="127.0.0.1",
                port=tunnel.local_bind_port,
                user=mysql_config["user"],
                password=mysql_config["password"],
                database=mysql_config["database"],
            )
            logging.info("Connected to MySQL")
            query = f"""
                SELECT id, Creator_Id, Type, Date, Time, Name
                FROM notes
                WHERE YEAR(Date) >= '{max_year}'
                  AND CONCAT(date, ' ', time) > '{max_date_time}'
            """
            df = pd.read_sql(query, connection)
            return df
    except Exception as e:
        logging.error(f"Error fetching data from MySQL: {e}")
        raise

# Main function
def main():
    max_year, max_date_time = fetch_max_duration_data()
    if not max_year or not max_date_time:
        logging.warning("No max year or max date/time fetched from Azure SQL.")
        return
    df = fetch_mysql_data(max_year, max_date_time)
    logging.info(f"Fetched {len(df)} rows from MySQL.")
    # Additional upload steps to Azure SQL if needed...

if __name__ == "__main__":
    main()
