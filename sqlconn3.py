import os
import pymysql
import pyodbc
import pandas as pd
import logging
from sshtunnel import SSHTunnelForwarder

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Debug environment variables
logging.info(f"AZURE_SERVER: {os.environ.get('AZURE_SERVER')}")
logging.info(f"AZURE_DATABASE: {os.environ.get('AZURE_DATABASE')}")
logging.info(f"AZURE_USER: {os.environ.get('AZURE_USER')}")
logging.info(f"AZURE_PASSWORD: {os.environ.get('AZURE_PASSOWRD')}")
logging.info(f"SSH_HOST: {os.environ.get('SSH')}")
logging.info(f"DB_HOST: {os.environ.get('DBH')}")
logging.info(f"MYSQL_USER: {os.environ.get('MSU')}")
logging.info(f"MYSQL_PASSWORD: {os.environ.get('MSP')}")
logging.info(f"MYSQL_DB: {os.environ.get('MSN')}")

# MySQL Configuration
mysql_config = {
    "host": os.environ.get("MSH"),
    "user": os.environ.get("MSU"),
    "password": os.environ.get("MSP"),
    "database": os.environ.get("MSN"),
}

# SSH Configuration
ssh_host = os.environ.get("SSH")
ssh_user = os.environ.get("SSU")
ssh_key_path = "./ssh_key.pem"
db_host = os.environ.get("DBH")

# Azure SQL Configuration
azure_sql_config = {
    "server": os.environ.get("AZURE_SERVER"),
    "database": os.environ.get("AZURE_DATABASE"),
    "username": os.environ.get("AZURE_USER"),
    "password": os.environ.get("AZURE_PASSOWRD"),
    "driver": "ODBC Driver 17 for SQL Server",
}

# Azure SQL Table Name
azure_table_name = "extra_staff.notes"
batch_size = 1000

def fetch_max_duration_data():
    """Fetch max duration data from Azure SQL."""
    logging.info("Starting data processing workflow...")
    conn_str = (
        f"DRIVER={{{azure_sql_config['driver']}}};"
        f"SERVER={azure_sql_config['server']};"
        f"DATABASE={azure_sql_config['database']};"
        f"UID={azure_sql_config['username']};"
        f"PWD={azure_sql_config['password']};"
    )
    logging.info(f"Azure SQL Connection String: {conn_str}")
    try:
        with pyodbc.connect(conn_str) as azure_conn:
            cursor = azure_conn.cursor()
            select_max_duration_query = """
            SELECT TOP 1
            YEAR(date) AS max_year,
            date AS max_date,
            time AS max_time
            FROM extra_staff.notes
            ORDER BY YEAR(date) DESC, date DESC, time DESC
            """
            cursor.execute(select_max_duration_query)
            max_duration_query_result = cursor.fetchone()
            max_year = max_duration_query_result.max_year
            max_date_time = str(max_duration_query_result.max_date) + " " + str(max_duration_query_result.max_time)
            return max_year, max_date_time
    except Exception as e:
        logging.error(f"Error connecting to Azure SQL: {e}")
        raise e

def fetch_mysql_data(max_year, max_date_time):
    """Fetch data from MySQL."""
    mysql_query = f"""
    SELECT id, Creator_Id, Type, Date, Time, Name
    FROM notes
    WHERE YEAR(Date) >= '{max_year}'
    AND CONCAT(date, ' ', time) > '{max_date_time}'
    AND YEAR(Date) NOT IN (5520, 2035)
    """
    try:
        with SSHTunnelForwarder(
            (ssh_host, 22),
            ssh_username=ssh_user,
            ssh_private_key=ssh_key_path,
            remote_bind_address=(db_host, 3306)
        ) as tunnel:
            connection = pymysql.connect(
                host=mysql_config['host'],
                port=tunnel.local_bind_port,
                user=mysql_config['user'],
                password=mysql_config['password'],
                database=mysql_config['database']
            )
            logging.info("Connected to MySQL.")
            df = pd.read_sql(mysql_query, connection)
            df['Time'] = df['Time'].apply(lambda x: str(x).split('days')[-1])
            logging.info(f"Fetched {len(df)} rows from MySQL.")
            return df
    except Exception as e:
        logging.error(f"Error fetching data from MySQL: {e}")
        return None

def upload_to_azure_sql(df):
    """Upload data to Azure SQL."""
    try:
        conn_str = (
            f"DRIVER={{{azure_sql_config['driver']}}};"
            f"SERVER={azure_sql_config['server']};"
            f"DATABASE={azure_sql_config['database']};"
            f"UID={azure_sql_config['username']};"
            f"PWD={azure_sql_config['password']};"
        )
        with pyodbc.connect(conn_str) as azure_conn:
            cursor = azure_conn.cursor()
            columns = ", ".join(df.columns)
            placeholders = ", ".join(["?"] * len(df.columns))
            query = f"INSERT INTO {azure_table_name} ({columns}) VALUES ({placeholders})"
            for start in range(0, len(df), batch_size):
                batch = df.iloc[start : start + batch_size]
                data = [tuple(row) for row in batch.to_numpy()]
                cursor.executemany(query, data)
            azure_conn.commit()
            logging.info("Data uploaded successfully to Azure SQL.")
    except Exception as e:
        logging.error(f"Error uploading data to Azure SQL: {e}")

def main():
    try:
        max_year, max_date_time = fetch_max_duration_data()
        df = fetch_mysql_data(max_year, max_date_time)
        if df is not None and not df.empty:
            upload_to_azure_sql(df)
        else:
            logging.warning("No data to upload.")
    except Exception as e:
        logging.error(f"Error in main workflow: {e}")

if __name__ == "__main__":
    main()
