import os
import pymysql
import pyodbc
import pandas as pd
import logging
from sshtunnel import SSHTunnelForwarder

# Configure logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# Log Environment Variables
logging.info("Environment Variables:")
logging.info(f"SSH_HOST: {os.environ.get('SSH')}")
logging.info(f"SSH_USER: {os.environ.get('SSU')}")
logging.info(f"DB_HOST: {os.environ.get('MSH')}")
logging.info(f"DB_USER: {os.environ.get('MSU')}")
logging.info(f"AZURE_SQL_SERVER: {os.environ.get('AZURE_SERVER')}")
logging.info(f"AZURE_SQL_DATABASE: {os.environ.get('AZURE_DATABASE')}")
logging.info(f"AZURE_SQL_USERNAME: {os.environ.get('AZURE_USER')}")

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
    "password": os.environ.get("AZURE_PASSWORD"),
    "driver": "ODBC Driver 17 for SQL Server",
}

# Azure SQL Table Name
azure_table_name = "extra_staff.notes"
batch_size = 1000  # Number of rows per batch

def fetch_max_duration_data():
    """Fetch the max date and time from Azure SQL."""
    select_max_duration_query = """
    SELECT TOP 1
    YEAR(date) as max_year, 
    date as max_date, 
    time as max_time
    FROM extra_staff.notes
    ORDER BY YEAR(date) DESC, date DESC, time DESC
    """

    # Build connection string
    conn_str = (
        f"DRIVER={{{azure_sql_config['driver']}}};"
        f"SERVER={azure_sql_config['server']};"
        f"DATABASE={azure_sql_config['database']};"
        f"UID={azure_sql_config['username']};"
        f"PWD={azure_sql_config['password']}"
    )

    logging.info("Azure SQL Connection String:")
    logging.info(conn_str)

    try:
        with pyodbc.connect(conn_str) as azure_conn:
            logging.info("Connected to Azure SQL successfully.")
            cursor = azure_conn.cursor()
            cursor.execute(select_max_duration_query)

            max_duration_query_result = cursor.fetchone()
            max_year = max_duration_query_result.max_year
            max_date_time = f"{max_duration_query_result.max_date} {max_duration_query_result.max_time}"
            logging.info(f"Max Year: {max_year}, Max DateTime: {max_date_time}")
            return max_year, max_date_time
    except pyodbc.Error as e:
        logging.error(f"Error connecting to Azure SQL: {e}", exc_info=True)
        raise

def fetch_mysql_data():
    """Fetch data from MySQL using pymysql and return as a DataFrame."""
    try:
        # Start SSH Tunnel
        logging.info("Establishing SSH Tunnel...")
        with SSHTunnelForwarder(
            (ssh_host, 22),
            ssh_username=ssh_user,
            ssh_private_key=ssh_key_path,
            remote_bind_address=(db_host, 3306)
        ) as tunnel:
            logging.info(f"SSH Tunnel established at port: {tunnel.local_bind_port}")
            
            # Connect to MySQL
            connection = pymysql.connect(
                host="127.0.0.1",
                port=tunnel.local_bind_port,
                user=mysql_config["user"],
                password=mysql_config["password"],
                database=mysql_config["database"]
            )
            logging.info("Connected to MySQL successfully.")
            
            # Fetch data
            query = f"""
            SELECT id, Creator_Id, Type, Date, Time, Name
            FROM notes 
            WHERE YEAR(Date) >= '{max_year}'
            AND CONCAT(date, ' ', time) > '{max_date_time}'
            AND YEAR(Date) NOT IN (5520, 2035)
            """
            logging.info("MySQL Query:")
            logging.info(query)
            df = pd.read_sql(query, connection)
            logging.info(f"Fetched {len(df)} rows from MySQL.")
            return df
    except Exception as e:
        logging.error(f"Error in MySQL connection or fetching data: {e}", exc_info=True)
        return None

def upload_to_azure_sql(df):
    """Upload a DataFrame to Azure SQL in batches."""
    try:
        conn_str = (
            f"DRIVER={{{azure_sql_config['driver']}}};"
            f"SERVER={azure_sql_config['server']};"
            f"DATABASE={azure_sql_config['database']};"
            f"UID={azure_sql_config['username']};"
            f"PWD={azure_sql_config['password']}"
        )
        with pyodbc.connect(conn_str) as azure_conn:
            cursor = azure_conn.cursor()
            logging.info("Connected to Azure SQL.")
            
            # Prepare batch insert query
            columns = ", ".join(df.columns)
            placeholders = ", ".join(["?"] * len(df.columns))
            query = f"INSERT INTO {azure_table_name} ({columns}) VALUES ({placeholders})"
            
            # Insert data in batches
            for start in range(0, len(df), batch_size):
                end = start + batch_size
                batch = df.iloc[start:end]
                data = [tuple(row) for row in batch.to_numpy()]
                cursor.executemany(query, data)
                logging.info(f"Inserted rows {start} to {end - 1} into Azure SQL.")
            azure_conn.commit()
    except pyodbc.Error as e:
        logging.error(f"Error uploading data to Azure SQL: {e}", exc_info=True)
        raise

def main():
    """Main workflow."""
    logging.info("Starting data processing workflow...")
    global max_year, max_date_time
    max_year, max_date_time = fetch_max_duration_data()
    df = fetch_mysql_data()
    if df is not None and not df.empty:
        upload_to_azure_sql(df)
    else:
        logging.warning("No data to upload. MySQL fetch returned empty or None.")

if __name__ == "__main__":
    main()
