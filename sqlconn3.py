import os
import pymysql
import pyodbc
import pandas as pd
import logging
from sshtunnel import SSHTunnelForwarder

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# MySQL Configuration
mysql_config = {
    "host": os.environ("MSH"),       # e.g., "localhost" or IP
    "user": os.environ("MSU"),       # MySQL username
    "password": os.environ("MSP"), # MySQL password
    "database": os.environ("MSN"),     # MySQL database name
}

# Configuration
# Configuration
ssh_host = os.environ("SSH")  # SSH server (bastion host) IP
ssh_user = os.environ("SSU")   # SSH username (e.g., ec2-user)
ssh_key_path = os.environ.get("SSH_KEY_PATH", "./neha_key.pem")
db_host = os.environ("DBH")

# Azure SQL Configuration
azure_sql_config = {
    "server": os.environ("ASS"),
    "database": os.environ("ASD"),
    "username": os.environ("ASU"),
    "password": os.environ("ASP"),
    "driver": "ODBC Driver 17 for SQL Server",  # Ensure the correct driver is installed
}

# Azure SQL Table Name
azure_table_name = "extra_staff.notes"
batch_size = 1000  # Number of rows per batch

def fetch_max_duration_data():
    select_max_duration_query = """
    SELECT top 1
    YEAR(date) as max_year, 
    date as max_date, 
    time as max_time
    from extra_staff.notes
    order by year(date) desc, date desc, time desc
    """
    conn_str = (
            f"DRIVER={{{azure_sql_config['driver']}}};"
            f"SERVER={azure_sql_config['server']};"
            f"DATABASE={azure_sql_config['database']};"
            f"UID={azure_sql_config['username']};"
            f"PWD={azure_sql_config['password']}"
        )

    with pyodbc.connect(conn_str) as azure_conn:
            cursor = azure_conn.cursor()
            cursor.execute(select_max_duration_query)

            max_duration_query_result = cursor.fetchone()
            max_year = max_duration_query_result.max_year
            max_date_time = str(max_duration_query_result.max_date) + ' ' + str(max_duration_query_result.max_time)
            
            return max_year, max_date_time
            
max_year, max_date_time = fetch_max_duration_data()

# Query to fetch data from MySQL
mysql_query = f"""
SELECT
    id,
    Creator_Id,
    Type,
    Date,
    Time,
    Name
FROM notes 
WHERE YEAR(Date) >= '{max_year}'
AND concat(date, ' ', time) > '{max_date_time}'
AND YEAR(Date) NOT IN (5520, 2035)
"""

print(mysql_query)

def fetch_mysql_data():
    """Fetch data from MySQL using pymysql and return as a DataFrame."""
    try:
        # Connect to MySQL
        with SSHTunnelForwarder(
        (ssh_host, 22),                      # Bastion host IP and port
        ssh_username=ssh_user,               # SSH username
        ssh_private_key=ssh_key_path,        # Path to private SSH key
        remote_bind_address=(db_host, 3306)  # RDS endpoint and port
    ) as tunnel:
        # Connect to RDS via the SSH tunnel
            connection = pymysql.connect(
            host=mysql_config['host'],                # Localhost for the SSH tunnel
            port=tunnel.local_bind_port,     # Dynamically assigned local port
            user=mysql_config['user'],                    # RDS username
            password=mysql_config['password'],            # RDS password
            database=mysql_config['database']    # Replace with your database name
            )
            logging.info("Connected to MySQL.")
            
            
            # Fetch data into a DataFrame
            df = pd.read_sql(mysql_query, connection)
            
            # Clean up 'Time' column
            df['Time'] = df['Time'].apply(lambda x: str(x).split('days')[-1])
            
            logging.info(f"Fetched {len(df)} rows from MySQL.")
            connection.close()
        return df
    except Exception as e:
        logging.error(f"Error fetching data from MySQL: {e}")
        return None


def upload_to_azure_sql(df):
    """Upload a DataFrame to Azure SQL in batches."""
    try:
        # Create Azure SQL connection string
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

            # Prepare data for batch insertion
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
            logging.info("All data uploaded to Azure SQL.")
    except Exception as e:
        logging.error(f"Error uploading data to Azure SQL: {e}")


def main():
    # Fetch data from MySQL
    df = fetch_mysql_data()
    if df is not None and not df.empty:
        # Upload data to Azure SQL
        upload_to_azure_sql(df)
    else:
        logging.warning("No data to upload.")


if __name__ == "__main__":
    main()
