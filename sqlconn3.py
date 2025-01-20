import os
import pymysql
import pyodbc
import pandas as pd
import logging
from sshtunnel import SSHTunnelForwarder

# Environment Variables
MSH = os.environ["MSH"]
MSU = os.environ["MSU"]
MSP = os.environ["MSP"]
MSN = os.environ["MSN"]

SSH = os.environ["SSH"]
SSU = os.environ["SSU"]
DBH = os.environ["DBH"]

ASS = os.environ["ASS"]
ASD = os.environ["ASD"]
ASU = os.environ["ASU"]
ASP = os.environ["ASP"]

# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# MySQL Configuration
mysql_config = {
    "host": MSH,
    "user": MSU,
    "password": MSP,
    "database": MSN,
}

# SSH Configuration
ssh_host = SSH
ssh_user = SSU
ssh_key_path = os.environ.get("SSH_KEY_PATH", "./neha_key.pem")
db_host = DBH

# Azure SQL Configuration
azure_sql_config = {
    "server": ASS,
    "database": ASD,
    "username": ASU,
    "password": ASP,
    "driver": "ODBC Driver 17 for SQL Server",
}

# Azure SQL Table Name
azure_table_name = "extra_staff.notes"
batch_size = 1000

def fetch_latest_date_from_azure():
    """Fetch the latest Date from Azure SQL."""
    query = f"SELECT MAX(Date) FROM {azure_table_name} WHERE YEAR(Date) NOT IN (5520, 2035)"
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
            cursor.execute(query)
            result = cursor.fetchone()
            latest_date = result[0] if result and result[0] else None
            logging.info(f"Latest valid date in Azure SQL: {latest_date}")
            return latest_date
    except Exception as e:
        logging.error(f"Error fetching latest date from Azure SQL: {e}")
        return None

def fetch_mysql_data_by_date(latest_date):
    """Fetch data from MySQL based on the latest date in Azure SQL."""
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

            # Build the query based on the latest date
            if latest_date:
                mysql_query = f"""
                SELECT id, Creator_Id, Type, Date, Time, Name
                FROM notes
                WHERE Date = (SELECT MAX(Date) FROM notes WHERE YEAR(Date) NOT IN (5520, 2035))
                  AND YEAR(Date) NOT IN (5520, 2035)
                  AND id NOT IN (SELECT id FROM {azure_table_name})
                """
            else:
                # First run: Fetch only rows for the maximum date
                mysql_query = f"""
                SELECT id, Creator_Id, Type, Date, Time, Name
                FROM notes
                WHERE Date = (SELECT MAX(Date) FROM notes WHERE YEAR(Date) NOT IN (5520, 2035))
                  AND YEAR(Date) NOT IN (5520, 2035)
                """
            
            # Fetch data into a DataFrame
            df = pd.read_sql(mysql_query, connection)
            logging.info(f"Fetched {len(df)} rows from MySQL.")
            connection.close()
        return df
    except Exception as e:
        logging.error(f"Error fetching data from MySQL: {e}")
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

            # Prepare data for batch insertion
            columns = ", ".join(df.columns)
            placeholders = ", ".join(["?"] * len(df.columns))
            query = f"INSERT INTO {azure_table_name} ({columns}) VALUES ({placeholders})"

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
    """Main function to synchronize data between MySQL and Azure SQL."""
    # Fetch the latest valid Date from Azure SQL
    latest_date = fetch_latest_date_from_azure()

    # Fetch new data from MySQL
    df = fetch_mysql_data_by_date(latest_date)
    if df is not None and not df.empty:
        # Upload data to Azure SQL
        upload_to_azure_sql(df)
    else:
        logging.info("No new data to upload.")

if __name__ == "__main__":
    main()
