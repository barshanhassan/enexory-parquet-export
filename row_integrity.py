import os
import mysql.connector
import fastparquet
import glob

def parse_conn_string(conn_str):
    """Parses a semicolon-separated connection string into a dictionary."""
    parts = [p.strip() for p in conn_str.split(";") if p.strip()]
    kv = dict(p.split("=", 1) for p in parts)
    kv["port"] = int(kv.get("port", 3306))
    return kv

def main():
    """
    Compares the total row count in the Parquet files against the count
    in the MySQL database up to the date of the latest Parquet file.
    """
    # --- Configuration ---
    TABLE_NAME = "api_data_timeseries"
    DATE_COLUMN = "date_time"
    PARQUET_FOLDER = "data"
    ENV_VAR_CONN_STRING = "MYSQL_CONN_STRING"

    # --- 1. Get Parquet File Information ---
    try:
        files = glob.glob(os.path.join(PARQUET_FOLDER, "*.parquet"))
        if not files:
            print("No Parquet files found. Exiting.")
            return

        # Find the latest file based on filename
        latest_file_path = max(files)
        latest_date_str = os.path.basename(latest_file_path).replace(".parquet", "")
        
        # Efficiently sum rows from all files' metadata
        parquet_total_rows = sum(fastparquet.ParquetFile(f).count() for f in files)

    except Exception as e:
        print(f"Error processing Parquet files: {e}")
        return

    # --- 2. Get Database Information ---
    conn = None
    try:
        conn_str = os.getenv(ENV_VAR_CONN_STRING)
        if not conn_str:
            raise ValueError(f"Environment variable '{ENV_VAR_CONN_STRING}' is not set.")

        conn = mysql.connector.connect(**parse_conn_string(conn_str))
        cursor = conn.cursor()

        # Build a safe SQL query
        sql_query = f"""
            SELECT COUNT(*) 
            FROM `{TABLE_NAME}` 
            WHERE `{DATE_COLUMN}` < DATE_ADD(%s, INTERVAL 1 DAY)
        """
        
        cursor.execute(sql_query, (latest_date_str,))
        db_filtered_rows = cursor.fetchone()[0]

    except Exception as e:
        print(f"Error connecting to or querying the database: {e}")
        return
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

    # --- 3. Compare and Report ---
    match = (db_filtered_rows == parquet_total_rows)

    print("-" * 40)
    print(f"Parquet Total Rows: {parquet_total_rows}")
    print(f"DB Filtered Rows (before {latest_date_str} + 1 day): {db_filtered_rows}")
    print(f"Counts Match: {match}")
    print("-" * 40)

    if not match:
        print(f"WARNING: Counts do not match! Difference: {abs(db_filtered_rows - parquet_total_rows)}")


if __name__ == "__main__":
    main()