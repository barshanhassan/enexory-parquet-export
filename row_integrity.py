import os
import mysql.connector
import fastparquet
import glob
import argparse

def parse_conn_string(conn_str):
    """Parses a semicolon-separated connection string into a dict."""
    parts = [p.strip() for p in conn_str.split(";") if p.strip()]
    kv = dict(p.split("=", 1) for p in parts)
    kv["port"] = int(kv.get("port", 3306))
    return kv

def main():
    """
    Compares the total row count in all Parquet files in PARQUET_FOLDER
    against the total row count in the MySQL table.
    """
    # --- Argument Parser ---
    parser = argparse.ArgumentParser(description="Compare Parquet and MySQL row counts.")
    parser.add_argument("--manual-db-count", type=int, help="Skips DB connection and query, using this value instead.")
    args = parser.parse_args()

    # --- Configuration ---
    TABLE_NAME = "api_data_timeseries"
    PARQUET_FOLDER = "/root/data"
    ENV_VAR_CONN_STRING = "MYSQL_CONN_STRING"

    files = glob.glob(os.path.join(PARQUET_FOLDER, "*.parquet"))
    if not files:
        print("No Parquet files found. Exiting.")
        return

    # --- 1. Get Database Total Count ---
    db_total_rows = 0
    if args.manual_db_count is not None:
        db_total_rows = args.manual_db_count
    else:
        conn = None
        cursor = None
        try:
            conn_str = os.getenv(ENV_VAR_CONN_STRING)
            if not conn_str:
                raise ValueError(f"Environment variable '{ENV_VAR_CONN_STRING}' is not set.")

            conn = mysql.connector.connect(**parse_conn_string(conn_str))
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM `{TABLE_NAME}`")
            db_total_rows = cursor.fetchone()[0]

        except Exception as e:
            print(f"Error connecting to or querying the database: {e}")
            return
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn and conn.is_connected():
                try:
                    conn.close()
                except Exception:
                    pass

    # --- 2. Get Parquet Total Rows ---
    try:
        parquet_total_rows = sum(fastparquet.ParquetFile(f).count() for f in files)
    except Exception as e:
        print(f"Error processing Parquet files: {e}")
        return

    # --- 3. Compare and Report ---
    match = (db_total_rows == parquet_total_rows)
    diff = parquet_total_rows - db_total_rows
    sign = "+" if diff > 0 else "-" if diff < 0 else "0"

    print("-" * 40)
    print(f"Parquet Total Rows: {parquet_total_rows}")
    print(f"DB Total Rows:      {db_total_rows}")
    print(f"Counts Match:       {match}")
    print(f"Difference:         {sign}{abs(diff)}  (+ means more rows in parquet)")
    print("-" * 40)


if __name__ == "__main__":
    main()