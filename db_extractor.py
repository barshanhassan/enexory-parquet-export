import os
import mysql.connector
import pandas as pd
import fastparquet
import time
import glob
from datetime import date

# --- Configuration Constants ---
ENV_VAR_MYSQL_CONN_STRING = "MYSQL_CONN_STRING"
ENV_VAR_BATCH_SIZE = "BATCH_SIZE"
ENV_VAR_MAX_DAYS = "MAX_DAYS"

# Pandas' absolute lower limit is 1677-09-21. We use 1678 to be safe.
PANDAS_SAFE_MIN_DATE = "1678-01-01 00:00:00"

def get_connection():
    conn_str = os.getenv(ENV_VAR_MYSQL_CONN_STRING)
    if conn_str:
        return mysql.connector.connect(**parse_conn_string(conn_str))
    raise ValueError("Connection string missing in env variable.")

def parse_conn_string(conn_str):
    parts = [p.strip() for p in conn_str.split(";") if p.strip()]
    kv = dict(p.split("=", 1) for p in parts)
    kv["port"] = int(kv.get("port", 3306))
    return kv

def find_latest_dt(base_folder, dt_column):
    """
    Finds the resume point for incremental runs.
    """
    files = sorted(glob.glob(os.path.join(base_folder, "*.parquet")), reverse=True)
    invalid_sentinel_value = "0001-01-01 00:00:00"

    print("Scanning newest files to find resume point...")
    for f in files:
        try:
            pf = fastparquet.ParquetFile(f)
            if pf.count == 0: continue
            df = pf.to_pandas(columns=[dt_column])
            if df.empty: continue

            valid_strings = df[dt_column][df[dt_column] != invalid_sentinel_value]
            if not valid_strings.empty:
                latest_dt_str = valid_strings.max()
                print(f"Found latest timestamp '{latest_dt_str}' in file: {os.path.basename(f)}")
                return latest_dt_str
        except Exception as e:
            print(f"Skipping {f} due to error: {e}")
            continue
            
    print(f"No recent timestamps found. Starting incremental run from the safe boundary: {PANDAS_SAFE_MIN_DATE}")
    return PANDAS_SAFE_MIN_DATE

def run_historical_extraction(conn, query, params, chunk_size, base_folder):
    """A dedicated loop for the one-time historical backfill with safe date handling."""
    iterator = pd.read_sql(query, conn, params=params, chunksize=chunk_size)
    total_extracted = 0
    start_time = time.time()
    print("Starting historical data extraction...")

    for df_chunk in iterator:
        if df_chunk.empty: continue
        
        # --- THE FIX FOR HISTORICAL DATA ---
        # Instead of forcing to pandas datetime, we apply a safe string format.
        # This handles Python's native datetime objects which support a wider range.
        for col in ["date_time", "ts"]:
            df_chunk[col] = df_chunk[col].apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) and hasattr(x, 'strftime') else "0001-01-01 00:00:00"
            )

        # Create the 'day' column using safe string slicing, not pd.to_datetime
        # This works because the column is now guaranteed to be a 'YYYY-MM-DD...' string.
        df_chunk["day"] = df_chunk[dt_col].str[:10]

        for day, group in df_chunk.groupby("day"):
            file_path = os.path.join(base_folder, f"{day}.parquet")
            fastparquet.write(file_path, group.drop(columns=["day"]), compression="snappy", append=os.path.exists(file_path))
            total_extracted += len(group)
            print(f"Wrote {len(group)} historical rows to {file_path}")

    elapsed = time.time() - start_time
    print(f"Historical extraction finished. {total_extracted} rows in {elapsed:.2f}s")

def main():
    conn = get_connection()
    try:
        global table, dt_col
        table = "api_data_timeseries"
        dt_col = "date_time"
        base_folder = "data"
        updated_ts_col = "updated_ts"

        os.makedirs(base_folder, exist_ok=True)
        files_exist = bool(glob.glob(os.path.join(base_folder, "*.parquet")))
        
        chunk_size = int(os.getenv(ENV_VAR_BATCH_SIZE, "1000"))
        max_days = int(os.getenv(ENV_VAR_MAX_DAYS, "3"))
        
        # --- DYNAMIC LOOKAHEAD SETUP ---
        default_lookahead_days = max_days * 2
        current_lookahead_days = default_lookahead_days
        MAX_LOOKAHEAD_DAYS = 365
        print(f"Targeting {max_days} data-days per run. Initial lookahead window: {default_lookahead_days} days.")

        if not files_exist:
            # --- PHASE 1: HISTORICAL BACKFILL ---
            print("="*50 + "\nNO PARQUET FILES FOUND. PERFORMING ONE-TIME HISTORICAL BACKFILL.\n" + "="*50)
            historical_query = f"""
                SELECT id, date_time, value, ts FROM `{table}`
                WHERE `{dt_col}` < %(end_date)s ORDER BY `{dt_col}` ASC
            """
            run_historical_extraction(conn, historical_query, {"end_date": PANDAS_SAFE_MIN_DATE}, chunk_size, base_folder)

        # --- PHASE 2: NORMAL INCREMENTAL RUN ---
        print("="*50 + "\nPERFORMING INCREMENTAL RUN.\n" + "="*50)
        
        current_start_dt = find_latest_dt(base_folder, dt_col)
        days_written_this_session = set()
        total_extracted_this_session = 0
        session_start_time = time.time()

        sentinel_date = date(1, 1, 1)

        while len(days_written_this_session) < max_days:
            print(f"\n--- Starting query iteration. Target: {max_days} days. Found: {len(days_written_this_session)} ---")
            print(f"Querying for {current_lookahead_days} days starting from {current_start_dt}")
            
            cursor = conn.cursor(buffered=True, dictionary=True)
            query = f"""
                SELECT id, date_time, value, ts FROM `{table}`
                WHERE `{dt_col}` > %(start_dt)s
                AND `{dt_col}` < DATE_ADD(%(start_dt)s, INTERVAL %(lookahead)s DAY)
                ORDER BY `{dt_col}` ASC
            """
            cursor.execute(query, {"start_dt": current_start_dt, "lookahead": current_lookahead_days})

            data_found_in_iteration = False
            latest_processed_dt_in_iteration = None

            while len(days_written_this_session) < max_days:
                rows = cursor.fetchmany(size=chunk_size)
                if not rows:
                    break # No more rows in the result set

                df_chunk = pd.DataFrame(rows)
                data_found_in_iteration = True
                
                for col in ["date_time", "ts"]:
                    dt_series = pd.to_datetime(df_chunk[col], errors="coerce")
                    fmt_series = dt_series.dt.strftime("%Y-%m-%d %H:%M:%S")
                    df_chunk[col] = fmt_series.fillna("0001-01-01 00:00:00")
                
                chunk_max_dt = df_chunk[dt_col].max()
                if latest_processed_dt_in_iteration is None or chunk_max_dt > latest_processed_dt_in_iteration:
                    latest_processed_dt_in_iteration = chunk_max_dt
                
                df_chunk["day"] = pd.to_datetime(df_chunk[dt_col], errors="coerce").dt.date.fillna(sentinel_date)
                
                for day, group in df_chunk.groupby("day"):
                    if len(days_written_this_session) >= max_days: break
                    file_path = os.path.join(base_folder, f"{day}.parquet")
                    fastparquet.write(file_path, group.drop(columns=["day"]), compression="snappy", append=os.path.exists(file_path))
                    
                    if day not in days_written_this_session:
                        days_written_this_session.add(day)
                        print(f"Wrote data for new day: {day}. Total days found: {len(days_written_this_session)}")
                    total_extracted_this_session += len(group)
                if len(days_written_this_session) >= max_days: break
            
            cursor.close()

            if data_found_in_iteration:
                current_start_dt = latest_processed_dt_in_iteration
                # --- RESET LOOKAHEAD on success ---
                if current_lookahead_days != default_lookahead_days:
                    print(f"Data found! Resetting lookahead window to {default_lookahead_days} days.")
                    current_lookahead_days = default_lookahead_days
            else:
                print(f"No data found in window. Jumping forward by {current_lookahead_days} days.")
                current_start_dt_ts = pd.to_datetime(current_start_dt)
                next_start_dt_ts = current_start_dt_ts + pd.Timedelta(days=current_lookahead_days)

                future_safety_boundary = pd.Timestamp.now() + pd.Timedelta(days=7)
                if next_start_dt_ts > future_safety_boundary:
                    print(f"Lookahead window has passed the safety boundary of one week from now. Stopping.")
                    break
                
                current_start_dt = next_start_dt_ts.strftime('%Y-%m-%d %H:%M:%S')
                # --- INCREASE LOOKAHEAD on failure ---
                current_lookahead_days = min(current_lookahead_days * 2, MAX_LOOKAHEAD_DAYS)
                print(f"Increasing lookahead window to {current_lookahead_days} days for the next iteration.")

        elapsed = time.time() - session_start_time
        print(f"\nIncremental run finished. Extracted {total_extracted_this_session} rows for {len(days_written_this_session)} distinct days in {elapsed:.2f}s.")

    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()