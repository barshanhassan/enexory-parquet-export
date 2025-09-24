import os
import mysql.connector
import pandas as pd
import fastparquet
import time
import glob
from datetime import date, timedelta

# --- Configuration Constants ---
ENV_VAR_MYSQL_CONN_STRING = "MYSQL_CONN_STRING"
ENV_VAR_BATCH_SIZE = "BATCH_SIZE"
ENV_VAR_MAX_DAYS = "MAX_DAYS"

MIN_DATE = "2010-01-02 00:00:00"

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
            
    print(f"No recent timestamps found. Starting from the safe boundary: {MIN_DATE}")
    return MIN_DATE

def run_historical_extraction(conn, query, params, chunk_size, base_folder):
    """A dedicated loop for the one-time historical backfill with safe date handling."""
    # This function is unchanged.
    iterator = pd.read_sql(query, conn, params=params, chunksize=chunk_size)
    total_extracted = 0
    start_time = time.time()
    print("Starting historical data extraction...")

    for df_chunk in iterator:
        if df_chunk.empty: continue
        # Use a special formatter for potentially very old historical dates
        custom_formatter = lambda x: f"{x.year:04d}-{x.month:02d}-{x.day:02d} {x.hour:02d}:{x.minute:02d}:{x.second:02d}" if pd.notna(x) and hasattr(x, 'strftime') else "0001-01-01 00:00:00"
        for col in ["date_time", "ts"]:
            df_chunk[col] = df_chunk[col].apply(custom_formatter)
        df_chunk["day"] = df_chunk[dt_col].str[:10]
        for day, group in df_chunk.groupby("day"):
            file_path = os.path.join(base_folder, f"{day}.parquet")
            fastparquet.write(file_path, group.drop(columns=["day"]), compression="snappy", append=os.path.exists(file_path))
            total_extracted += len(group)
            print(f"Wrote {len(group)} historical rows to {file_path}")
    elapsed = time.time() - start_time
    print(f"Historical extraction finished. {total_extracted} rows in {elapsed:.2f}s")

def _process_single_day(conn, day_to_process, table, dt_col, base_folder, chunk_size):
    """
    Fetches all data for a single day, overwrites the corresponding file, and returns row count.
    """
    start_of_day = pd.Timestamp(day_to_process)
    end_of_day = start_of_day + timedelta(days=1)
    
    query = f"""
        SELECT id, date_time, value, ts FROM `{table}`
        WHERE `{dt_col}` >= %(start_of_day)s AND `{dt_col}` < %(end_of_day)s
    """
    
    # Use pandas to read SQL in chunks for memory efficiency
    iterator = pd.read_sql(query, conn, params={"start_of_day": start_of_day, "end_of_day": end_of_day}, chunksize=chunk_size)
    
    all_chunks_for_day = []
    for df_chunk in iterator:
        if not df_chunk.empty:
            all_chunks_for_day.append(df_chunk)

    if not all_chunks_for_day:
        print(f"No data found for {day_to_process.strftime('%Y-%m-%d')}.")
        return 0

    # Combine all chunks into a single DataFrame for the day
    full_day_df = pd.concat(all_chunks_for_day, ignore_index=True)
    
    # Standard date formatting
    for col in ["date_time", "ts"]:
        dt_series = pd.to_datetime(full_day_df[col], errors="coerce")
        fmt_series = dt_series.dt.strftime("%Y-%m-%d %H:%M:%S")
        full_day_df[col] = fmt_series.fillna("0001-01-01 00:00:00")
        
    file_path = os.path.join(base_folder, f"{day_to_process.strftime('%Y-%m-%d')}.parquet")
    
    # Overwrite the file with the complete data for the day
    fastparquet.write(file_path, full_day_df, compression="snappy", append=False)
    
    rows_written = len(full_day_df)
    print(f"Wrote {rows_written} rows to {file_path} (overwritten).")
    return rows_written

def main():
    conn = get_connection()
    try:
        global table, dt_col
        table = "api_data_timeseries"
        dt_col = "date_time"
        base_folder = "data"

        os.makedirs(base_folder, exist_ok=True)
        files_exist = bool(glob.glob(os.path.join(base_folder, "*.parquet")))
        
        chunk_size = int(os.getenv(ENV_VAR_BATCH_SIZE, "1000"))
        max_days_to_find = int(os.getenv(ENV_VAR_MAX_DAYS, "3"))

        if not files_exist:
            print("="*50 + "\nNO PARQUET FILES FOUND. PERFORMING ONE-TIME HISTORICAL BACKFILL.\n" + "="*50)
            historical_query = f"""SELECT id, date_time, value, ts FROM `{table}` WHERE `{dt_col}` < %(end_date)s ORDER BY `{dt_col}` ASC"""
            run_historical_extraction(conn, historical_query, {"end_date": MIN_DATE}, chunk_size, base_folder)

        print("="*50 + "\nPERFORMING INCREMENTAL RUN.\n" + "="*50)
        
        latest_dt_str = find_latest_dt(base_folder, dt_col)
        latest_timestamp = pd.to_datetime(latest_dt_str)

        # --- REFETCH AND OVERWRITE LATEST DAY ---
        # Determine the starting date for the walking loop
        if latest_timestamp.strftime('%Y-%m-%d %H:%M:%S') == MIN_DATE:
            # No real data exists yet, start walking from the safe boundary
            current_date_for_loop = latest_timestamp.date()
        else:
            # Real data exists. First, refetch the most recent day.
            date_to_refetch = latest_timestamp.date()
            print(f"\n--- REFETCHING LATEST DAY: {date_to_refetch.strftime('%Y-%m-%d')} ---")
            _process_single_day(conn, date_to_refetch, table, dt_col, base_folder, chunk_size)
            
            # Then, set the loop to start on the *next* day.
            current_date_for_loop = date_to_refetch + timedelta(days=1)

        # --- START DAY-BY-DAY WALKING LOOP ---
        days_written_this_session = set()
        total_extracted_this_session = 0
        session_start_time = time.time()
        future_safety_boundary = date.today() + timedelta(days=7)

        print(f"\n--- Starting incremental walk from {current_date_for_loop.strftime('%Y-%m-%d')} ---")
        while len(days_written_this_session) < max_days_to_find:
            if current_date_for_loop > future_safety_boundary:
                print(f"Stopping: current date {current_date_for_loop} has passed the safety boundary.")
                break

            print(f"Checking for day: {current_date_for_loop.strftime('%Y-%m-%d')}")
            rows_written = _process_single_day(conn, current_date_for_loop, table, dt_col, base_folder, chunk_size)

            if rows_written > 0:
                days_written_this_session.add(current_date_for_loop)
                total_extracted_this_session += rows_written
                print(f"Found new day with data. Total days found this session: {len(days_written_this_session)}")

            # Walk to the next day
            current_date_for_loop += timedelta(days=1)

        elapsed = time.time() - session_start_time
        print(f"\nIncremental run finished. Extracted {total_extracted_this_session} rows for {len(days_written_this_session)} new days in {elapsed:.2f}s.")

    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()