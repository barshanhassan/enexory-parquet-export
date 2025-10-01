# To be run as a cronjob

import os
import subprocess
import pandas as pd
from datetime import date, timedelta, datetime
import time
import fastparquet
from diskcache import Cache
import csv

BASE_FOLDER = "/root/data"
PK_COL = "id"
DT_COL = "date_time"
DISKCACHE_TEMP_FOLDER = "/tmp/mycache"
BINLOG_FOLDER = "/var/log/mysql"
ENEX_DIR = "/tmp"

def get_yesterday_range():
    yday = date.today() - timedelta(days=1)
    start = datetime.combine(yday, datetime.min.time())
    end = datetime.combine(yday + timedelta(days=1), datetime.min.time())
    return start, end

custom_formatter = lambda x: (
    f"{x.year:04d}-{x.month:02d}-{x.day:02d} {x.hour:02d}:{x.minute:02d}:{x.second:02d}"
    if pd.notna(x) and hasattr(x, "strftime") else "0001-01-01 00:00:00"
)

def run_parse_binlogs(start_ts, end_ts):
    """Run parse_binlogs.sh and save output to a timestamped .enex file."""
    timestamp_ns = int(time.time() * 1_000_000_000)
    enex_file_path = os.path.join(ENEX_DIR, f"{timestamp_ns}.enex")
    start_str = start_ts.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_ts.strftime("%Y-%m-%d %H:%M:%S")
    
    # Run parse_binlogs.sh and pipe to consolidate
    cmd = f"./parse_binlogs.sh --binlog-folder \"{BINLOG_FOLDER}\" --start-datetime \"{start_str}\" --stop-datetime \"{end_str}\"  --output-file \"{enex_file_path}\""
    try:
        subprocess.run(cmd, shell=True, check=True, text=True)
        print(f"Saved binlog output to {enex_file_path}")
        return enex_file_path
    except subprocess.CalledProcessError as e:
        print(f"Error running parse_binlogs.sh: {e}")
        raise

def read_and_group_changes(enex_file):
    """Read .enex CSV and group changes by day using diskcache."""
    changes_by_day = Cache(os.path.join(DISKCACHE_TEMP_FOLDER, "changes_by_day"))
    utc_plus_2 = datetime.timezone(datetime.timedelta(hours=2)) #ALWAYS CEST, EXTRACTOR IF RAN MUST ALSO BE SAVING IT IN CEST. SEEMS LIKE OUTSIDE SUMMERS A PROBLEM MAY ARISE
    
    # Stream CSV to minimize memory
    with open(enex_file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 2 or row[0] not in ('I', 'U', 'D'):
                continue  # Skip invalid rows
            
            change = {'type': row[0], 'data': {}}

            change['data'][PK_COL] = int(row[1])  # id
            change['data'][DT_COL] = row[2].strip("'")  # date_time
            
            if change['type'] in ('I', 'U'):
                change['data']['value'] = None if row[3] == 'NULL' else float(row[3])
                change['data']['ts'] = datetime.datetime.utcfromtimestamp(int(row[4])).replace(tzinfo=datetime.timezone.utc).astimezone(utc_plus_2).strftime("%Y-%m-%d %H:%M:%S")
            
            # Group by day (YYYY-MM-DD)
            day = change['data'][DT_COL][:10]  # Extract YYYY-MM-DD
            bucket = changes_by_day.get(day, [])
            bucket.append(change)
            changes_by_day[day] = bucket
    
    return changes_by_day

def apply_changes_to_parquet(base_folder, changes_by_day):
    """Apply changes to Parquet files by day."""
    for day, changes in changes_by_day.items():
        file_path = os.path.join(base_folder, f"{day}.parquet")
        
        try:
            day_df = pd.read_parquet(file_path)
        except FileNotFoundError:
            print(f"File for day {day} not found. Creating new file.")
            day_df = pd.DataFrame(columns=[PK_COL, DT_COL, 'value', 'ts'])

        pks_to_modify = {change['data'][PK_COL] for change in changes}
        upsert_rows = [change['data'] for change in changes if change['type'] in ('I', 'U')]

        # Filter out rows to be deleted or updated
        if not day_df.empty and pks_to_modify:
            day_df = day_df[~day_df[PK_COL].isin(pks_to_modify)]

        # Add new/updated rows
        if upsert_rows:
            upsert_df = pd.DataFrame(upsert_rows)
            # Ensure proper column types
            upsert_df[PK_COL] = upsert_df[PK_COL].astype('uint64')
            upsert_df[DT_COL] = upsert_df[DT_COL].apply(custom_formatter)
            upsert_df['value'] = upsert_df['value'].astype('float64', errors='ignore')
            upsert_df['ts'] = upsert_df['ts']
            final_df = pd.concat([day_df, upsert_df], ignore_index=True)
        else:
            final_df = day_df

        # Write updated Parquet file
        os.makedirs(base_folder, exist_ok=True)
        fastparquet.write(file_path, final_df, compression='snappy', append=False, row_group_offsets=None, write_index=False)
        
        if final_df.empty:
            print(f"Applied {len(changes)} changes to {file_path}. File is now empty.")
        else:
            print(f"Applied {len(changes)} changes to {file_path}. New row count: {len(final_df)}")

def run_binlog_merge_sync():
    """Main function to orchestrate binlog sync."""
    start_program = datetime.now()
    start_ts, end_ts = get_yesterday_range()
    changes_by_day = None
    
    print(f"Processing binlog for {start_ts} to {end_ts}")
    
    # Run parse_binlogs.sh and save to .enex
    enex_file = run_parse_binlogs(start_ts, end_ts)
    
    try:
        # Read and group changes by day
        print(f"Reading and grouping changes from {enex_file}...")
        changes_by_day = read_and_group_changes(enex_file)
        
        if not changes_by_day:
            print("No changes found in .enex file.")
            return
        
        print(f"Found changes for {len(changes_by_day)} day(s). Applying to Parquet...")
        apply_changes_to_parquet(BASE_FOLDER, changes_by_day)
        
    finally:
        if changes_by_day is not None:
            changes_by_day.clear()
            changes_by_day.close()
            print(f"Cleared and closed diskcache at {DISKCACHE_TEMP_FOLDER}/changes_by_day")

        # Delete .enex file
        if os.path.exists(enex_file):
            os.remove(enex_file)
            print(f"Deleted {enex_file}")
        
    print("\nBinlog merge process finished successfully.")
    end_program = datetime.now()
    print(f"Program took {(end_program - start_program).total_seconds():.4f} seconds")

if __name__ == "__main__":
    run_binlog_merge_sync()