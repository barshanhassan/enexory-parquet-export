# To be run as a cronjob

import os
import pandas as pd
from datetime import date, timedelta, datetime
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
import fastparquet
from collections import defaultdict

ENV_VAR_MYSQL_CONN_STRING = "MYSQL_CONN_STRING"
BASE_FOLDER = "/root/data"
TABLE = "api_data_timeseries"
PK_COL = "id"
DT_COL = "date_time"
BINLOG_FOLDER="/var/log/mysql/"

fix_binlog_cols_mapping = {
    "UNKNOWN_COL0": "id",
    "UNKNOWN_COL1": "file_id",
    "UNKNOWN_COL2": "date_time",
    "UNKNOWN_COL3": "value",
    "UNKNOWN_COL4": "dst",
    "UNKNOWN_COL5": "ts",
}

def parse_conn_string(conn_str):
    parts = [p.strip() for p in conn_str.split(";") if p.strip()]
    kv = dict(p.split("=", 1) for p in parts)
    kv["port"] = int(kv.get("port", 3306))
    return kv

conn_str = os.getenv(ENV_VAR_MYSQL_CONN_STRING)
if not conn_str:
    raise ValueError("Connection string missing in env variable.")

conn_str_parsed = parse_conn_string(conn_str)
SCHEMA = conn_str_parsed["database"]

def get_yesterday_range():
    today = date.today()
    yday = today - timedelta(days=1)
    start = datetime.combine(yday, datetime.min.time())
    end = datetime.combine(yday + timedelta(days=1), datetime.min.time())
    return start, end

custom_formatter = lambda x: (
    f"{x.year:04d}-{x.month:02d}-{x.day:02d} {x.hour:02d}:{x.minute:02d}:{x.second:02d}"
    if pd.notna(x) and hasattr(x, "strftime") else "0001-01-01 00:00:00"
)

def map_binlog_row(raw_row_dict, mapping):
    """Translates a dict with generic keys to one with real column names."""
    return {mapping.get(k, k): v for k, v in raw_row_dict.items()}

def collect_and_consolidate_changes(stream, end_ts, mapping):
    """
    Reads the binlog stream and consolidates events down to their net change for each primary key.
    This version is robust against INSERT/UPDATE/DELETE sequences like:
    - INSERT -> DELETE (treated as a no-op)
    - UPDATE -> DELETE (treated as a final DELETE)
    - DELETE -> INSERT (treated as a final INSERT/UPSERT)
    """
    consolidated_changes = {}

    for binlogevent in stream:
        if datetime.fromtimestamp(binlogevent.timestamp) >= end_ts:
            print("Reached end of yesterday's time window. Stopping stream.")
            break
        
        if getattr(binlogevent, "table", None) != TABLE:
            continue
        
        for row in binlogevent.rows:
            if isinstance(binlogevent, WriteRowsEvent):
                mapped_data = map_binlog_row(row['values'], mapping)
                pk = mapped_data[PK_COL]
                consolidated_changes[pk] = {'type': 'INSERT', 'data': mapped_data}

            elif isinstance(binlogevent, UpdateRowsEvent):
                mapped_data = map_binlog_row(row['after_values'], mapping)
                pk = mapped_data[PK_COL]
                current_type = consolidated_changes.get(pk, {}).get('type', 'UPDATE')
                if current_type != 'INSERT':
                    current_type = 'UPDATE'
                consolidated_changes[pk] = {'type': current_type, 'data': mapped_data}

            elif isinstance(binlogevent, DeleteRowsEvent):
                mapped_data = map_binlog_row(row['values'], mapping)
                pk = mapped_data[PK_COL]
                if pk in consolidated_changes and consolidated_changes[pk]['type'] == 'INSERT':
                    del consolidated_changes[pk]
                else:
                    consolidated_changes[pk] = {'type': 'DELETE', 'data': mapped_data}
    
    return consolidated_changes

def apply_changes_to_parquet(base_folder, changes_by_day):
    """
    Iterates through each affected day and applies the consolidated changes
    via a read-modify-write cycle on the corresponding Parquet file.
    """
    for day, changes in changes_by_day.items():
        file_path = os.path.join(base_folder, f"{day}.parquet")
        
        try:
            day_df = pd.read_parquet(file_path)
        except FileNotFoundError:
            print(f"File for day {day} not found. A new file will be created.")
            day_df = pd.DataFrame()

        pks_to_modify = {change['data'][PK_COL] for change in changes}
        upsert_rows = [change['data'] for change in changes if change['type'] in ('INSERT', 'UPDATE')]

        # --- The Core Merge/Upsert Logic ---
        # 1. Filter out all rows that will be deleted OR updated to get a clean base.
        if not day_df.empty and pks_to_modify:
            day_df = day_df[~day_df[PK_COL].isin(pks_to_modify)]

        # 2. If there are new or updated rows, create a DataFrame for them.
        if upsert_rows:
            upsert_df = pd.DataFrame(upsert_rows)
            # Ensure proper datetime formatting before concatenation.
            for col in ["date_time", "ts"]:
                if col in upsert_df.columns:
                     upsert_df[col] = upsert_df[col].apply(custom_formatter)
            
            # 3. Combine the clean base with the new/updated data.
            final_df = pd.concat([day_df, upsert_df], ignore_index=True)

            cols_to_drop = ["file_id", "dst"]
            final_df = final_df.drop(columns=[c for c in cols_to_drop if c in final_df.columns])
        else:
            # This branch is taken if there were only deletions for this day.
            final_df = day_df

        # 4. Write the final result back to disk, overwriting the old file.
        os.makedirs(base_folder, exist_ok=True)
        fastparquet.write(file_path, final_df, compression='snappy', append=False, row_group_offsets=None, write_index=False)
        
        if final_df.empty:
            print(f"Applied {len(changes)} net changes to {file_path}. The file is now empty.")
        else:
            print(f"Applied {len(changes)} net changes to {file_path}. New row count: {len(final_df)}")

def get_binlog_start_filename(start_ts):
    """
    Returns the latest mysql-bin.* filename in BINLOG_FOLDER 
    whose modification time is strictly before start_ts.
    """
    start_epoch = int(start_ts.timestamp())
    candidates = []

    for fname in os.listdir(BINLOG_FOLDER):
        if not fname.startswith("mysql-bin."):
            continue
        fpath = os.path.join(BINLOG_FOLDER, fname)
        try:
            mtime = int(os.path.getmtime(fpath))
        except FileNotFoundError:
            continue
        if mtime < start_epoch:
            candidates.append((mtime, fname))

    if not candidates:
        raise FileNotFoundError(
            f"No binlog files found in {BINLOG_FOLDER} before {start_ts}"
        )

    latest = max(candidates, key=lambda x: x[0])
    return latest[1]

def run_binlog_merge_sync():
    """Main function to orchestrate the binlog synchronization process."""
    start_ts, end_ts = get_yesterday_range()

    binlog_start_filename = get_binlog_start_filename(start_ts)

    print(f"Starting binlog merge sync for events from {start_ts} to {end_ts}")

    stream = None
    try:
        stream = BinLogStreamReader(
            connection_settings=conn_str_parsed,
            server_id=25925,  # Must be unique in the replication cluster
            only_schemas=[SCHEMA],
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
            blocking=False,
            resume_stream=False,
            freeze_schema=True,
            skip_to_timestamp=int(start_ts.timestamp()),
            log_pos=4,
            log_file=binlog_start_filename
        )

        print("Streaming binlog to collect and consolidate changes...")
        consolidated_changes = collect_and_consolidate_changes(stream, end_ts, fix_binlog_cols_mapping) 
    finally:
        if stream:
            stream.close()

    if not consolidated_changes:
        print("No net changes found in the binlog for the specified time window.")
        return

    print(f"Consolidated {len(consolidated_changes)} net row changes. Grouping by day partition...")

    # Group the consolidated changes by their target day
    changes_by_day = defaultdict(list)
    for pk, change in consolidated_changes.items():
        dt_val = change['data'].get(DT_COL)
        if dt_val:
            # Safely format and extract the day part (YYYY-MM-DD)
            day = custom_formatter(dt_val)[:10]
            changes_by_day[day].append(change)
    
    if not changes_by_day:
        print("No changes found for valid day partitions.")
        return

    print(f"Found changes affecting {len(changes_by_day)} day partition(s). Applying changes...")
    apply_changes_to_parquet(BASE_FOLDER, changes_by_day)
    print("\nBinlog merge process finished successfully.")

if __name__ == "__main__":
    run_binlog_merge_sync()