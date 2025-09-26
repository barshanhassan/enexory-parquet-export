# To be run as a cronjob

import os
import pandas as pd
from datetime import date, timedelta, datetime
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
import mysql.connector
import fastparquet

ENV_VAR_MYSQL_CONN_STRING = "MYSQL_CONN_STRING"
CHUNK_SIZE = 1000000
BASE_FOLDER = "data2"
TABLE = "api_data_timeseries"
DT_COL = "date_time"
DT_COL_BINLOGS = "UNKNOWN_COL2"
    
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

def get_connection():
    return mysql.connector.connect(**conn_str_parsed)

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

def collect_binlog_days(start_ts, end_ts):
    """
    Collect affected days directly from row-based binlogs.
    """

    affected_days = set()

    stream = BinLogStreamReader(
        connection_settings=conn_str_parsed,
        server_id=25925,  # must be unique in replication cluster
        only_schemas=[SCHEMA],
        only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
        blocking=False,
        resume_stream=False,
        # log_file=last_log,
        freeze_schema=True,
        log_pos=4,
        skip_to_timestamp=int(start_ts.timestamp())
    )

    for binlogevent in stream:
        ev_time = datetime.fromtimestamp(binlogevent.timestamp)

        if ev_time < start_ts:
            continue
        if ev_time >= end_ts:
            break
        
        if getattr(binlogevent, "table", None) != TABLE:
            continue
        
        for row in binlogevent.rows:
            
            if isinstance(binlogevent, WriteRowsEvent):
                dt_val = row["values"].get(DT_COL_BINLOGS)
            elif isinstance(binlogevent, UpdateRowsEvent):
                dt_val = row["after_values"].get(DT_COL_BINLOGS)
            elif isinstance(binlogevent, DeleteRowsEvent):
                dt_val = row["values"].get(DT_COL_BINLOGS)
            else:
                dt_val = None

            if dt_val:
                safe_val = custom_formatter(dt_val)
                day = safe_val[:10]  # YYYY-MM-DD
                affected_days.add(day)

    stream.close()
    return affected_days

def _process_single_day(conn, day_to_process, table, dt_col, base_folder, chunk_size):
    """
    day_to_process must be a string 'YYYY-MM-DD'.
    Builds start/end datetimes using Python's datetime and fetches rows in chunks.
    Applies custom_formatter to date_time and ts per chunk before writing parquet.
    """
    # parse YYYY-MM-DD safely without pandas
    try:
        y, m, d = map(int, day_to_process.split("-"))
        start_of_day = datetime(y, m, d)
    except Exception:
        print(f"Skipping invalid day token: {day_to_process}")
        return 0

    end_of_day = start_of_day + timedelta(days=1)

    query = f"""
        SELECT id, date_time, value, ts FROM `{table}`
        WHERE `{dt_col}` >= %(start_of_day)s AND `{dt_col}` < %(end_of_day)s
    """

    iterator = pd.read_sql(
        query,
        conn,
        params={"start_of_day": start_of_day, "end_of_day": end_of_day},
        chunksize=chunk_size
    )

    all_chunks_for_day = []
    for df_chunk in iterator:
        if df_chunk.empty:
            continue
        # apply safe formatter immediately on chunk
        for col in ["date_time", "ts"]:
            df_chunk[col] = df_chunk[col].apply(custom_formatter)
        all_chunks_for_day.append(df_chunk)

    if not all_chunks_for_day:
        # write empty file so downstream sees partition present (optional)
        os.makedirs(base_folder, exist_ok=True)
        file_path = os.path.join(base_folder, f"{day_to_process}.parquet")
        empty_df = pd.DataFrame(columns=["id", "date_time", "value", "ts"])
        fastparquet.write(file_path, empty_df, compression="snappy", append=False)
        print(f"Day {day_to_process} has no rows → wrote empty file {file_path}")
        return 0

    full_day_df = pd.concat(all_chunks_for_day, ignore_index=True)

    os.makedirs(base_folder, exist_ok=True)
    file_path = os.path.join(base_folder, f"{day_to_process}.parquet")
    fastparquet.write(file_path, full_day_df, compression="snappy", append=False)

    rows_written = len(full_day_df)
    print(f"Wrote {rows_written} rows to {file_path} (overwritten).")
    return rows_written

def run_binlog_refresh():
    conn = get_connection()
    try:
        start_ts, end_ts = get_yesterday_range()
        print(f"Streaming binlogs {start_ts} → {end_ts} ...")
        days = collect_binlog_days(start_ts, end_ts)
        print(f"Affected days: {days}")

        for d in days:
            print(f"--- Refreshing {d}")
            _process_single_day(conn, d, TABLE, DT_COL, BASE_FOLDER, CHUNK_SIZE)

    finally:
        if conn and conn.is_connected():
            conn.close()

if __name__ == "__main__":
    run_binlog_refresh()
