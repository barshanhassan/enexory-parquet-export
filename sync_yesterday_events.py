import os, re, subprocess, gzip, shutil
from datetime import date, datetime, timedelta
from collections import defaultdict
import pandas as pd, fastparquet

SERVER_USER = os.getenv("SSH_USER")
SERVER_HOST = os.getenv("SSH_HOST")
SSH_KEY = os.getenv("SSH_KEY")
if not SERVER_USER or not SERVER_HOST or not SSH_KEY:
    raise ValueError("SSH credentials are missing from environment variables.")
SERVER = f"{SERVER_USER}@{SERVER_HOST}"

REMOTE_DIR = "/var/tmp/binlog_exports"
BINLOG_PATH = "/var/log/mysql/mysql-bin.*"
LOCAL_BINLOGS_DIR = "./binlogs"

RSYNC_BIN = shutil.which("rsync") or "rsync"
SSH_BIN = shutil.which("ssh") or "ssh"

SCHEMA = "enexory"
TABLE = "api_data_timeseries"

BASE_FOLDER = "data"

PK_COL = "id"
DT_COL = "date_time"

fix_binlog_cols_mapping = {
    "UNKNOWN_COL0": "id",
    "UNKNOWN_COL1": "file_id",
    "UNKNOWN_COL2": "date_time",
    "UNKNOWN_COL3": "value",
    "UNKNOWN_COL4": "dst",
    "UNKNOWN_COL5": "ts",
}

def yesterday_range():
    yday = date.today() - timedelta(days=1)
    start = datetime.combine(yday, datetime.min.time())
    end = datetime.combine(yday, datetime.max.time())
    return start, end

def sh_escape(s):
    return s.replace("'", "'\"'\"'")

def remote_export_command(start_dt, stop_dt, remote_dir):
    """
    Build the command that runs on the server (no mysql client, no Python).
    It directly uses mysqlbinlog against the known binlog file path.
    """
    ts = datetime.now().strftime("%Y%m%dT%H%M%SZ")
    
    out_tmp = f"/tmp/binlog_export_{ts}.txt"
    out_gz = f"{remote_dir}/export_{ts}.txt.gz"
    
    cmd = f"""
mkdir -p '{remote_dir}' && \
mysqlbinlog --base64-output=DECODE-ROWS --verbose \
  --start-datetime='{sh_escape(start_dt)}' \
  --stop-datetime='{sh_escape(stop_dt)}' \
  {BINLOG_PATH} > '{out_tmp}' 2>/dev/null && \
gzip -c '{out_tmp}' > '{out_gz}' && \
rm -f '{out_tmp}'
"""
    return " ".join(line.strip() for line in cmd.splitlines() if line.strip())

def run_ssh(cmd):
    full = [SSH_BIN, "-i", SSH_KEY, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new", SERVER, cmd]
    subprocess.check_call(full, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def rsync_pull(remote_dir, local_dir):
    os.makedirs(local_dir, exist_ok=True)
    rsync_cmd = [
        RSYNC_BIN, "-az", "--partial", "--prune-empty-dirs", "--remove-source-files",
        "--include", "*/", "--include", "*.txt.gz", "--exclude", "*",
        "-e", f"ssh -i {SSH_KEY} -o BatchMode=yes -o StrictHostKeyChecking=accept-new",
        f"{SERVER}:{remote_dir}/", local_dir + "/"
    ]
    subprocess.check_call(rsync_cmd)

########################## CHECK OPEN ##########################

# ------------------ mysqlbinlog verbose parser ------------------
# parse lines like:
# ### INSERT INTO `db`.`table`
# ### SET
# ###   @1=1
# ###   @2='foo'
#
# UPDATE has WHERE then SET.
# DELETE has WHERE.
re_event = re.compile(r"^###\s+(INSERT INTO|UPDATE|DELETE FROM)\s+`([^`]+)`\.`([^`]+)`")
re_at = re.compile(r"@(\d+)=((NULL)|'(.*)'|([^'\s].*))$")  # crude value capture

def parse_mysqlbinlog_verbose_file(path, target_table, mapping):
    """
    Returns consolidated_changes dict {pk: {'type': 'INSERT'|'UPDATE'|'DELETE', 'data': {...}}}
    mapping maps UNKNOWN_COLn -> real col name.
    """
    consolidated = {}
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
        state = None
        cur_schema = cur_table = None
        # temporary holders
        where_vals = {}
        set_vals = {}
        for ln in fh:
            ln = ln.rstrip("\n")
            m = re_event.match(ln)
            if m:
                # flush previous event if any
                state = None
                where_vals = {}
                set_vals = {}
                typ = m.group(1)
                cur_schema = m.group(2)
                cur_table = m.group(3)
                if cur_table != target_table:
                    state = "SKIP"
                    continue
                if typ.startswith("INSERT"):
                    state = "INSERT"
                elif typ.startswith("UPDATE"):
                    state = "UPDATE_WHERE"   # start with WHERE, then we will see SET
                elif typ.startswith("DELETE"):
                    state = "DELETE_WHERE"
                continue

            if not state or state == "SKIP":
                continue

            # lines with @N=... appear under WHERE or SET blocks prefixed by "###"
            # trim leading "###" and whitespace
            if ln.lstrip().startswith("###"):
                text = ln.lstrip()[3:].strip()
                # detect entering SET block
                if text.upper() == "SET":
                    if state == "UPDATE_WHERE":
                        state = "UPDATE_SET"
                    else:
                        state = "SET"
                    continue
                if text.upper() == "WHERE":
                    # already in WHERE for update/delete
                    continue
                # parse @N=value
                m2 = re_at.search(text)
                if m2:
                    idx = int(m2.group(1)) - 1
                    rawval = m2.group(2)
                    # normalize value: remove surrounding single quotes if present, handle NULL
                    if rawval == "NULL":
                        val = None
                    elif rawval.startswith("'") and rawval.endswith("'"):
                        # crude unescape of single quotes inside value
                        val = rawval[1:-1].replace("\\'", "'").replace('\\\\', '\\')
                    else:
                        val = rawval
                    key = f"UNKNOWN_COL{idx}"
                    if state in ("WHERE", "UPDATE_WHERE", "DELETE_WHERE"):
                        where_vals[key] = val
                    else:
                        # SET or UPDATE_SET or INSERT or other
                        set_vals[key] = val
                continue
            # blank or other lines ignored

            # End-of-block heuristic:
            # mysqlbinlog verbose often separates events by blank lines; when we hit a blank line
            # and we have set_vals or where_vals, commit the event.
            if ln.strip() == "":
                if state == "INSERT" and set_vals:
                    mapped = {mapping.get(k, k): v for k, v in set_vals.items()}
                    pk = mapped.get(PK_COL)
                    if pk is not None:
                        consolidated[pk] = {'type':'INSERT', 'data': mapped}
                    set_vals = {}
                elif state in ("DELETE_WHERE",) and where_vals:
                    mapped = {mapping.get(k, k): v for k, v in where_vals.items()}
                    pk = mapped.get(PK_COL)
                    if pk is not None:
                        # treat DELETE
                        if pk in consolidated and consolidated[pk]['type']=='INSERT':
                            del consolidated[pk]
                        else:
                            consolidated[pk] = {'type':'DELETE', 'data': mapped}
                    where_vals = {}
                elif state in ("UPDATE_SET","UPDATE_WHERE"):
                    # update event uses where_vals as before, set_vals as after
                    if set_vals or where_vals:
                        before = {mapping.get(k,k): v for k,v in where_vals.items()}
                        after = {mapping.get(k,k): v for k,v in set_vals.items()}
                        pk = after.get(PK_COL) or before.get(PK_COL)
                        if pk is not None:
                            # emulate your consolidation rules:
                            current_type = consolidated.get(pk, {}).get('type', 'UPDATE')
                            if current_type != 'INSERT':
                                current_type = 'UPDATE'
                            consolidated[pk] = {'type': current_type, 'data': after if after else before}
                    where_vals = {}; set_vals = {}
                # reset state to allow next event
                state = None
                continue
        # final tail commit if needed
        if state == "INSERT" and set_vals:
            mapped = {mapping.get(k, k): v for k, v in set_vals.items()}
            pk = mapped.get(PK_COL)
            if pk is not None:
                consolidated[pk] = {'type':'INSERT', 'data': mapped}
        if state in ("DELETE_WHERE",) and where_vals:
            mapped = {mapping.get(k, k): v for k, v in where_vals.items()}
            pk = mapped.get(PK_COL)
            if pk is not None:
                if pk in consolidated and consolidated[pk]['type']=='INSERT':
                    del consolidated[pk]
                else:
                    consolidated[pk] = {'type':'DELETE', 'data': mapped}
        if state in ("UPDATE_SET","UPDATE_WHERE") and (where_vals or set_vals):
            before = {mapping.get(k,k): v for k,v in where_vals.items()}
            after = {mapping.get(k,k): v for k,v in set_vals.items()}
            pk = after.get(PK_COL) or before.get(PK_COL)
            if pk is not None:
                current_type = consolidated.get(pk, {}).get('type', 'UPDATE')
                if current_type != 'INSERT':
                    current_type = 'UPDATE'
                consolidated[pk] = {'type': current_type, 'data': after if after else before}
    return consolidated

########################## CHECK CLOSE ##########################

custom_formatter = lambda x: (
    f"{x.year:04d}-{x.month:02d}-{x.day:02d} {x.hour:02d}:{x.minute:02d}:{x.second:02d}"
    if pd.notna(x) and hasattr(x, "strftime") else "0001-01-01 00:00:00"
)

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

        if not day_df.empty and pks_to_modify:
            day_df = day_df[~day_df[PK_COL].isin(pks_to_modify)]

        if upsert_rows:
            upsert_df = pd.DataFrame(upsert_rows)
            for col in ["date_time", "ts"]:
                if col in upsert_df.columns:
                     upsert_df[col] = upsert_df[col].apply(custom_formatter)
            
            final_df = pd.concat([day_df, upsert_df], ignore_index=True)

            cols_to_drop = ["file_id", "dst"]
            final_df = final_df.drop(columns=[c for c in cols_to_drop if c in final_df.columns])
        else:
            final_df = day_df

        os.makedirs(base_folder, exist_ok=True)
        fastparquet.write(file_path, final_df, compression='snappy', append=False, row_group_offsets=None, write_index=False)
        
        if final_df.empty:
            print(f"Applied {len(changes)} net changes to {file_path}. The file is now empty.")
        else:
            print(f"Applied {len(changes)} net changes to {file_path}. New row count: {len(final_df)}")

def main():
    start_dt, stop_dt = yesterday_range()
    start_s = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    stop_s = stop_dt.strftime("%Y-%m-%d %H:%M:%S")

    remote_cmd = remote_export_command(start_s, stop_s, REMOTE_DIR)
    print("Triggering remote export for", start_s, "->", stop_s)
    run_ssh(remote_cmd)

    print("Pulling completed files via rsync")
    rsync_pull(REMOTE_DIR, LOCAL_BINLOGS_DIR)

    # files = sorted([f for f in os.listdir(LOCAL_BINLOGS_DIR) if f.endswith(".txt.gz")])
    # if not files:
    #     print("no files to process"); return

    # consolidated_changes = {}
    # for fname in files:
    #     gzpath = os.path.join(LOCAL_BINLOGS_DIR, fname)
    #     print("parsing", gzpath)
    #     try:
    #         found = parse_mysqlbinlog_verbose_file(gzpath, TABLE, fix_binlog_cols_mapping)
    #         for pk, change in found.items():
    #             if pk in consolidated_changes and consolidated_changes[pk]['type']=='INSERT' and change['type']=='DELETE':
    #                 del consolidated_changes[pk]
    #                 continue
    #             consolidated_changes[pk] = change
    #     finally:
    #         # cleanup no matter what
    #         try:
    #             os.remove(gzpath)
    #         except FileNotFoundError:
    #             pass

    # if not consolidated_changes:
    #     print("no consolidated changes found"); return

    # # group by day and apply parquet upserts
    # changes_by_day = defaultdict(list)
    # for pk, change in consolidated_changes.items():
    #     dt_val = change['data'].get(DT_COL)
    #     if dt_val:
    #         # Safely format and extract the day part (YYYY-MM-DD)
    #         day = custom_formatter(dt_val)[:10]
    #         changes_by_day[day].append(change)

    # if not changes_by_day:
    #     print("No changes found for valid day partitions.")
    #     return

    # print(f"Found changes affecting {len(changes_by_day)} day partition(s). Applying changes...")
    # apply_changes_to_parquet(BASE_FOLDER, changes_by_day)
    # print("\nBinlog merge process finished successfully.")

if __name__ == "__main__":
    main()