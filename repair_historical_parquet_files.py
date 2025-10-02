import os
import pandas as pd
import fastparquet
import glob
import re
from datetime import datetime
import numpy as np

# Constants
MIN_DATE = "2010-01-02 00:00:00"
BASE_FOLDER = "/root/data"
DT_FORMAT_REGEX = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"

def validate_and_clean_df(df, file_path):
    """Validates DataFrame in memory, raising an error if any row is invalid."""
    print(f"Validating {file_path} with {len(df)} rows")
    
    # Expected dtypes
    expected_dtypes = {
        "id": np.int64,
        "date_time": "string",
        "value": "float64",
        "ts": "string"
    }
    
    # Check for missing columns
    for col in expected_dtypes:
        if col not in df:
            raise ValueError(f"Missing column '{col}' in {file_path}")
    
    # Convert dtypes to ensure consistency
    cleaned_df = pd.DataFrame({
        "id": pd.Series(dtype=np.int64),
        "date_time": pd.Series(dtype="string"),
        "value": pd.Series(dtype="float64"),
        "ts": pd.Series(dtype="string")
    })
    
    # Validate each row
    for idx, row in df.iterrows():
        cleaned_row = {}
        
        # id: Must be int64, non-negative
        try:
            id_val = np.int64(row["id"])
            if id_val < 0:
                raise ValueError(f"Row {idx} in {file_path}: Negative id value: {row['id']}")
            cleaned_row["id"] = id_val
        except (ValueError, TypeError, OverflowError) as e:
            raise ValueError(f"Row {idx} in {file_path}: Invalid int64 for id: {row['id']} ({str(e)})")
        
        # date_time: Must be string, 19 chars, valid format
        try:
            dt_str = str(row["date_time"])
            if len(dt_str) != 19:
                raise ValueError(f"Row {idx} in {file_path}: date_time length {len(dt_str)} != 19: {dt_str}")
            if not re.match(DT_FORMAT_REGEX, dt_str):
                raise ValueError(f"Row {idx} in {file_path}: Invalid date_time format: {dt_str}")
            cleaned_row["date_time"] = dt_str
        except Exception as e:
            raise ValueError(f"Row {idx} in {file_path}: date_time error: {str(e)}")
        
        # value: Must be float64 or null
        try:
            if pd.isna(row["value"]):
                cleaned_row["value"] = np.nan
            else:
                cleaned_row["value"] = float(row["value"])
        except (ValueError, TypeError) as e:
            raise ValueError(f"Row {idx} in {file_path}: Invalid float64 for value: {row['value']} ({str(e)})")
        
        # ts: Must be string, 19 chars, valid format
        try:
            ts_str = str(row["ts"])
            if len(ts_str) != 19:
                raise ValueError(f"Row {idx} in {file_path}: ts length {len(ts_str)} != 19: {ts_str}")
            if not re.match(DT_FORMAT_REGEX, ts_str):
                raise ValueError(f"Row {idx} in {file_path}: Invalid ts format: {ts_str}")
            cleaned_row["ts"] = ts_str
        except Exception as e:
            raise ValueError(f"Row {idx} in {file_path}: ts error: {str(e)}")
        
        cleaned_df = pd.concat([cleaned_df, pd.DataFrame([cleaned_row])], ignore_index=True)
    
    return cleaned_df

def repair_historical_data():
    """Repairs Parquet files older than MIN_DATE in memory, raising errors for invalid data."""
    min_date = datetime.strptime(MIN_DATE, "%Y-%m-%d %H:%M:%S")
    
    # Find Parquet files
    files = glob.glob(os.path.join(BASE_FOLDER, "*.parquet"))
    historical_files = []
    for f in files:
        try:
            file_date_str = os.path.basename(f).replace(".parquet", "")
            file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
            if file_date < min_date:
                historical_files.append(f)
        except ValueError:
            print(f"Skipping file with invalid date format: {f}")
    
    if not historical_files:
        print(f"No historical Parquet files found (before {MIN_DATE})")
        return
    
    print(f"Found {len(historical_files)} historical Parquet files to repair")
    
    for file_path in historical_files:
        try:
            # Read Parquet
            df = pd.read_parquet(file_path)
            if df.empty:
                print(f"Skipping empty file: {file_path}")
                continue
            
            # Validate and clean in memory
            cleaned_df = validate_and_clean_df(df, file_path)
            
            # Overwrite Parquet file
            fastparquet.write(file_path, cleaned_df, compression="snappy", append=False)
            print(f"Overwrote {file_path} with {len(cleaned_df)} valid rows")
            
        except ValueError as e:
            print(f"Validation error in {file_path}: {str(e)}")
            raise  # Stop processing on first invalid file
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
            raise

def main():
    print("="*50 + "\nREPAIRING HISTORICAL PARQUET FILES\n" + "="*50)
    try:
        repair_historical_data()
    except Exception as e:
        print(f"Repair failed: {str(e)}")
        raise
    print("="*50 + "\nREPAIR COMPLETE\n" + "="*50)

if __name__ == "__main__":
    main()