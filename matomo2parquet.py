import logging
import os
import sys
import argparse
from calendar import monthrange
import gc
import warnings
import psutil
import tracemalloc

import pymysql
import pymysql.cursors
import pandas as pd

import datetime

from config import read_ini, resolve_chunk_size
from s3logger import S3Logger 

# logger for s3
s3logger = S3Logger('matomo2parquet');

def emit_progress(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line)
    s3logger.loginfo(message)

def log_memory_usage(stage, debug_mode=False):
    """
    Log memory usage at different stages of processing
    """
    if debug_mode:
        # Get memory info
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        memory_percent = process.memory_percent()
        
        # Convert bytes to MB
        rss_mb = memory_info.rss / 1024 / 1024
        vms_mb = memory_info.vms / 1024 / 1024
        
        # Get system memory info
        system_memory = psutil.virtual_memory()
        available_mb = system_memory.available / 1024 / 1024
        
        memory_msg = f"[MEMORY {stage}] RSS: {rss_mb:.2f}MB, VMS: {vms_mb:.2f}MB, Percent: {memory_percent:.2f}%, Available: {available_mb:.2f}MB"
        
        s3logger.loginfo(memory_msg)
        print(memory_msg)
        
        # Force garbage collection if memory usage is high
        if memory_percent > 70:
            s3logger.logwarning(f"High memory usage detected ({memory_percent:.2f}%), forcing garbage collection")
            collected = gc.collect()
            s3logger.loginfo(f"Garbage collector freed {collected} objects")
            
        # Emergency mode if memory usage is extremely high
        if memory_percent > 85:
            s3logger.logerror(f"CRITICAL: Memory usage extremely high ({memory_percent:.2f}%)")
            s3logger.logerror("Consider processing data in smaller date ranges or using more selective queries")
            
        return rss_mb, memory_percent
    return None, None

def optimize_dataframe_memory(df, debug_mode=False):
    """
    Optimize DataFrame memory usage by downcasting numeric types
    """
    if df.empty:
        return df
        
    if debug_mode:
        original_memory = df.memory_usage(deep=True).sum() / 1024 / 1024
        s3logger.loginfo(f"Original DataFrame memory: {original_memory:.2f}MB")
    
    # Downcast integer columns
    for col in df.select_dtypes(include=['int']).columns:
        df[col] = pd.to_numeric(df[col], downcast='integer')
    
    # Downcast float columns
    for col in df.select_dtypes(include=['float']).columns:
        df[col] = pd.to_numeric(df[col], downcast='float')
    
    # Convert object columns to category if they have few unique values
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].nunique() / len(df) < 0.5:  # Less than 50% unique values
            df[col] = df[col].astype('category')
    
    if debug_mode:
        optimized_memory = df.memory_usage(deep=True).sum() / 1024 / 1024
        saved_memory = original_memory - optimized_memory
        s3logger.loginfo(f"Optimized DataFrame memory: {optimized_memory:.2f}MB (saved: {saved_memory:.2f}MB)")
    
    return df

def get_dataframe_memory_usage(df, name):
    """
    Get detailed memory usage of a DataFrame
    """
    memory_usage = df.memory_usage(deep=True).sum()
    memory_mb = memory_usage / 1024 / 1024
    rows, cols = df.shape
    
    msg = f"[DF MEMORY] {name}: {memory_mb:.2f}MB, Shape: ({rows}, {cols})"
    s3logger.loginfo(msg)
    print(msg)
    
    return memory_mb

# Chunked reader removed (rollback): use awswrangler's mysql.read_sql_query directly.

def process_data_type(query, data_type, conn_params, s3_bucket, partition_cols, site, year, month, day, dry_run, debug_mode, chunk_size=100000):
    """
    Process a specific data type (visits or events) using Server-Side Cursor
    to stream results in chunks without loading entire dataset in memory.
    
    Uses SSCursor which executes the query ONCE on the server and streams
    results in batches, avoiding repeated queries.
    """
    if debug_mode:
        print(f"\n{'='*60}")
        print(f"[DEBUG] Processing {data_type.upper()}")
        print(f"{'='*60}")
        print(f"[DEBUG] Query:\n{query}")
        print(f"[DEBUG] Chunk size: {chunk_size}")
        print(f"{'='*60}\n")
        s3logger.loginfo(f"Executing {data_type} query with chunk_size={chunk_size}: {query}")
    else:
        emit_progress(f"[{data_type}] Query started with chunk_size={chunk_size}")
    
    log_memory_usage(f"BEFORE_{data_type.upper()}_QUERY", debug_mode)

    wr = None
    if not dry_run:
        import awswrangler as wr
        emit_progress(f"[{data_type}] Target dataset: s3://{s3_bucket}")
    else:
        emit_progress(f"[{data_type}] Dry run mode enabled (no writes to S3)")

    # Use Server-Side Cursor to stream results in chunks.
    streaming_conn = pymysql.connect(
        host=conn_params['host'],
        user=conn_params['user'],
        passwd=conn_params['passwd'],
        db=conn_params['db'],
        connect_timeout=conn_params.get('connect_timeout', 5),
        cursorclass=pymysql.cursors.SSCursor
    )
    
    try:
        chunk_num = 0
        total_rows = 0
        persisted_rows = 0
        persisted_chunks = 0

        # pandas warns for non-SQLAlchemy DBAPI connections; this path is intentional
        # because SSCursor streams rows from MySQL without loading everything in memory.
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="pandas only supports SQLAlchemy connectable.*",
                category=UserWarning,
            )
            for chunk_df in pd.read_sql(query, streaming_conn, chunksize=chunk_size):
                chunk_num += 1
                rows_in_chunk = len(chunk_df)
                total_rows += rows_in_chunk
                emit_progress(f"[{data_type}] Chunk {chunk_num}: fetched {rows_in_chunk} rows (total fetched: {total_rows})")
                
                if debug_mode:
                    print(f"[DEBUG] {data_type} chunk {chunk_num}: {rows_in_chunk} rows (total so far: {total_rows})")
                
                if chunk_df.empty:
                    continue
                
                log_memory_usage(f"{data_type.upper()}_CHUNK_{chunk_num}", debug_mode)
                
                if debug_mode:
                    get_dataframe_memory_usage(chunk_df, f"{data_type}_chunk_{chunk_num}")
                    chunk_df = optimize_dataframe_memory(chunk_df, debug_mode)
                
                # Process data based on type
                if data_type == "visits":
                    chunk_df['day'] = day if day is not None else 1
                    chunk_df['month'] = month
                    chunk_df['year'] = year
                else:  # events
                    chunk_df['day'] = chunk_df['server_time'].dt.day
                    chunk_df['month'] = chunk_df['server_time'].dt.month
                    chunk_df['year'] = chunk_df['server_time'].dt.year
                
                # Write chunk to S3
                if not dry_run:
                    # Use 'append' mode to add to existing dataset
                    # First chunk can use 'overwrite_partitions' if needed
                    write_mode = 'append'
                    emit_progress(f"[{data_type}] Chunk {chunk_num}: persisting {rows_in_chunk} rows to s3://{s3_bucket}")
                    wr.s3.to_parquet(
                        df=chunk_df,
                        path='s3://' + s3_bucket,
                        dataset=True,
                        mode=write_mode,
                        partition_cols=partition_cols
                    )
                    persisted_rows += rows_in_chunk
                    persisted_chunks += 1
                    emit_progress(f"[{data_type}] Chunk {chunk_num}: persisted successfully to s3://{s3_bucket}")
                else:
                    emit_progress(f"[{data_type}] Chunk {chunk_num}: dry run, skip persist to s3://{s3_bucket}")
                
                # Clear chunk from memory
                del chunk_df
                gc.collect()
                
                log_memory_usage(f"AFTER_{data_type.upper()}_CHUNK_{chunk_num}_CLEANUP", debug_mode)

        if total_rows == 0:
            s3logger.logwarning(f"No {data_type} data for site: {site} year: {year} month: {month} day: {day}")
            emit_progress(f"[{data_type}] No data found")
            if debug_mode:
                print(f"[DEBUG] {data_type.upper()}: No data found")
        else:
            s3logger.loginfo(f"Completed {data_type}: processed {total_rows} rows in {chunk_num} chunks")
            if dry_run:
                emit_progress(f"[{data_type}] Completed dry run: {total_rows} rows across {chunk_num} chunks")
            else:
                emit_progress(f"[{data_type}] Completed: persisted {persisted_rows} rows in {persisted_chunks} chunks to s3://{s3_bucket}")
            if debug_mode:
                print(f"\n[DEBUG] {data_type.upper()} COMPLETE: {total_rows} total rows in {chunk_num} chunks\n")
            
    finally:
        streaming_conn.close()
        s3logger.loginfo(f"Closed streaming connection for {data_type}")
    
    log_memory_usage(f"AFTER_{data_type.upper()}_COMPLETE", debug_mode)
    return {
        "data_type": data_type,
        "chunks": chunk_num,
        "rows": total_rows,
        "persisted_chunks": persisted_chunks,
        "persisted_rows": persisted_rows,
        "bucket": s3_bucket,
        "dry_run": dry_run,
    }

def main(args_dict):

    config_file_path = args_dict.get('config_file_path', None)

    site = args_dict.get('site', None)
    year = args_dict.get('year', None) 
    month = args_dict.get('month', None)
    day = args_dict.get('day', None)

    dry_run = args_dict.get('dry_run', False)
    debug_mode = args_dict.get('debug', False)
    
    # Start memory tracking if debug mode is enabled
    if debug_mode:
        tracemalloc.start()
        s3logger.loginfo("DEBUG MODE: Memory tracking enabled")
        
    log_memory_usage("STARTUP", debug_mode)

    try: 
        # read config file
        config = read_ini(config_file_path);
        
        log_memory_usage("CONFIG_LOADED", debug_mode)

        db_host = config["MATOMO_DB"]["HOST"] 
        db_port = int(config["MATOMO_DB"].get("PORT", "3306"))
        db_username = config["MATOMO_DB"]["USERNAME"] 
        db_passwd = config["MATOMO_DB"]["PASSWORD"] 
        db_database = config["MATOMO_DB"]["DATABASE"] 

        s3_visits_bucket = config["S3_STATS"]["VISITS_PATH"]
        s3_events_bucket = config["S3_STATS"]["EVENTS_PATH"]
   
    
        # logger bucket
        s3logger.set_bucket(config["S3_LOGS"]["LOGS_PATH"])
        s3logger.loginfo("Starting procesing on datetime: %s site: %s year: %s month: %s day: %s" % ( datetime.datetime.now(), site, year, month, day))
        emit_progress(
            f"Starting processing site={site} year={year} month={month} day={day} "
            f"matomo_host={db_host}:{db_port} visits_path=s3://{s3_visits_bucket} "
            f"events_path=s3://{s3_events_bucket}"
        )


    except Exception as e:
        print("Error : %s" % e)
        # print stack trace
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # Prepare connection parameters (connection created per data type for SSCursor)
    conn_params = {
        'host': db_host,
        'port': db_port,
        'user': db_username,
        'passwd': db_passwd,
        'db': db_database,
        'connect_timeout': 5
    }
    
    # Test connection
    try:
        log_memory_usage("TESTING_DB_CONNECTION", debug_mode)
        test_conn = pymysql.connect(**conn_params)
        test_conn.close()
        s3logger.loginfo("Database connection test successful")
        emit_progress("Matomo DB connection test successful")
    except pymysql.MySQLError as e:
        s3logger.logerror("ERROR: Unexpected error: Could not connect to MySQL instance.")
        s3logger.logerror(e)
        emit_progress("Matomo DB connection test failed")
        sys.exit(1)

    
    # get data from mysql
    # Build date range strings safely using datetime objects
    def build_date_range(year: int, month: int, day_start: int, day_end: int):
        """Build date range strings safely from validated integer parameters"""
        start_date = datetime.datetime(year, month, day_start, 0, 0, 0)
        end_date = datetime.datetime(year, month, day_end, 23, 59, 59)
        return start_date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S')
    
    # Validate parameters are integers (argparse already enforces this, but double-check)
    if not all(isinstance(x, int) for x in [year, month, site]):
        raise ValueError(f"Invalid parameter types: year={type(year)}, month={type(month)}, site={type(site)}")
    
    if day is not None and not isinstance(day, int):
        raise ValueError(f"Invalid day type: {type(day)}")
    
    # Build queries based on whether day is specified
    if day is None:
        # Monthly query - calculate last day of month
        last_day = monthrange(year, month)[1]
        start_dt, end_dt = build_date_range(year, month, 1, last_day)
        
        visit_query = f"""SELECT * FROM matomo_log_visit WHERE idvisit in 
                        (SELECT idvisit FROM matomo_log_link_visit_action 
                         WHERE idsite = {int(site)} 
                         AND server_time BETWEEN '{start_dt}.000' AND '{end_dt}.999')"""
        
        event_query = f"""SELECT va.*, a.`type` as action_type, a.name as action_url, a.url_prefix as action_url_prefix
                         FROM (matomo_log_link_visit_action va 
                               LEFT JOIN matomo_log_action a ON (va.idaction_url = a.idaction)) 
                         WHERE idsite = {int(site)} 
                         AND server_time BETWEEN '{start_dt}.000' AND '{end_dt}.999' 
                         AND NOT RIGHT(a.name, 8) = '.pdf.jpg'"""
    else:
        # Daily query
        start_dt, end_dt = build_date_range(year, month, day, day)
        
        visit_query = f"""SELECT * FROM matomo_log_visit 
                         WHERE idsite = {int(site)} 
                         AND visit_last_action_time BETWEEN '{start_dt}' AND '{end_dt}'"""
        
        event_query = f"""SELECT va.*, a.`type` as action_type, a.name as action_url, a.url_prefix as action_url_prefix
                         FROM (matomo_log_link_visit_action va 
                               LEFT JOIN matomo_log_action a ON (va.idaction_url = a.idaction)) 
                         WHERE idsite = {int(site)} 
                         AND server_time BETWEEN '{start_dt}.000' AND '{end_dt}.999' 
                         AND NOT RIGHT(a.name, 8) = '.pdf.jpg'"""

    
        # Setup partition columns
    partition_cols = ['idsite', 'year', 'month']
    if day is not None:
        partition_cols.append('day')
    
    # Get chunk size from config with safe fallback
    raw_chunk_size = config.get("PROCESSING", "CHUNK_SIZE", fallback="10000")
    chunk_size, used_default = resolve_chunk_size(raw_chunk_size, default=10000)
    if used_default and str(raw_chunk_size).strip() not in {"", "10000"}:
        s3logger.logwarning(f"Invalid CHUNK_SIZE value '{raw_chunk_size}', using default 10000")
    s3logger.loginfo(f"Using chunk size: {chunk_size}")
    
    # Process visits first (streaming with SSCursor)
    emit_progress("Starting VISITS extraction and persistence")
    visits_result = process_data_type(visit_query, "visits", conn_params, s3_visits_bucket, partition_cols, site, year, month, day, dry_run, debug_mode, chunk_size)
    
    # Process events separately after visits are processed and memory freed
    emit_progress("Starting EVENTS extraction and persistence")
    events_result = process_data_type(event_query, "events", conn_params, s3_events_bucket, partition_cols, site, year, month, day, dry_run, debug_mode, chunk_size)
                       
    log_memory_usage("BEFORE_CLEANUP", debug_mode)
        
    # Final cleanup
    gc.collect()
    
    log_memory_usage("AFTER_FINAL_CLEANUP", debug_mode)
    
    # Show memory usage summary if debug mode is enabled
    if debug_mode:
        if tracemalloc.is_tracing():
            current, peak = tracemalloc.get_traced_memory()
            s3logger.loginfo(f"MEMORY SUMMARY - Current: {current / 1024 / 1024:.2f}MB, Peak: {peak / 1024 / 1024:.2f}MB")
            tracemalloc.stop()
                       
    if dry_run:
        emit_progress(
            "Dry run summary: "
            f"visits_rows={visits_result['rows']} events_rows={events_result['rows']} (no S3 writes performed)"
        )
    else:
        emit_progress(
            "S3 persistence summary: "
            f"visits={visits_result['persisted_rows']} rows to s3://{visits_result['bucket']} | "
            f"events={events_result['persisted_rows']} rows to s3://{events_result['bucket']}"
        )
        emit_progress("S3 persistence completed successfully")

    s3logger.loginfo("Ending procesing on datetime : %s site: %s year: %s month: %s day: %s" % ( datetime.datetime.now(), site, year, month, day))
    emit_progress(f"Finished processing site={site} year={year} month={month} day={day}")


def validate_cli_args(parser, args):
    if args.site <= 0:
        parser.error("argument -s/--site: must be a positive integer")

    if args.year < 1 or args.year > 9999:
        parser.error("argument -y/--year: must be between 1 and 9999")

    if args.month < 1 or args.month > 12:
        parser.error("argument -m/--month: must be between 1 and 12")

    if args.day is not None:
        if args.day < 1:
            parser.error("argument -d/--day: must be greater than or equal to 1")

        max_day = monthrange(args.year, args.month)[1]
        if args.day > max_day:
            parser.error(
                f"argument -d/--day: must be between 1 and {max_day} for {args.year}-{args.month:02d}"
            )


def parse_args():

    parser = argparse.ArgumentParser(
        description="Usage Statistics Matomo MySQL to S3 persistence",
        epilog="Example: python matomo2parquet.py -c config.ini -s 48 -y 2026 -m 1 [-d 15]",
    )

    parser.add_argument("--loglevel", 
                        help="Log level", 
                        default=logging.INFO)

    parser.add_argument("-c",
                        "--config_file_path",
                        default='config.ini',
                        help="config file",
                        required=False)

    parser.add_argument("-s",
                        "--site",
                        type=int,
                        help="site id",
                        required=True)

    parser.add_argument("-y",
                        "--year",
                        type=int,
                        help="year",
                        required=True)
    
    parser.add_argument("-m",
                        "--month",
                        type=int,
                        help="month",
                        required=True)

    parser.add_argument("-d",
                        "--day",
                        default=None,
                        type=int,
                        help="day",
                        required=False)
    
    # Kept for backwards compatibility with runner.py (not used in this script)
    parser.add_argument("-t",
                        "--type", 
                        default='R', 
                        type=str, 
                        help="(R|L|N) - deprecated, kept for compatibility",
                        required=False)

   
    parser.add_argument("-v", "--verbose", action='store_true', help="verbose mode")

    parser.add_argument("--debug", default=False, action='store_true', help="enable debug mode with detailed memory monitoring")
    
    parser.add_argument("--dry_run", action='store_true', help="dont write to s3")

    args = parser.parse_args()
    validate_cli_args(parser, args)

    return args 

  

if __name__ == "__main__":

    args = vars(parse_args())
    print("Arguments: ", args )     

    # set log level
    s3logger.set_log_level(args.get('loglevel'))
    
    if (args.get('verbose')):
        s3logger.set_log_level(logging.INFO)
        s3logger.set_local_mode(True)

    # call main function
    main(args)
