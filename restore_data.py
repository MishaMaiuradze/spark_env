#!/usr/bin/env python3
# restore_data.py - Restore data from Parquet files to SQL Server

import os
import argparse
import logging
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, inspect, Table, Column, MetaData, String, Integer, Float, DateTime
from sqlalchemy.dialects.mssql import DATETIME2
import duckdb
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def connect_to_duckdb(database_path=":memory:"):
    """Create or connect to a DuckDB database."""
    logger.info(f"Connecting to DuckDB at {database_path}")
    return duckdb.connect(database_path)

def load_parquet_to_pandas(parquet_path):
    """Load Parquet file(s) into a pandas DataFrame using DuckDB."""
    logger.info(f"Loading Parquet data from {parquet_path}")
    
    # Check if the path exists
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Parquet path not found: {parquet_path}")
    
    # Connect to DuckDB
    conn = connect_to_duckdb()
    
    try:
        # Load all Parquet files if directory, or single file if file
        query = f"SELECT * FROM read_parquet('{parquet_path}')"
        df = conn.execute(query).fetchdf()
        
        logger.info(f"Loaded {len(df)} rows from Parquet")
        
        return df
    
    finally:
        conn.close()

def create_sqlalchemy_engine(server, database, username, password, driver='ODBC Driver 17 for SQL Server'):
    """Create SQLAlchemy engine for SQL Server connection."""
    logger.info(f"Creating connection to SQL Server: {server}, Database: {database}")
    
    # Create connection string
    conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver.replace(' ', '+')}"
    
    # Create engine
    engine = create_engine(conn_str)
    
    return engine

def get_sql_data_type(pandas_dtype, column_name):
    """Map pandas dtype to SQLAlchemy data type for SQL Server."""
    if pd.api.types.is_integer_dtype(pandas_dtype):
        return Integer()
    elif pd.api.types.is_float_dtype(pandas_dtype):
        return Float()
    elif pd.api.types.is_datetime64_dtype(pandas_dtype):
        return DATETIME2()
    else:
        # For string/object types, use VARCHAR with appropriate length
        return String(255)  # Default length, can be adjusted

def create_table_if_not_exists(engine, df, table_name, schema=None, if_exists='fail'):
    """Create a table in SQL Server based on DataFrame schema if it doesn't exist."""
    logger.info(f"Checking if table {table_name} exists")
    
    inspector = inspect(engine)
    table_exists = table_name in inspector.get_table_names(schema=schema)
    
    if table_exists:
        if if_exists == 'fail':
            raise ValueError(f"Table {table_name} already exists")
        elif if_exists == 'replace':
            logger.info(f"Dropping existing table {table_name}")
            metadata = MetaData()
            metadata.reflect(bind=engine, only=[table_name], schema=schema)
            if table_name in metadata.tables:
                metadata.tables[f"{schema}.{table_name}" if schema else table_name].drop(engine)
            table_exists = False
    
    if not table_exists or if_exists == 'replace':
        logger.info(f"Creating table {table_name}")
        
        metadata = MetaData()
        
        # Create table definition
        columns = []
        for column_name, dtype in df.dtypes.items():
            sql_type = get_sql_data_type(dtype, column_name)
            columns.append(Column(column_name, sql_type))
        
        # Create table
        table = Table(table_name, metadata, *columns, schema=schema)
        metadata.create_all(engine)
        
        logger.info(f"Created table {table_name} with {len(columns)} columns")

def insert_data_to_sql(engine, df, table_name, schema=None, batch_size=1000, if_exists='append'):
    """Insert DataFrame data into SQL Server table."""
    logger.info(f"Beginning data insertion to {table_name}")
    
    # Create table if it doesn't exist
    create_table_if_not_exists(engine, df, table_name, schema, if_exists)
    
    # Get total rows to insert
    total_rows = len(df)
    logger.info(f"Preparing to insert {total_rows} rows")
    
    # Insert in batches
    for i in range(0, total_rows, batch_size):
        batch_end = min(i + batch_size, total_rows)
        logger.info(f"Inserting batch {i//batch_size + 1}/{(total_rows + batch_size - 1)//batch_size}: rows {i} to {batch_end}")
        
        batch_df = df.iloc[i:batch_end]
        
        # Handle schema for to_sql
        if schema:
            batch_df.to_sql(
                name=table_name,
                schema=schema,
                con=engine,
                if_exists='append',
                index=False,
                chunksize=batch_size
            )
        else:
            batch_df.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False,
                chunksize=batch_size
            )
    
    logger.info(f"Successfully inserted {total_rows} rows into {table_name}")

def main():
    """Main function to restore data from Parquet to SQL Server."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Restore data from Parquet files to SQL Server")
    
    # Input options
    parser.add_argument("--parquet-path", required=True, help="Path to Parquet file or directory")
    
    # SQL Server connection parameters
    parser.add_argument("--server", required=True, help="SQL Server hostname/IP")
    parser.add_argument("--database", required=True, help="Database name")
    parser.add_argument("--username", required=True, help="SQL Server username")
    parser.add_argument("--password", required=True, help="SQL Server password")
    parser.add_argument("--driver", default="ODBC Driver 17 for SQL Server", help="ODBC Driver name")
    
    # Output options
    parser.add_argument("--table-name", required=True, help="Target table name in SQL Server")
    parser.add_argument("--schema", help="SQL Server schema name (default: dbo)")
    parser.add_argument("--if-exists", choices=["fail", "replace", "append"], default="fail",
                        help="Action if table exists: fail, replace, or append")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for inserting rows")
    
    # DuckDB options for preprocessing
    parser.add_argument("--transform-query", help="SQL query to transform data before insertion")
    
    args = parser.parse_args()
    
    try:
        # Load data from Parquet
        if args.transform_query:
            # Load with transformation using DuckDB
            logger.info(f"Applying transformation query: {args.transform_query}")
            conn = connect_to_duckdb()
            conn.execute(f"CREATE VIEW parquet_view AS SELECT * FROM read_parquet('{args.parquet_path}')")
            df = conn.execute(args.transform_query).fetchdf()
            conn.close()
        else:
            # Load directly
            df = load_parquet_to_pandas(args.parquet_path)
        
        # Create SQLAlchemy engine
        engine = create_sqlalchemy_engine(
            server=args.server,
            database=args.database,
            username=args.username,
            password=args.password,
            driver=args.driver
        )
        
        # Insert data to SQL Server
        insert_data_to_sql(
            engine=engine,
            df=df,
            table_name=args.table_name,
            schema=args.schema,
            batch_size=args.batch_size,
            if_exists=args.if_exists
        )
        
        logger.info("Data restoration completed successfully")
        
    except Exception as e:
        logger.error(f"Error during restoration process: {str(e)}")
        raise

if __name__ == "__main__":
    main()