#!/usr/bin/env python3
# extract_data_docker.py - Extract data from SQL Server and save as Parquet using Dockerized Spark

import os
import argparse
import logging
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession

def get_spark_session(app_name):
    """Create and return a Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.jars", "/opt/spark/jars/mssql-jdbc-12.2.0.jre8.jar") \
        .getOrCreate()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_from_sql_server(spark, server, database, table, username, password, query=None):
    """Extract data from SQL Server using JDBC."""
    logger.info(f"Extracting data from {database}.{table if table else 'custom query'}")
    
    # JDBC connection URL
    jdbc_url = f"jdbc:sqlserver://{server};databaseName={database};trustServerCertificate=true"
    
    # JDBC connection properties
    connection_properties = {
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "user": username,
        "password": password
    }
    
    # Read data from SQL Server
    if query:
        # Use custom query with a temporary view name for the subquery
        df = spark.read.jdbc(
            url=jdbc_url,
            table=f"({query}) AS custom_query",
            properties=connection_properties
        )
    else:
        # Use specified table
        df = spark.read.jdbc(
            url=jdbc_url,
            table=table,
            properties=connection_properties
        )
    
    # Log row count
    row_count = df.count()
    logger.info(f"Extracted {row_count} rows from source")
    
    return df

def save_as_parquet(df, output_path, compression_method="snappy", partition_columns=None):
    """Save DataFrame as Parquet with specified compression method."""
    logger.info(f"Saving data as Parquet with {compression_method} compression")
    
    # Validate compression method
    valid_methods = ["snappy", "gzip", "lzo", "zstd", "none"]
    if compression_method.lower() not in valid_methods:
        logger.warning(f"Invalid compression method: {compression_method}. Using snappy instead.")
        compression_method = "snappy"
    
    # Add timestamp column for tracking extraction time
    from pyspark.sql.functions import lit
    df_with_timestamp = df.withColumn("extraction_timestamp", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    
    # Write with partitioning if specified
    if partition_columns:
        df_with_timestamp.write \
            .option("compression", compression_method) \
            .partitionBy(partition_columns) \
            .mode("overwrite") \
            .parquet(output_path)
    else:
        df_with_timestamp.write \
            .option("compression", compression_method) \
            .mode("overwrite") \
            .parquet(output_path)
    
    logger.info(f"Data saved to {output_path}")

def main():
    """Main function to extract data from SQL Server and save as Parquet."""
    # Load environment variables
    load_dotenv()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Extract data from SQL Server and save as Parquet using Dockerized Spark")
    
    # SQL Server connection parameters
    parser.add_argument("--server", default=os.getenv("SQL_SERVER"), help="SQL Server hostname/IP")
    parser.add_argument("--database", default=os.getenv("SQL_DATABASE"), help="Database name")
    parser.add_argument("--username", default=os.getenv("SQL_USERNAME"), help="SQL Server username")
    parser.add_argument("--password", default=os.getenv("SQL_PASSWORD"), help="SQL Server password")
    
    # Source data options
    parser.add_argument("--table", help="SQL Server table name")
    parser.add_argument("--query", help="Custom SQL query (alternative to --table)")
    
    # Output options
    parser.add_argument("--output-path", required=True, help="Output directory for Parquet files")
    parser.add_argument("--compression", default="snappy", 
                        choices=["snappy", "gzip", "lzo", "zstd", "none"],
                        help="Compression method for Parquet files")
    
    # Advanced options
    parser.add_argument("--partition-by", nargs="+", help="Columns to partition by")
    parser.add_argument("--app-name", default="SQL-to-Parquet", help="Spark application name")
    
    args = parser.parse_args()
    
    # Validate that either table or query is provided
    if not args.table and not args.query:
        parser.error("Either --table or --query must be specified")
    
    # Validate SQL Server connection parameters
    for param_name, param_value in [
        ("server", args.server),
        ("database", args.database),
        ("username", args.username),
        ("password", args.password)
    ]:
        if not param_value:
            parser.error(f"Missing SQL Server parameter: {param_name}. "
                         f"Provide it with --{param_name} or set {param_name.upper()} in .env file")
    
    # Get Spark session
    spark = None
    try:
        logger.info(f"Creating Spark session with app name: {args.app_name}")
        spark = get_spark_session(app_name=args.app_name)
        
        # Extract data from SQL Server
        df = extract_from_sql_server(
            spark=spark,
            server=args.server,
            database=args.database,
            table=args.table,
            username=args.username,
            password=args.password,
            query=args.query
        )
        
        # Save as Parquet
        save_as_parquet(
            df=df,
            output_path=args.output_path,
            compression_method=args.compression,
            partition_columns=args.partition_by
        )
        
        logger.info("Data extraction and saving completed successfully")
        
    except Exception as e:
        logger.error(f"Error during extraction process: {str(e)}")
        raise
    
    finally:
        # Stop Spark session
        if spark:
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()