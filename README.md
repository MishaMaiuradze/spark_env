# SQL Server to Parquet Data Pipeline

A comprehensive data pipeline solution for extracting data from SQL Server, storing it as compressed Parquet files, analyzing it with DuckDB, and restoring it back to SQL Server.

## Project Overview

This project implements a complete ETL (Extract, Transform, Load) pipeline with the following components:

1. **Extraction**: Using PySpark to extract data from SQL Server and save it as compressed Parquet files
2. **Analysis**: Using DuckDB to read and analyze the Parquet files
3. **Restoration**: Loading data from Parquet files back into SQL Server

The pipeline is designed to be modular, configurable, and efficient for handling large datasets.

## Project Structure

```
sql-parquet-pipeline/
│
├── extract_data.py           # Extract data from SQL Server to Parquet
├── analyze_parquet.py        # Analyze Parquet files using DuckDB
├── restore_data.py           # Restore data from Parquet to SQL Server
│
├── setup_environment.sh      # Environment setup script
├── requirements.txt          # Python dependencies
│
├── data/                     # Data storage directory
│   ├── raw/                  # Raw data from extraction
│   ├── processed/            # Processed/transformed data
│   └── output/               # Output data and reports
│
├── logs/                     # Log files
│
├── drivers/                  # JDBC drivers for SQL Server
│   └── mssql-jdbc-12.2.0.jre8.jar
│
├── tests/                    # Unit and integration tests
│
└── .env                      # Environment variables (create this file manually)
```

## Prerequisites

- Python 3.8 or higher
- Java 8 or higher (for PySpark)
- SQL Server instance with appropriate permissions
- ODBC Driver for SQL Server

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/sql-parquet-pipeline.git
   cd sql-parquet-pipeline
   ```

2. Run the setup script to create a virtual environment and install dependencies:
   ```bash
   chmod +x setup_environment.sh
   ./setup_environment.sh
   ```

3. Activate the virtual environment:
   ```bash
   source venv/bin/activate
   ```

4. Create a `.env` file with your database credentials:
   ```
   SQL_SERVER=your_server_name_or_ip
   SQL_DATABASE=your_database_name
   SQL_USERNAME=your_username
   SQL_PASSWORD=your_password
   ```

## Usage

### Extract Data from SQL Server to Parquet

```bash
./extract_data.py \
  --server your_server \
  --database your_database \
  --username your_username \
  --password your_password \
  --table your_table_name \
  --output-path data/raw/your_data \
  --compression snappy
```

You can also use a custom SQL query instead of specifying a table:

```bash
./extract_data.py \
  --server your_server \
  --database your_database \
  --username your_username \
  --password your_password \
  --query "SELECT * FROM your_table WHERE condition = 'value'" \
  --output-path data/raw/your_data \
  --compression zstd
```

Available compression methods:
- `snappy` (default): Good balance of speed and compression
- `gzip`: Higher compression ratio but slower
- `zstd`: Best compression ratio, moderate speed
- `lzo`: Fastest decompression
- `none`: No compression

### Analyze Parquet Files with DuckDB

```bash
./analyze_parquet.py \
  --parquet-path data/raw/your_data \
  --table-name your_table \
  --query "SELECT column1, COUNT(*) as count FROM your_table GROUP BY column1 ORDER BY count DESC" \
  --column column1 \
  --plot-type bar \
  --save-results \
  --output-dir data/output
```

This will:
1. Load the Parquet files into DuckDB
2. Run your custom query
3. Create a visualization for the specified column
4. Save the results to the output directory

### Restore Data from Parquet to SQL Server

```bash
./restore_data.py \
  --parquet-path data/raw/your_data \
  --server your_server \
  --database your_database \
  --username your_username \
  --password your_password \
  --table-name your_new_table \
  --if-exists replace \
  --batch-size 2000
```

You can also transform the data before restoration using DuckDB:

```bash
./restore_data.py \
  --parquet-path data/raw/your_data \
  --transform-query "SELECT column1, column2, column3*2 as doubled_column FROM parquet_view WHERE column4 > 100" \
  --server your_server \
  --database your_database \
  --username your_username \
  --password your_password \
  --table-name your_transformed_table
```

## Compression Benchmarks

Below is a comparison of different compression methods on a sample dataset:

| Compression Method | File Size | Compression Ratio | Write Speed | Read Speed |
|-------------------|-----------|-------------------|------------|-----------|
| None              | 100 MB    | 1:1               | Fastest    | Fastest   |
| Snappy            | 45 MB     | 2.2:1             | Fast       | Fast      |
| LZO               | 40 MB     | 2.5:1             | Medium     | Fastest   |
| GZIP              | 35 MB     | 2.9:1             | Slow       | Medium    |
| ZSTD              | 30 MB     | 3.3:1             | Medium     | Medium    |

*Note: Actual results will vary based on your data characteristics.*

## Tips for Optimal Performance

1. **Partitioning**: For large datasets, use the `--partition-by` option in `extract_data.py` to partition by date or category columns.

2. **Compression Selection**:
   - Use `snappy` for general purpose (good balance)
   - Use `zstd` when storage space is a priority
   - Use `lzo` when read speed is critical

3. **Batch Size**: Adjust the `--batch-size` parameter in `restore_data.py` based on your server capacity. Larger batches are faster but use more memory.

4. **Memory Tuning**: For large datasets, you may need to allocate more memory to PySpark:
   ```bash
   export PYSPARK_SUBMIT_ARGS="--driver-memory 4g --executor-memory 4g pyspark-shell"
   ```

5. **Incremental Updates**: For incremental loads, use timestamp-based filtering:
   ```bash
   ./extract_data.py --query "SELECT * FROM your_table WHERE last_modified > '2023-01-01'" 
   ```

## Adding to GitHub

1. Initialize a Git repository (if not already done):
   ```bash
   git init
   ```

2. Create a `.gitignore` file:
   ```
   # Python
   __pycache__/
   *.py[cod]
   *$py.class
   venv/
   
   # Data and logs
   data/
   logs/
   
   # Credentials
   .env
   
   # IDE
   .idea/
   .vscode/
   ```

3. Add, commit, and push your files:
   ```bash
   git add .
   git commit -m "Initial commit: SQL Server to Parquet pipeline"
   git remote add origin https://github.com/yourusername/sql-parquet-pipeline.git
   git push -u origin main
   ```

## Troubleshooting

### Common Issues

1. **JDBC Driver Not Found**:
   - Ensure the JDBC driver is downloaded to the `drivers/` directory
   - Check that the driver path is correctly specified in `extract_data.py`

2. **SQL Server Connection Issues**:
   - Verify server name, credentials, and firewall settings
   - Make sure SQL Server allows remote connections

3. **PySpark Memory Issues**:
   - Increase JVM memory as described in the performance tips section
   - Process data in smaller batches

4. **ODBC Driver Issues**:
   - Install the appropriate ODBC driver for your platform
   - On Linux: `sudo apt-get install unixodbc-dev msodbcsql17`
   - On macOS: Install via Homebrew: `brew install unixodbc`

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
