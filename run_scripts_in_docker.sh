#!/bin/bash
# run_extract_in_docker.sh - Run the data extraction inside Docker container

echo "Running data extraction inside Docker container..."

# Run the extraction script inside the container
docker exec -it pyspark-sql-container python /app/extract_data.py \
    --server "192.168.56.1" \
    --database "DataWarehouse" \
    --username "airbyte_user" \
    --password "airbyte_user" \
    --table "dim_customer" \
    --output-path "/data/output/dim_customer.parquet" \
    --app-name "SQLToParquetExtraction"

echo "Data extraction completed. Files should be available in ./data/output/"


# Run the extraction script inside the container
docker exec -it pyspark-sql-container python /app/restore_data.py \
  --parquet-path data/raw/your_data \
  --server your_server \
  --database your_db \
  --username your_user \
  --password your_pass \
  --table-name your_new_table \
  --if-exists replace

echo "Data extraction completed. Files should be available in ./data/output/"