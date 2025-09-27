FROM apache/spark:3.5.0-python3

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    unixodbc \
    unixodbc-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft SQL Server ODBC driver
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir \
    pyodbc \
    pandas \
    pyarrow \
    duckdb \
    matplotlib \
    seaborn \
    sqlalchemy \
    python-dotenv \
    jupyter \
    jupyterlab \
    notebook \
    py4j

# Set environment variables
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:${PYTHONPATH}"

# Create a directory for data
RUN mkdir -p /data

# Create a startup script
RUN echo '#!/bin/bash' > /start-spark.sh && \
    echo 'cd "${SPARK_HOME}"' >> /start-spark.sh && \
    echo 'bin/spark-class org.apache.spark.deploy.master.Master --ip 0.0.0.0 --port 7077 --webui-port 8080 &' >> /start-spark.sh && \
    echo 'sleep 5' >> /start-spark.sh && \
    echo 'bin/spark-class org.apache.spark.deploy.worker.Worker spark://$(hostname):7077 --webui-port 8081 &' >> /start-spark.sh && \
    echo 'echo "Spark started. Master UI available at http://localhost:8080"' >> /start-spark.sh && \
    echo 'echo "Worker UI available at http://localhost:8081"' >> /start-spark.sh && \
    echo 'sleep 3' >> /start-spark.sh && \
    echo 'cd /app' >> /start-spark.sh && \
    echo 'jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token="" --NotebookApp.password="" &' >> /start-spark.sh && \
    echo 'echo "Jupyter Lab started. Available at http://localhost:8888"' >> /start-spark.sh && \
    echo 'tail -f /dev/null' >> /start-spark.sh && \
    chmod +x /start-spark.sh

# Expose Spark ports
EXPOSE 4040 8080 8081 7077

# Set working directory
WORKDIR /app

# Set entry point to the startup script
ENTRYPOINT ["/start-spark.sh"]