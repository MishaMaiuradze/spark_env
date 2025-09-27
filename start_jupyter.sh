#!/bin/bash

echo "Starting Jupyter Lab in Docker container..."
echo
echo "Jupyter Lab will be available at: http://localhost:8888"
echo "Spark UI will be available at: http://localhost:4040"
echo

# Rebuild container with Jupyter support
echo "Rebuilding Docker container with Jupyter support..."
docker-compose build

# Start the container
echo "Starting container..."
docker-compose up -d

# Wait a moment for services to start
echo "Waiting for services to start..."
sleep 10

echo
echo "========================================"
echo "Services are ready!"
echo "========================================"
echo "Jupyter Lab: http://localhost:8888"
echo "Spark UI:    http://localhost:4040"
echo "Spark Master: http://localhost:8080"
echo "========================================"
echo
echo "Opening Jupyter Lab in your browser..."

# Try to open browser (works on most systems)
if command -v xdg-open > /dev/null; then
    xdg-open http://localhost:8888
elif command -v open > /dev/null; then
    open http://localhost:8888
else
    echo "Please open http://localhost:8888 in your browser"
fi

echo
echo "To stop the services, run: docker-compose down"