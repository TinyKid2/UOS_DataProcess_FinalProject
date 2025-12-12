#!/bin/bash

echo "Starting ETL Pipeline with Docker Compose..."

# Create necessary directories
mkdir -p uploads nifi/data nifi/content nifi/database nifi/flowfile nifi/provenance
mkdir -p models results spark web eval

# Stop any existing containers
docker-compose down

# Build and start services
docker-compose up --build -d

echo "Waiting for services to start..."
sleep 10

# Check service status
echo -e "\n=== Service Status ==="
docker-compose ps

echo -e "\n=== Available Services ==="
echo "Web Server: http://localhost:5000"
echo "NiFi UI: http://localhost:8080 (admin/adminPassword123)"
echo "Kafka UI: http://localhost:8090"
echo "Spark UI: http://localhost:8081"

echo -e "\n=== Logs ==="
echo "To view logs: docker-compose logs -f [service-name]"
echo "Services: web-server, eval-node, kafka, nifi, mariadb"