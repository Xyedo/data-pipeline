.PHONY: help build up down logs clean restart pipeline check-health

# Default target
help:
	@echo "Available commands:"
	@echo "  build        - Build Docker images"
	@echo "  up           - Start all services"
	@echo "  down         - Stop all services"
	@echo "  logs         - View logs from all services"
	@echo "  clean        - Remove all containers and volumes"
	@echo "  restart      - Restart all services"
	@echo "  pipeline     - Run the data pipeline manually"
	@echo "  check-health - Check ClickHouse health"
	@echo "  clickhouse-cli - Connect to ClickHouse CLI"

# Build Docker images
build:
	docker-compose build

# Start all services
up:
	docker-compose up -d

# Stop all services
down:
	docker-compose down

# View logs
logs:
	docker-compose logs -f

# Clean everything
clean:
	docker-compose down -v --remove-orphans
	docker system prune -f

# Restart services
restart: down up

# Run pipeline manually
pipeline:
	docker-compose run --rm data-pipeline python main.py

# Check ClickHouse health
check-health:
	docker-compose exec clickhouse clickhouse-client --query "SELECT 1"

# Connect to ClickHouse CLI
clickhouse-cli:
	docker-compose exec clickhouse clickhouse-client --user admin --password password123 --database netflix_warehouse

# View table counts
table-counts:
	docker-compose exec clickhouse clickhouse-client --user admin --password password123 --database netflix_warehouse --query "SELECT table, total_rows FROM system.tables WHERE database = 'netflix_warehouse' AND total_rows > 0"