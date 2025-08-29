#!/bin/bash

# deploy.sh - Main deployment script

set -e  # Exit on error

# Configuration
PROJECT_NAME="rico-graph"
BACKUP_DIR="./backup"
NEO4J_DATA_DIR="./neo4j/data"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Create directory structure
setup_directories() {
    print_status "Creating directory structure..."
    mkdir -p neo4j/{data,logs,import,plugins,conf}
    mkdir -p backup
    mkdir -p logs/api
    mkdir -p api
    
    # Set proper permissions
    chmod 755 neo4j/{data,logs,import,plugins,conf}
    chmod 755 backup
    print_status "Directories created successfully"
}

# Generate secure passwords
generate_passwords() {
    print_status "Generating secure passwords..."
    
    # Generate URL-safe passwords without special characters that break Neo4j
    NEO4J_PASSWORD=$(openssl rand -hex 16)
    API_KEY=$(openssl rand -hex 32)
    
    # Create .env file
    cat > .env <<EOF
# Generated on $(date)
NEO4J_PASSWORD=${NEO4J_PASSWORD}
API_KEY=${API_KEY}
NEO4J_URI=bolt://neo4j:7687
NEO4J_USER=neo4j
EOF
    
    chmod 600 .env
    print_status "Passwords generated and saved to .env"
    print_warning "IMPORTANT: Save these credentials securely!"
    echo "NEO4J_PASSWORD: ${NEO4J_PASSWORD}"
    echo "API_KEY: ${API_KEY}"
}

# Create Python API files
setup_api() {
    print_status "Setting up API..."
    
    # Create requirements.txt
    cat > api/requirements.txt <<'EOF'
fastapi==0.109.0
uvicorn==0.27.0
neo4j==5.16.0
pydantic==2.5.0
python-dotenv==1.0.0
python-multipart==0.0.6
httpx==0.26.0
EOF
    
    # Create Dockerfile for API
    cat > api/Dockerfile <<'EOF'
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
EOF
    
    print_status "API setup complete"
}

# Initialize Neo4j schema
init_schema() {
    print_status "Waiting for Neo4j to be ready..."
    sleep 10
    
    print_status "Initializing Neo4j schema..."
    
    # Load environment variables
    source .env
    
    # Execute schema initialization using docker exec
    docker exec -i rico-neo4j cypher-shell -u neo4j -p "${NEO4J_PASSWORD}" < init_schema.cypher
    
    print_status "Schema initialized successfully"
}

# Main deployment function
deploy() {
    print_status "Starting RICO Graph deployment..."
    
    # Check if Docker is installed
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    # Check if Docker Compose is installed
    if ! command -v docker-compose &> /dev/null; then
        print_warning "docker-compose not found, trying docker compose..."
        if ! docker compose version &> /dev/null; then
            print_error "Docker Compose is not installed. Please install it first."
            exit 1
        fi
        COMPOSE_CMD="docker compose"
    else
        COMPOSE_CMD="docker-compose"
    fi
    
    # Setup
    setup_directories
    generate_passwords
    setup_api
    
    # Update docker-compose.yml with generated passwords
    source .env
    # macOS and Linux compatible sed - use | as delimiter to avoid issues with / in passwords
    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' "s|your-secure-password-here|${NEO4J_PASSWORD}|g" docker-compose.yml
        sed -i '' "s|your-api-key-here|${API_KEY}|g" docker-compose.yml
    else
        sed -i "s|your-secure-password-here|${NEO4J_PASSWORD}|g" docker-compose.yml
        sed -i "s|your-api-key-here|${API_KEY}|g" docker-compose.yml
    fi
    
    # Start services
    print_status "Starting Docker containers..."
    $COMPOSE_CMD up -d
    
    # Wait for Neo4j to be ready
    print_status "Waiting for Neo4j to initialize..."
    sleep 20
    
    # Initialize schema
    init_schema
    
    print_status "Deployment complete!"
    print_status "Neo4j Browser: http://localhost:7474"
    print_status "API Documentation: http://localhost:8000/docs"
    print_status "Use the credentials from .env file to login"
}

# Backup function
backup() {
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    BACKUP_FILE="${BACKUP_DIR}/neo4j_backup_${TIMESTAMP}.dump"
    
    print_status "Creating backup: ${BACKUP_FILE}"
    
    source .env
    
    docker exec rico-neo4j neo4j-admin database dump neo4j --to-path=/backup --verbose
    
    # Compress the backup
    gzip "${BACKUP_DIR}/neo4j.dump"
    mv "${BACKUP_DIR}/neo4j.dump.gz" "${BACKUP_FILE}.gz"
    
    print_status "Backup created: ${BACKUP_FILE}.gz"
    
    # Clean old backups (keep last 7)
    print_status "Cleaning old backups..."
    ls -t ${BACKUP_DIR}/*.dump.gz | tail -n +8 | xargs -r rm
}

# Restore function
restore() {
    if [ -z "$1" ]; then
        print_error "Please provide backup file path"
        echo "Usage: $0 restore <backup_file>"
        exit 1
    fi
    
    BACKUP_FILE=$1
    
    if [ ! -f "$BACKUP_FILE" ]; then
        print_error "Backup file not found: $BACKUP_FILE"
        exit 1
    fi
    
    print_warning "This will replace all current data. Continue? (y/n)"
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        print_status "Restore cancelled"
        exit 0
    fi
    
    print_status "Stopping Neo4j..."
    docker-compose stop neo4j
    
    print_status "Restoring from: $BACKUP_FILE"
    
    # Extract if compressed
    if [[ $BACKUP_FILE == *.gz ]]; then
        gunzip -c "$BACKUP_FILE" > "${BACKUP_DIR}/restore.dump"
        RESTORE_FILE="${BACKUP_DIR}/restore.dump"
    else
        RESTORE_FILE=$BACKUP_FILE
    fi
    
    # Clear existing data
    rm -rf ${NEO4J_DATA_DIR}/databases/neo4j
    rm -rf ${NEO4J_DATA_DIR}/transactions/neo4j
    
    # Restore
    docker run --rm \
        -v $(pwd)/${BACKUP_DIR}:/backup \
        -v $(pwd)/${NEO4J_DATA_DIR}:/data \
        neo4j:5.15-community \
        neo4j-admin database load neo4j --from-path=/backup --verbose
    
    print_status "Starting Neo4j..."
    docker-compose start neo4j
    
    print_status "Restore complete"
}

# Health check function
health_check() {
    print_status "Checking system health..."
    
    # Check if containers are running
    if docker ps | grep -q rico-neo4j; then
        print_status "✓ Neo4j container is running"
    else
        print_error "✗ Neo4j container is not running"
    fi
    
    if docker ps | grep -q rico-api; then
        print_status "✓ API container is running"
    else
        print_error "✗ API container is not running"
    fi
    
    # Check Neo4j connectivity
    if curl -s http://localhost:7474 > /dev/null; then
        print_status "✓ Neo4j Browser is accessible"
    else
        print_error "✗ Neo4j Browser is not accessible"
    fi
    
    # Check API health
    if curl -s http://localhost:8000/health > /dev/null; then
        print_status "✓ API is responding"
    else
        print_error "✗ API is not responding"
    fi
    
    # Check disk space
    DISK_USAGE=$(df -h . | awk 'NR==2 {print $5}' | sed 's/%//')
    if [ $DISK_USAGE -lt 80 ]; then
        print_status "✓ Disk usage is ${DISK_USAGE}%"
    else
        print_warning "⚠ Disk usage is ${DISK_USAGE}%"
    fi
    
    # Check Neo4j logs for errors
    if docker logs rico-neo4j --tail 50 2>&1 | grep -q ERROR; then
        print_warning "⚠ Recent errors found in Neo4j logs"
    else
        print_status "✓ No recent errors in Neo4j logs"
    fi
}

# Import data function
import_data() {
    print_status "Importing initial data..."
    
    # This is a placeholder - replace with your actual import logic
    python3 - <<'EOF'
import requests
import json
from datetime import date

API_URL = "http://localhost:8000"
API_KEY = open('.env').read().split('API_KEY=')[1].split('\n')[0]

headers = {"X-API-Key": API_KEY}

# Example: Create a test company
company_data = {
    "dot_number": 12345,
    "mc_number": "MC-67890",
    "legal_name": "Test Trucking Inc",
    "entity_type": "CARRIER",
    "authority_status": "ACTIVE",
    "total_drivers": 50,
    "total_trucks": 30,
    "created_date": "2020-01-01"
}

response = requests.post(
    f"{API_URL}/companies",
    headers=headers,
    json=company_data
)

if response.status_code == 200:
    print("Test company created successfully")
else:
    print(f"Failed to create company: {response.text}")
EOF
}

# Stop services
stop_services() {
    print_status "Stopping services..."
    docker-compose down
    print_status "Services stopped"
}

# Show logs
show_logs() {
    SERVICE=${1:-all}
    
    if [ "$SERVICE" == "all" ]; then
        docker-compose logs -f
    else
        docker-compose logs -f $SERVICE
    fi
}

# Main script logic
case "$1" in
    deploy)
        deploy
        ;;
    backup)
        backup
        ;;
    restore)
        restore "$2"
        ;;
    health)
        health_check
        ;;
    import)
        import_data
        ;;
    stop)
        stop_services
        ;;
    logs)
        show_logs "$2"
        ;;
    *)
        echo "Usage: $0 {deploy|backup|restore|health|import|stop|logs}"
        echo ""
        echo "Commands:"
        echo "  deploy   - Deploy the entire stack"
        echo "  backup   - Create a backup of Neo4j data"
        echo "  restore  - Restore from a backup file"
        echo "  health   - Check system health"
        echo "  import   - Import initial data"
        echo "  stop     - Stop all services"
        echo "  logs     - Show logs (optional: service name)"
        exit 1
        ;;
esac