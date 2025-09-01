# RICO Graph API

Graph-based fraud detection system for the trucking industry using Neo4j.

## Quick Start

### Prerequisites
- Python 3.8+
- Neo4j 4.x or 5.x
- Docker (for testing)

### Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your Neo4j credentials
```

### Run with Docker
```bash
docker-compose up -d
```

### Run Locally
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

## API Documentation

**Interactive documentation available at: http://localhost:8000/docs**

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI Schema**: http://localhost:8000/openapi.json

### Authentication
All endpoints except `/health` require API key authentication:
```bash
curl -H "X-API-Key: your_api_key_here" http://localhost:8000/carriers
```

## Data Model

### Entities
- **TargetCompany**: Large companies that contract carriers (e.g., JB Hunt)
- **Carrier**: Trucking companies with safety/violation data
- **InsuranceProvider**: Insurance companies
- **Person**: Officers and executives

### Relationships
- `(:TargetCompany)-[:CONTRACTS_WITH]->(:Carrier)`
- `(:TargetCompany)-[:HAS_EXECUTIVE]->(:Person)`
- `(:Carrier)-[:INSURED_BY]->(:InsuranceProvider)`
- `(:Carrier)-[:MANAGED_BY]->(:Person)`

## Testing

```bash
# Run all tests with test database
./run_tests.sh

# Keep test database running
./run_tests.sh --keep-running

# Run specific tests
pytest tests/test_carrier_endpoints.py -v
```

## Configuration

Environment variables (see `.env.example`):
- `NEO4J_URI`: Database URI (default: bolt://localhost:7687)
- `NEO4J_USER`: Username (default: neo4j)
- `NEO4J_PASSWORD`: Password (required)
- `API_KEY`: API authentication key (optional)

## Import Scripts

```bash
# Import JB Hunt carriers
python scripts/ingest/jb_hunt_carriers_import.py

# Create officers from carriers
python scripts/ingest/create_officers_from_carriers.py

# Fix insurance relationships
python scripts/ingest/fix_insurance_relationships.py
```

## Project Structure

```
api/
├── models/           # Pydantic models
├── repositories/     # Neo4j repository classes
├── routes/          # FastAPI route handlers
├── scripts/         # Import and utility scripts
├── tests/           # Test files
├── config.py        # Configuration management
├── database.py      # Database connection
└── main.py          # FastAPI application
```

## Development

```bash
# Format code
black api/

# Run type checking
mypy api/

# Run linting
pylint api/
```

## Monitoring

- Health check: `GET /health`
- Logs: `logs/api.log`