#!/bin/bash

# Script to run tests with test Neo4j database
# Usage: ./run_tests.sh [--keep-running]

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Starting test environment...${NC}"

# Check if --keep-running flag is passed
KEEP_RUNNING=false
if [[ "$1" == "--keep-running" ]]; then
    KEEP_RUNNING=true
fi

# Function to cleanup on exit
cleanup() {
    if [[ "$KEEP_RUNNING" == false ]]; then
        echo -e "\n${YELLOW}🧹 Cleaning up test environment...${NC}"
        docker-compose -f docker-compose.test.yml down -v
    else
        echo -e "\n${YELLOW}ℹ️  Test Neo4j container is still running. To stop it, run:${NC}"
        echo "docker-compose -f docker-compose.test.yml down -v"
    fi
}

# Set up trap to cleanup on script exit (unless --keep-running)
if [[ "$KEEP_RUNNING" == false ]]; then
    trap cleanup EXIT
fi

# Stop any existing test containers
echo -e "${YELLOW}Stopping any existing test containers...${NC}"
docker-compose -f docker-compose.test.yml down -v 2>/dev/null || true

# Start test Neo4j container
echo -e "${GREEN}Starting test Neo4j container...${NC}"
docker-compose -f docker-compose.test.yml up -d

# Wait for Neo4j to be ready
echo -e "${YELLOW}Waiting for Neo4j to be ready...${NC}"
MAX_WAIT=60
WAIT_TIME=0
while ! docker-compose -f docker-compose.test.yml exec -T neo4j-test cypher-shell -u neo4j -p testpassword123 "RETURN 1" > /dev/null 2>&1; do
    if [ $WAIT_TIME -ge $MAX_WAIT ]; then
        echo -e "${RED}❌ Neo4j failed to start within ${MAX_WAIT} seconds${NC}"
        exit 1
    fi
    echo -n "."
    sleep 2
    WAIT_TIME=$((WAIT_TIME + 2))
done
echo -e "\n${GREEN}✅ Neo4j is ready!${NC}"

# Run tests
echo -e "\n${GREEN}🧪 Running tests...${NC}"
cd api

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Run pytest with verbose output and coverage
if python -m pytest tests/ -v --tb=short; then
    echo -e "\n${GREEN}✅ All tests passed!${NC}"
    TEST_RESULT=0
else
    echo -e "\n${RED}❌ Some tests failed${NC}"
    TEST_RESULT=1
fi

# Return to original directory
cd ..

# Exit with test result
exit $TEST_RESULT