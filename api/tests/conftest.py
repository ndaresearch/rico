"""
Shared pytest configuration for all tests.
Sets up test database connection and common fixtures.
"""
import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load test environment variables before any imports
from dotenv import load_dotenv

# Load test-specific environment variables
test_env_path = Path(__file__).parent.parent / ".env.test"
if test_env_path.exists():
    load_dotenv(test_env_path, override=True)
else:
    # Fallback to hardcoded test values if .env.test doesn't exist
    os.environ["NEO4J_URI"] = "bolt://localhost:7688"
    os.environ["NEO4J_USER"] = "neo4j"
    os.environ["NEO4J_PASSWORD"] = "testpassword123"
    os.environ["API_KEY"] = "test-api-key"