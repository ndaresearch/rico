import logging
from contextlib import contextmanager
from typing import Generator

from neo4j import GraphDatabase, Session
from config import settings

logger = logging.getLogger(__name__)


class Neo4jConnection:
    """Manages Neo4j database connections with connection pooling."""
    
    def __init__(self):
        """Initialize Neo4j connection with settings from config."""
        self.uri = settings.neo4j_uri
        self.user = settings.neo4j_user
        self.password = settings.neo4j_password
        
        if not self.password:
            raise ValueError("NEO4J_PASSWORD is required in settings")
        
        self.driver = GraphDatabase.driver(
            self.uri,
            auth=(self.user, self.password),
            max_connection_pool_size=50,
            connection_acquisition_timeout=30,
            max_transaction_retry_time=30
        )
    
    def close(self):
        """Close the driver connection"""
        if self.driver:
            self.driver.close()
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Get a database session with automatic cleanup"""
        session = self.driver.session()
        try:
            yield session
        finally:
            session.close()
    
    def verify_connectivity(self) -> bool:
        """Verify database connectivity.
        
        Returns:
            bool: True if connected, False otherwise
        """
        try:
            with self.get_session() as session:
                result = session.run("RETURN 1 as test")
                return result.single()["test"] == 1
        except Exception as e:
            logger.error(f"Database connectivity check failed: {e}")
            return False


# Singleton instance
db = Neo4jConnection()

class BaseRepository:
    """Base repository with common Neo4j operations.
    
    Provides common database operations that all specific repositories inherit.
    Handles query execution, transactions, and connection management.
    """
    
    def __init__(self):
        self.db = db
    
    def execute_query(self, query: str, parameters: dict = None) -> list:
        """Execute a read query and return results.
        
        Args:
            query: Cypher query string
            parameters: Optional query parameters
            
        Returns:
            list: Query results as list of dictionaries
        """
        with self.db.get_session() as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]
    
    def execute_write(self, query: str, parameters: dict = None) -> dict:
        """Execute a write query and return summary.
        
        Args:
            query: Cypher query string
            parameters: Optional query parameters
            
        Returns:
            dict: Summary of changes made to the database
        """
        with self.db.get_session() as session:
            result = session.run(query, parameters or {})
            summary = result.consume()
            return {
                "nodes_created": summary.counters.nodes_created,
                "nodes_deleted": summary.counters.nodes_deleted,
                "relationships_created": summary.counters.relationships_created,
                "relationships_deleted": summary.counters.relationships_deleted,
                "properties_set": summary.counters.properties_set,
            }
    
    def transaction_write(self, queries: list) -> dict:
        """Execute multiple write queries in a transaction.
        
        Args:
            queries: List of tuples (query, parameters)
            
        Returns:
            dict: Success status
        """
        with self.db.get_session() as session:
            with session.begin_transaction() as tx:
                for query, params in queries:
                    tx.run(query, params or {})
                tx.commit()
                return {"success": True}