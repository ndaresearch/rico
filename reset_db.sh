#!/bin/bash
# reset_db.sh - Clear Neo4j database for testing

echo "Clearing Neo4j database..."
docker exec rico-neo4j cypher-shell -u neo4j -p ricograph123 "MATCH (n) DETACH DELETE n" 

echo "Verifying empty database..."
COUNT=$(docker exec rico-neo4j cypher-shell -u neo4j -p ricograph123 "MATCH (n) RETURN COUNT(n) as count" --format plain | grep -o '[0-9]*' | tail -1)

if [ -z "$COUNT" ] || [ "$COUNT" = "0" ]; then
    echo "✅ Database cleared successfully (0 nodes)"
else
    echo "⚠️ Database has $COUNT nodes remaining"
fi