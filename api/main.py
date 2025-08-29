from fastapi import FastAPI, HTTPException, Depends, Security
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from neo4j import GraphDatabase
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime, date
import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/api.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(title="RICO Graph API", version="1.0.0")

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Add your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Key Security
API_KEY = os.getenv("API_KEY", "your-api-key-here")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def get_api_key(api_key: str = Security(api_key_header)):
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return api_key

# Neo4j connection
class Neo4jConnection:
    def __init__(self):
        self.driver = None
        self.connect()
    
    def connect(self):
        uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        user = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "password")
        
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            self.driver.verify_connectivity()
            logger.info("Connected to Neo4j")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            logger.warning("Starting in offline mode - database operations will fail")
            self.driver = None
    
    def close(self):
        if self.driver:
            self.driver.close()
    
    def execute_query(self, query, parameters=None):
        if not self.driver:
            raise HTTPException(status_code=503, detail="Database not available")
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]
    
    def execute_write(self, query, parameters=None):
        if not self.driver:
            raise HTTPException(status_code=503, detail="Database not available")
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                result = tx.run(query, parameters or {})
                tx.commit()
                return [record.data() for record in result]

# Initialize connection
db = Neo4jConnection()

# Import Pydantic models
from models.company import Company
from models.person import Person
from models.equipment import Equipment

class ChameleonDetectionParams(BaseModel):
    start_date: date = Field(default=date(2015, 1, 1))
    confidence_threshold: int = Field(default=70, ge=0, le=100)
    window_days: int = Field(default=180, ge=30, le=365)

# API Endpoints

@app.get("/")
async def root():
    return {"message": "RICO Graph API", "status": "online"}

@app.get("/health")
async def health_check():
    try:
        db.execute_query("MATCH (n) RETURN count(n) as count LIMIT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Database unavailable")

# Company endpoints
@app.post("/companies", dependencies=[Depends(get_api_key)])
async def create_company(company: Company):
    query = """
    CREATE (c:Company {
        dot_number: $dot_number,
        mc_number: $mc_number,
        duns_number: $duns_number,
        ein: $ein,
        legal_name: $legal_name,
        dba_name: $dba_name,
        entity_type: $entity_type,
        authority_status: $authority_status,
        safety_rating: $safety_rating,
        operation_classification: $operation_classification,
        company_type: $company_type,
        operating_model: $operating_model,
        parent_dot_model: $parent_dot_model,
        ultimate_parent_id: $ultimate_parent_id,
        consolidation_level: $consolidation_level,
        is_publicly_traded: $is_publicly_traded,
        parent_company_name: $parent_company_name,
        sec_cik: $sec_cik,
        known_subsidiaries: $known_subsidiaries,
        total_drivers: $total_drivers,
        total_trucks: $total_trucks,
        total_trailers: $total_trailers,
        chameleon_risk_score: $chameleon_risk_score,
        safety_risk_score: $safety_risk_score,
        financial_risk_score: $financial_risk_score,
        created_date: $created_date,
        last_updated: datetime(),
        mcs150_date: $mcs150_date,
        insurance_minimum: $insurance_minimum,
        cargo_carried: $cargo_carried,
        data_completeness_score: $data_completeness_score
    })
    RETURN c
    """
    try:
        result = db.execute_write(query, company.dict())
        logger.info(f"Created company: {company.dot_number}")
        return {"message": "Company created", "data": result}
    except Exception as e:
        logger.error(f"Failed to create company: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/companies/{dot_number}")
async def get_company(dot_number: int, api_key: str = Depends(get_api_key)):
    query = """
    MATCH (c:Company {dot_number: $dot_number})
    OPTIONAL MATCH (c)-[r:HAS_OFFICER]->(p:Person)
    OPTIONAL MATCH (c)-[:LOCATED_AT]->(l:Location)
    OPTIONAL MATCH (c)-[:OPERATES]->(e:Equipment)
    RETURN c as company,
           collect(DISTINCT p) as officers,
           collect(DISTINCT l) as locations,
           count(DISTINCT e) as equipment_count
    """
    try:
        result = db.execute_query(query, {"dot_number": dot_number})
        if not result:
            raise HTTPException(status_code=404, detail="Company not found")
        return result[0]
    except Exception as e:
        logger.error(f"Failed to get company: {e}")
        raise HTTPException(status_code=400, detail=str(e))

# Pattern detection endpoints
@app.post("/detect/chameleon", dependencies=[Depends(get_api_key)])
async def detect_chameleon_carriers(params: ChameleonDetectionParams):
    query = """
    // Main detection query for chameleon carrier chains
    MATCH (defunct:Company)-[:HAD_AUTHORITY]->(old_auth:Authority)
    WHERE old_auth.status IN ['REVOKED', 'INACTIVE', 'OUT_OF_SERVICE']
    AND old_auth.status_change_date >= date($start_date)
    
    // Find potential reincarnations
    MATCH (phoenix:Company)-[:HAS_AUTHORITY]->(new_auth:Authority)
    WHERE new_auth.granted_date >= old_auth.status_change_date
    AND new_auth.granted_date <= old_auth.status_change_date + duration({days: $window_days})
    AND new_auth.status = 'ACTIVE'
    
    // Check for connecting evidence
    OPTIONAL MATCH (defunct)-[:HAS_OFFICER]->(officer:Person)<-[:HAS_OFFICER]-(phoenix)
    OPTIONAL MATCH (defunct)-[:LOCATED_AT]->(address:Location)<-[:LOCATED_AT]-(phoenix)
    OPTIONAL MATCH (defunct)-[:OPERATES]->(equipment:Equipment)<-[:OPERATES]-(phoenix)
    
    WITH defunct, phoenix, old_auth, new_auth,
         COLLECT(DISTINCT officer) as shared_officers,
         COLLECT(DISTINCT address) as shared_addresses,
         COLLECT(DISTINCT equipment) as shared_equipment
    
    // Calculate confidence score
    WITH defunct, phoenix,
         SIZE(shared_officers) * 25 as officer_score,
         SIZE(shared_addresses) * 20 as address_score,
         SIZE(shared_equipment) * 35 as equipment_score,
         old_auth, new_auth
    
    WHERE (officer_score + address_score + equipment_score) >= $confidence_threshold
    
    RETURN defunct.dot_number as defunct_dot,
           defunct.legal_name as defunct_name,
           phoenix.dot_number as phoenix_dot,
           phoenix.legal_name as phoenix_name,
           (officer_score + address_score + equipment_score) as confidence_score
    ORDER BY confidence_score DESC
    LIMIT 50
    """
    
    try:
        result = db.execute_query(query, {
            "start_date": params.start_date.isoformat(),
            "window_days": params.window_days,
            "confidence_threshold": params.confidence_threshold
        })
        logger.info(f"Chameleon detection found {len(result)} matches")
        return {"matches": result, "count": len(result)}
    except Exception as e:
        logger.error(f"Chameleon detection failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/detect/equipment-transfers/{vin}")
async def detect_equipment_transfers(vin: str, api_key: str = Depends(get_api_key)):
    query = """
    MATCH (equip:Equipment {vin: $vin})
    MATCH (equip)<-[op:OPERATES]-(company:Company)
    WITH equip, company, op
    ORDER BY op.start_date
    
    RETURN company.dot_number as dot_number,
           company.legal_name as company_name,
           op.start_date as start_date,
           op.end_date as end_date,
           op.operation_type as type
    ORDER BY op.start_date
    """
    
    try:
        result = db.execute_query(query, {"vin": vin})
        return {"vin": vin, "transfer_history": result}
    except Exception as e:
        logger.error(f"Equipment transfer detection failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/network/{dot_number}")
async def get_company_network(dot_number: int, depth: int = 2, api_key: str = Depends(get_api_key)):
    """Get the network of relationships for a company"""
    query = """
    MATCH path = (c:Company {dot_number: $dot_number})-[*1..$depth]-(connected)
    WHERE connected:Company OR connected:Person OR connected:Equipment
    WITH c, connected, relationships(path) as rels
    RETURN DISTINCT 
        c.dot_number as source_dot,
        c.legal_name as source_name,
        labels(connected)[0] as target_type,
        CASE 
            WHEN connected:Company THEN connected.dot_number
            WHEN connected:Person THEN connected.person_id
            WHEN connected:Equipment THEN connected.vin
        END as target_id,
        CASE 
            WHEN connected:Company THEN connected.legal_name
            WHEN connected:Person THEN connected.full_name
            WHEN connected:Equipment THEN connected.vin
        END as target_name,
        [r IN rels | type(r)] as relationship_types
    LIMIT 500
    """
    
    try:
        result = db.execute_query(query, {"dot_number": dot_number, "depth": depth})
        return {"company": dot_number, "network": result, "node_count": len(result)}
    except Exception as e:
        logger.error(f"Network query failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))

# Person endpoints
@app.post("/persons", dependencies=[Depends(get_api_key)])
async def create_person(person: Person):
    query = """
    CREATE (p:Person {
        person_id: $person_id,
        full_name: $full_name,
        first_name: $first_name,
        last_name: $last_name,
        date_of_birth: $date_of_birth,
        email: $email,
        phone: $phone,
        first_seen: $first_seen,
        last_seen: $last_seen,
        source: $source
    })
    RETURN p
    """
    try:
        result = db.execute_write(query, person.dict())
        logger.info(f"Created person: {person.person_id}")
        return {"message": "Person created", "data": result}
    except Exception as e:
        logger.error(f"Failed to create person: {e}")
        raise HTTPException(status_code=400, detail=str(e))

# Equipment endpoints
@app.post("/equipment", dependencies=[Depends(get_api_key)])
async def create_equipment(equipment: Equipment):
    query = """
    CREATE (e:Equipment {
        vin: $vin,
        type: $type,
        make: $make,
        model: $model,
        year: $year,
        title_state: $title_state,
        title_number: $title_number,
        registration_states: $registration_states,
        status: $status,
        lien_holder: $lien_holder,
        purchase_price: $purchase_price,
        purchase_date: $purchase_date,
        under_lease: $under_lease,
        lease_company: $lease_company,
        incident_count: $incident_count,
        inspection_failure_rate: $inspection_failure_rate,
        first_seen: $first_seen,
        last_verified: $last_verified
    })
    RETURN e
    """
    try:
        result = db.execute_write(query, equipment.dict())
        logger.info(f"Created equipment: {equipment.vin}")
        return {"message": "Equipment created", "data": result}
    except Exception as e:
        logger.error(f"Failed to create equipment: {e}")
        raise HTTPException(status_code=400, detail=str(e))

# Relationship endpoints
@app.post("/relationships/operates", dependencies=[Depends(get_api_key)])
async def create_operates_relationship(
    dot_number: int, 
    vin: str, 
    start_date: date,
    end_date: Optional[date] = None,
    operation_type: str = "OWNED"
):
    query = """
    MATCH (c:Company {dot_number: $dot_number})
    MATCH (e:Equipment {vin: $vin})
    CREATE (c)-[r:OPERATES {
        start_date: date($start_date),
        end_date: date($end_date),
        operation_type: $operation_type,
        reported_date: date()
    }]->(e)
    RETURN c.legal_name as company, e.vin as equipment, r as relationship
    """
    try:
        result = db.execute_write(query, {
            "dot_number": dot_number,
            "vin": vin,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat() if end_date else None,
            "operation_type": operation_type
        })
        logger.info(f"Created OPERATES relationship: {dot_number} -> {vin}")
        return {"message": "Relationship created", "data": result}
    except Exception as e:
        logger.error(f"Failed to create relationship: {e}")
        raise HTTPException(status_code=400, detail=str(e))

# Cleanup on shutdown
@app.on_event("shutdown")
def shutdown():
    db.close()
    logger.info("Database connection closed")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)