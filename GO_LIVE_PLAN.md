# GO_LIVE_PLAN.md - RICO Graph API Production Deployment Guide

## Overview
This guide provides step-by-step instructions for deploying the RICO Graph API with Neo4j database to a production VPS. The deployment prioritizes security and reliability while avoiding over-engineering.

---

## STEP 1: Pre-Deployment Preparation

### Role
You are a DevOps engineer preparing to deploy a FastAPI application with Neo4j database to a production VPS. You need to ensure all prerequisites are met and existing data is safely backed up.

### Context
- Current setup: Local development environment with Docker
- Target: Ubuntu/Debian VPS with root or sudo access
- Application: FastAPI (Python) + Neo4j graph database
- Purpose: Secure API for web application integration

### Tasks

1. **Verify VPS Access and Requirements**
   ```bash
   # SSH into your VPS
   ssh your_user@your_vps_ip
   
   # Check OS version (should be Ubuntu 20.04+ or Debian 11+)
   lsb_release -a
   
   # Check available resources (minimum: 2GB RAM, 20GB disk)
   free -h
   df -h
   
   # Check current running services
   sudo netstat -tulpn
   ```

2. **Backup Existing Data (if applicable)**
   ```bash
   # If you have an existing Neo4j instance, backup the data
   # Create backup directory
   mkdir -p ~/backups/$(date +%Y%m%d)
   
   # If using Docker, backup Neo4j data
   sudo docker exec old-neo4j-container neo4j-admin dump --to=/data/backup.dump
   sudo docker cp old-neo4j-container:/data/backup.dump ~/backups/$(date +%Y%m%d)/
   
   # Backup any existing .env files
   cp /path/to/old/app/.env ~/backups/$(date +%Y%m%d)/env_backup
   ```

3. **Document Current Configuration**
   ```bash
   # Save current Docker containers info
   sudo docker ps -a > ~/backups/$(date +%Y%m%d)/docker_containers.txt
   
   # Save current environment variables (sanitized)
   env | grep -E "API|NEO4J|SEARCH" | sed 's/=.*/=REDACTED/' > ~/backups/$(date +%Y%m%d)/env_vars.txt
   ```

### Verification
- ✅ Can SSH into VPS successfully
- ✅ VPS has minimum 2GB RAM and 20GB free disk space
- ✅ Existing data is backed up (if applicable)
- ✅ Documented current configuration

### Before Moving On
Ensure you have backed up any critical data and have documented your current setup. You should have SSH access working reliably.

---

## STEP 2: VPS System Preparation

### Role
You are a system administrator setting up a Ubuntu/Debian VPS for hosting a Docker-based application. You need to install required software and configure the system.

### Context
The VPS needs Docker, Docker Compose, Git, and basic security tools. The system should be updated and hardened for production use.

### Tasks

1. **Update System Packages**
   ```bash
   # Update package list and upgrade system
   sudo apt update && sudo apt upgrade -y
   
   # Install essential packages
   sudo apt install -y \
     curl \
     wget \
     git \
     vim \
     htop \
     ufw \
     fail2ban \
     software-properties-common \
     apt-transport-https \
     ca-certificates \
     gnupg \
     lsb-release
   ```

2. **Install Docker**
   ```bash
   # Add Docker's official GPG key
   curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
   
   # Add Docker repository
   echo \
     "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
     $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
   
   # Install Docker Engine
   sudo apt update
   sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
   
   # Add your user to docker group (replace 'your_user' with actual username)
   sudo usermod -aG docker $USER
   
   # Log out and back in for group changes to take effect
   exit
   # SSH back in
   ssh your_user@your_vps_ip
   ```

3. **Configure Firewall**
   ```bash
   # Enable UFW firewall
   sudo ufw default deny incoming
   sudo ufw default allow outgoing
   
   # Allow SSH (important - do this before enabling!)
   sudo ufw allow 22/tcp
   
   # Allow HTTP and HTTPS
   sudo ufw allow 80/tcp
   sudo ufw allow 443/tcp
   
   # Enable firewall
   sudo ufw --force enable
   
   # Check status
   sudo ufw status
   ```

4. **Setup Fail2ban for SSH Protection**
   ```bash
   # Configure fail2ban for SSH
   sudo cp /etc/fail2ban/jail.conf /etc/fail2ban/jail.local
   
   # Edit jail.local to enable SSH protection
   sudo vim /etc/fail2ban/jail.local
   # Find [sshd] section and ensure enabled = true
   
   # Restart fail2ban
   sudo systemctl restart fail2ban
   sudo systemctl enable fail2ban
   ```

### Verification
```bash
# Verify Docker installation
docker --version
docker compose version

# Verify firewall is active
sudo ufw status

# Verify fail2ban is running
sudo systemctl status fail2ban

# Test Docker
docker run hello-world
```

### Before Moving On
- ✅ Docker and Docker Compose are installed and working
- ✅ Firewall is configured and active
- ✅ Fail2ban is protecting SSH
- ✅ System is fully updated

---

## STEP 3: Deploy Application Code

### Role
You are a software engineer deploying a FastAPI + Neo4j application to production. You need to transfer code, set up the directory structure, and prepare for configuration.

### Context
The application code needs to be cloned from Git repository or transferred from local machine. The directory structure should be organized for production use.

### Tasks

1. **Create Application Directory Structure**
   ```bash
   # Create application directory
   sudo mkdir -p /opt/rico-graph
   sudo chown $USER:$USER /opt/rico-graph
   cd /opt/rico-graph
   
   # Create necessary subdirectories
   mkdir -p logs neo4j/data neo4j/logs backups
   ```

2. **Transfer Application Code**
   
   **Option A: Clone from Git Repository**
   ```bash
   # If using Git (recommended)
   cd /opt/rico-graph
   git clone https://github.com/your-username/rico-graph.git .
   # Or if already exists
   git pull origin main
   ```
   
   **Option B: Transfer from Local Machine**
   ```bash
   # From your LOCAL machine, not VPS
   # Create archive excluding unnecessary files
   cd ~/Developer/nda.tools/rico-graph
   tar -czf rico-graph.tar.gz \
     --exclude='*.pyc' \
     --exclude='__pycache__' \
     --exclude='venv' \
     --exclude='.git' \
     --exclude='neo4j/data/*' \
     --exclude='*.log' \
     .
   
   # Transfer to VPS
   scp rico-graph.tar.gz your_user@your_vps_ip:/tmp/
   
   # On VPS, extract
   cd /opt/rico-graph
   tar -xzf /tmp/rico-graph.tar.gz
   rm /tmp/rico-graph.tar.gz
   ```

3. **Set Proper Permissions**
   ```bash
   # Set ownership
   sudo chown -R $USER:$USER /opt/rico-graph
   
   # Set directory permissions
   find /opt/rico-graph -type d -exec chmod 755 {} \;
   
   # Set file permissions
   find /opt/rico-graph -type f -exec chmod 644 {} \;
   
   # Make scripts executable
   chmod +x /opt/rico-graph/*.sh
   chmod +x /opt/rico-graph/api/*.sh 2>/dev/null || true
   ```

4. **Verify File Structure**
   ```bash
   # Check critical files exist
   ls -la /opt/rico-graph/
   ls -la /opt/rico-graph/api/
   ls -la /opt/rico-graph/docker-compose.yml
   
   # Verify new model exists
   ls -la /opt/rico-graph/api/models/ingest_request.py
   ```

### Verification
```bash
# Check directory structure
tree -L 2 /opt/rico-graph/ || ls -la /opt/rico-graph/

# Verify critical files
test -f /opt/rico-graph/docker-compose.yml && echo "✅ docker-compose.yml exists"
test -f /opt/rico-graph/api/main.py && echo "✅ API main.py exists"
test -f /opt/rico-graph/api/models/ingest_request.py && echo "✅ New ingest model exists"
```

### Before Moving On
- ✅ Application code is in /opt/rico-graph
- ✅ Directory structure is correct
- ✅ Permissions are properly set
- ✅ New ingest_request.py model file is present

---

## STEP 4: Configure Production Environment

### Role
You are a security-conscious DevOps engineer setting up production environment variables. You need to create secure credentials and configure the application for production use.

### Context
Production requires different credentials than development. API keys should be strong, database passwords secure, and sensitive data properly protected.

### Tasks

1. **Generate Secure Credentials**
   ```bash
   # Generate secure API key
   API_KEY=$(openssl rand -hex 32)
   echo "Generated API_KEY: $API_KEY"
   
   # Generate secure Neo4j password
   NEO4J_PASSWORD=$(openssl rand -base64 24)
   echo "Generated NEO4J_PASSWORD: $NEO4J_PASSWORD"
   
   # Save these securely - you'll need them!
   ```

2. **Create Production .env File**
   ```bash
   cd /opt/rico-graph
   
   # Create .env file with production values
   cat > .env << EOF
   # Neo4j Configuration
   NEO4J_URI=bolt://neo4j:7687
   NEO4J_USER=neo4j
   NEO4J_PASSWORD=${NEO4J_PASSWORD}
   
   # API Configuration
   API_KEY=${API_KEY}
   
   # SearchCarriers API (use your actual token)
   SEARCH_CARRIERS_API_TOKEN=your_actual_searchcarriers_token_here
   
   # Production Settings
   ENVIRONMENT=production
   LOG_LEVEL=INFO
   EOF
   
   # Secure the .env file
   chmod 600 .env
   ```

3. **Create Docker Override for Production**
   ```bash
   # Create docker-compose.prod.yml for production overrides
   cat > docker-compose.prod.yml << 'EOF'
   version: '3.8'
   
   services:
     neo4j:
       restart: always
       environment:
         - NEO4J_server_memory_heap_initial__size=1G
         - NEO4J_server_memory_heap_max__size=2G
         - NEO4J_dbms_memory_transaction_total_max=500m
       volumes:
         - /opt/rico-graph/neo4j/data:/data
         - /opt/rico-graph/neo4j/logs:/logs
   
     api:
       restart: always
       environment:
         - ENVIRONMENT=production
         - LOG_LEVEL=INFO
       volumes:
         - /opt/rico-graph/api:/app:ro
         - /opt/rico-graph/logs:/app/logs
   EOF
   ```

4. **Update Nginx/Caddy Configuration (for HTTPS)**
   ```bash
   # Install Nginx
   sudo apt install -y nginx certbot python3-certbot-nginx
   
   # Create Nginx configuration
   sudo tee /etc/nginx/sites-available/rico-api << 'EOF'
   server {
       listen 80;
       server_name api.yourdomain.com;  # Replace with your domain
       
       location / {
           proxy_pass http://localhost:8000;
           proxy_http_version 1.1;
           proxy_set_header Upgrade $http_upgrade;
           proxy_set_header Connection 'upgrade';
           proxy_set_header Host $host;
           proxy_cache_bypass $http_upgrade;
           proxy_set_header X-Real-IP $remote_addr;
           proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
           proxy_set_header X-Forwarded-Proto $scheme;
           
           # Security headers
           add_header X-Content-Type-Options nosniff;
           add_header X-Frame-Options DENY;
           add_header X-XSS-Protection "1; mode=block";
           
           # Rate limiting
           limit_req zone=api burst=10 nodelay;
       }
   }
   EOF
   
   # Add rate limiting configuration
   sudo tee -a /etc/nginx/nginx.conf << 'EOF'
   http {
       # ... existing config ...
       
       # Rate limiting
       limit_req_zone $binary_remote_addr zone=api:10m rate=10r/s;
       limit_req_status 429;
   }
   EOF
   
   # Enable site
   sudo ln -s /etc/nginx/sites-available/rico-api /etc/nginx/sites-enabled/
   sudo nginx -t
   sudo systemctl reload nginx
   ```

### Verification
```bash
# Verify .env file exists and is secure
ls -la /opt/rico-graph/.env
# Should show: -rw------- (600 permissions)

# Verify environment variables are set
source /opt/rico-graph/.env
echo "API_KEY is set: $([ ! -z "$API_KEY" ] && echo "Yes" || echo "No")"
echo "NEO4J_PASSWORD is set: $([ ! -z "$NEO4J_PASSWORD" ] && echo "Yes" || echo "No")"

# Test Nginx configuration
sudo nginx -t
```

### Before Moving On
- ✅ Strong production credentials generated
- ✅ .env file created with 600 permissions
- ✅ Docker production overrides configured
- ✅ Nginx configured (if using domain)
- ⚠️ **SAVE YOUR CREDENTIALS SECURELY**

---

## STEP 5: Build and Deploy with Docker

### Role
You are a DevOps engineer deploying the containerized application. You need to build images, start services, and ensure everything is running correctly.

### Context
Docker Compose will orchestrate both Neo4j and the API. Production settings should be applied, and services should auto-restart on failure.

### Tasks

1. **Stop Any Existing Services**
   ```bash
   cd /opt/rico-graph
   
   # Stop any existing containers
   docker compose down
   
   # Remove old containers and volumes (if doing fresh install)
   # WARNING: This will delete data!
   # docker compose down -v
   ```

2. **Build and Start Services**
   ```bash
   # Load environment variables
   source .env
   
   # Build and start with production overrides
   docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
   
   # Watch logs to ensure startup
   docker compose logs -f
   # Press Ctrl+C to exit logs (containers keep running)
   ```

3. **Verify Services Are Running**
   ```bash
   # Check container status
   docker ps
   
   # Should see two containers:
   # - rico-neo4j (healthy)
   # - rico-api (running)
   
   # Check Neo4j health
   docker exec rico-neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" "RETURN 1"
   
   # Check API health
   curl -X GET "http://localhost:8000/docs" -v
   ```

4. **Initialize Database Schema**
   ```bash
   # If you have an initialization script
   if [ -f /opt/rico-graph/init_schema.cypher ]; then
       docker exec rico-neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" < /opt/rico-graph/init_schema.cypher
   fi
   ```

### Verification
```bash
# Test API with authentication
source /opt/rico-graph/.env
curl -X GET "http://localhost:8000/carriers" \
  -H "X-API-Key: $API_KEY" \
  -v

# Should return empty array [] or existing carriers

# Check Docker auto-restart policy
docker inspect rico-api | grep -A 2 RestartPolicy
# Should show "Name": "always"
```

### Before Moving On
- ✅ Docker containers are running
- ✅ Neo4j is healthy and accessible
- ✅ API responds to requests
- ✅ Auto-restart is configured

---

## STEP 6: Configure SSL/HTTPS (If Using Domain)

### Role
You are a security engineer setting up HTTPS encryption for the API. You need to obtain SSL certificates and configure secure connections.

### Context
HTTPS is essential for production APIs. Let's Encrypt provides free SSL certificates. Skip this step if only using IP address internally.

### Tasks

1. **Obtain SSL Certificate**
   ```bash
   # Replace api.yourdomain.com with your actual domain
   sudo certbot --nginx -d api.yourdomain.com
   
   # Follow prompts:
   # - Enter email
   # - Agree to terms
   # - Choose whether to redirect HTTP to HTTPS (recommend: yes)
   ```

2. **Configure Auto-Renewal**
   ```bash
   # Test renewal
   sudo certbot renew --dry-run
   
   # Add cron job for auto-renewal
   echo "0 0,12 * * * root python3 -c 'import random; import time; time.sleep(random.random() * 3600)' && certbot renew -q" | sudo tee -a /etc/crontab > /dev/null
   ```

3. **Update Firewall for HTTPS**
   ```bash
   # Already done in Step 2, but verify
   sudo ufw status
   # Should show 80/tcp and 443/tcp ALLOW
   ```

### Verification
```bash
# Test HTTPS endpoint (replace with your domain)
curl -X GET "https://api.yourdomain.com/docs" -v

# Check certificate
echo | openssl s_client -connect api.yourdomain.com:443 2>/dev/null | openssl x509 -noout -dates
```

### Before Moving On
- ✅ SSL certificate obtained (if using domain)
- ✅ HTTPS is working
- ✅ Auto-renewal is configured
- ✅ HTTP redirects to HTTPS

---

## STEP 7: Test Core Functionality

### Role
You are a QA engineer validating that all API endpoints work correctly in production, including the new JSON ingestion with enrichment.

### Context
Need to verify that all critical functions work: CRUD operations, JSON ingestion, and SearchCarriers enrichment.

### Tasks

1. **Test Basic API Operations**
   ```bash
   source /opt/rico-graph/.env
   
   # Test API is responding
   curl -X GET "http://localhost:8000/docs"
   
   # Test authenticated endpoint
   curl -X GET "http://localhost:8000/carriers" \
     -H "X-API-Key: $API_KEY"
   ```

2. **Test JSON Ingestion (Without Enrichment)**
   ```bash
   # Create test CSV data
   TEST_CSV=$(echo -n "dot_number,JB Carrier,Carrier,Primary Officer, Insurance,Amount, Trucks
   9999901,Yes,Test Prod Carrier One,John Test,Test Insurance,$1 Million,10
   9999902,Yes,Test Prod Carrier Two,Jane Test,Test Insurance,$500k,5" | base64 -w 0)
   
   # Test ingestion without enrichment
   curl -X POST "http://localhost:8000/ingest/" \
     -H "Content-Type: application/json" \
     -H "X-API-Key: $API_KEY" \
     -d "{
       \"csv_content\": \"$TEST_CSV\",
       \"target_company\": \"JB_HUNT\",
       \"enable_enrichment\": false,
       \"skip_invalid\": true
     }"
   
   # Should return status: "completed"
   ```

3. **Test JSON Ingestion (With Enrichment)**
   ```bash
   # Test with real carrier for enrichment
   TEST_CSV_ENRICHMENT=$(echo -n "dot_number,JB Carrier,Carrier,Primary Officer, Insurance,Amount, Trucks
   190979,Yes,SCHNEIDER NATIONAL CARRIERS INC,Mark Rourke,Unknown,$1 Million,9000" | base64 -w 0)
   
   # Test ingestion with enrichment
   curl -X POST "http://localhost:8000/ingest/" \
     -H "Content-Type: application/json" \
     -H "X-API-Key: $API_KEY" \
     -d "{
       \"csv_content\": \"$TEST_CSV_ENRICHMENT\",
       \"target_company\": \"JB_HUNT\",
       \"enable_enrichment\": true,
       \"skip_invalid\": true
     }"
   
   # Should return status: "processing" with job_id
   ```

4. **Monitor Enrichment Logs**
   ```bash
   # Watch logs for enrichment activity
   docker logs rico-api -f | grep -E "(SearchCarriers|enrichment|insurance)"
   
   # Should see API calls to SearchCarriers
   ```

5. **Verify Data Was Created**
   ```bash
   # Check carriers were created
   curl -X GET "http://localhost:8000/carriers?limit=10" \
     -H "X-API-Key: $API_KEY" | python3 -m json.tool
   
   # Check specific carrier
   curl -X GET "http://localhost:8000/carriers/9999901" \
     -H "X-API-Key: $API_KEY" | python3 -m json.tool
   ```

### Verification
- ✅ API responds to health checks
- ✅ Authentication with API key works
- ✅ JSON ingestion without enrichment completes synchronously
- ✅ JSON ingestion with enrichment returns "processing" status
- ✅ SearchCarriers API is being called (check logs)
- ✅ Data is persisted in Neo4j

### Before Moving On
All core functionality should be working. If enrichment fails, check:
- SEARCH_CARRIERS_API_TOKEN is set correctly
- Network connectivity to searchcarriers.com
- Docker logs for specific errors

---

## STEP 8: Setup Monitoring and Logging

### Role
You are a DevOps engineer implementing basic monitoring and logging to ensure system reliability and debuggability.

### Context
Production systems need proper logging and basic monitoring. We'll set up log rotation and basic health monitoring.

### Tasks

1. **Configure Log Rotation**
   ```bash
   # Create logrotate configuration for API logs
   sudo tee /etc/logrotate.d/rico-api << 'EOF'
   /opt/rico-graph/logs/*.log {
       daily
       rotate 30
       compress
       delaycompress
       notifempty
       create 644 root root
       sharedscripts
       postrotate
           docker exec rico-api kill -USR1 1
       endscript
   }
   EOF
   
   # Test logrotate configuration
   sudo logrotate -d /etc/logrotate.d/rico-api
   ```

2. **Create Health Check Script**
   ```bash
   # Create monitoring script
   cat > /opt/rico-graph/health_check.sh << 'EOF'
   #!/bin/bash
   
   # Load environment
   source /opt/rico-graph/.env
   
   # Check API health
   API_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -H "X-API-Key: $API_KEY" http://localhost:8000/carriers)
   
   # Check Neo4j health
   NEO4J_STATUS=$(docker exec rico-neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" "RETURN 1" 2>&1)
   
   if [ "$API_STATUS" != "200" ]; then
       echo "$(date): API health check failed with status $API_STATUS"
       # Could add alerting here (email, Slack, etc.)
       docker restart rico-api
   fi
   
   if [[ "$NEO4J_STATUS" != *"1"* ]]; then
       echo "$(date): Neo4j health check failed"
       docker restart rico-neo4j
   fi
   EOF
   
   chmod +x /opt/rico-graph/health_check.sh
   
   # Add to crontab for regular checks
   (crontab -l 2>/dev/null; echo "*/5 * * * * /opt/rico-graph/health_check.sh >> /opt/rico-graph/logs/health.log 2>&1") | crontab -
   ```

3. **Setup Basic Docker Monitoring**
   ```bash
   # Create disk usage monitoring
   cat > /opt/rico-graph/monitor_disk.sh << 'EOF'
   #!/bin/bash
   
   THRESHOLD=80
   USAGE=$(df /opt/rico-graph | grep -v Filesystem | awk '{print $5}' | sed 's/%//')
   
   if [ "$USAGE" -gt "$THRESHOLD" ]; then
       echo "$(date): WARNING - Disk usage is at ${USAGE}%"
       # Add alerting here
   fi
   EOF
   
   chmod +x /opt/rico-graph/monitor_disk.sh
   
   # Add to daily cron
   (crontab -l 2>/dev/null; echo "0 6 * * * /opt/rico-graph/monitor_disk.sh >> /opt/rico-graph/logs/disk.log 2>&1") | crontab -
   ```

4. **Configure Docker Logging**
   ```bash
   # Check Docker logs size
   docker ps -q | xargs docker inspect --format='{{.Name}}: {{.HostConfig.LogConfig}}'
   
   # Limit log size in docker-compose.prod.yml (already included)
   # Add under each service:
   # logging:
   #   driver: "json-file"
   #   options:
   #     max-size: "10m"
   #     max-file: "3"
   ```

### Verification
```bash
# Check cron jobs are installed
crontab -l

# Test health check script
/opt/rico-graph/health_check.sh

# Check logs directory
ls -la /opt/rico-graph/logs/

# Monitor Docker resource usage
docker stats --no-stream
```

### Before Moving On
- ✅ Log rotation configured
- ✅ Health checks running every 5 minutes
- ✅ Disk monitoring in place
- ✅ Docker logs limited in size

---

## STEP 9: Setup Backup Strategy

### Role
You are a database administrator implementing a backup strategy to prevent data loss and enable disaster recovery.

### Context
Regular backups of Neo4j data are critical. We'll implement daily automated backups with retention policy.

### Tasks

1. **Create Backup Script**
   ```bash
   cat > /opt/rico-graph/backup.sh << 'EOF'
   #!/bin/bash
   
   # Configuration
   BACKUP_DIR="/opt/rico-graph/backups"
   DATE=$(date +%Y%m%d_%H%M%S)
   BACKUP_FILE="neo4j_backup_${DATE}.dump"
   RETENTION_DAYS=7
   
   # Load environment
   source /opt/rico-graph/.env
   
   echo "$(date): Starting backup..."
   
   # Create backup
   docker exec rico-neo4j neo4j-admin database dump --to-path=/data --overwrite-destination neo4j
   
   # Copy backup to backup directory
   docker cp rico-neo4j:/data/neo4j.dump ${BACKUP_DIR}/${BACKUP_FILE}
   
   # Compress backup
   gzip ${BACKUP_DIR}/${BACKUP_FILE}
   
   # Remove old backups
   find ${BACKUP_DIR} -name "neo4j_backup_*.dump.gz" -mtime +${RETENTION_DAYS} -delete
   
   echo "$(date): Backup completed: ${BACKUP_FILE}.gz"
   
   # Optional: Copy to remote storage
   # aws s3 cp ${BACKUP_DIR}/${BACKUP_FILE}.gz s3://your-bucket/backups/
   # or
   # rsync -avz ${BACKUP_DIR}/${BACKUP_FILE}.gz user@backup-server:/path/to/backups/
   EOF
   
   chmod +x /opt/rico-graph/backup.sh
   ```

2. **Schedule Daily Backups**
   ```bash
   # Add to crontab (runs at 2 AM daily)
   (crontab -l 2>/dev/null; echo "0 2 * * * /opt/rico-graph/backup.sh >> /opt/rico-graph/logs/backup.log 2>&1") | crontab -
   ```

3. **Create Restore Script**
   ```bash
   cat > /opt/rico-graph/restore.sh << 'EOF'
   #!/bin/bash
   
   # Usage: ./restore.sh backup_file.dump.gz
   
   if [ $# -eq 0 ]; then
       echo "Usage: $0 <backup_file.dump.gz>"
       exit 1
   fi
   
   BACKUP_FILE=$1
   
   # Load environment
   source /opt/rico-graph/.env
   
   echo "WARNING: This will replace all current data!"
   read -p "Are you sure? (yes/no): " confirm
   
   if [ "$confirm" != "yes" ]; then
       echo "Restore cancelled"
       exit 1
   fi
   
   # Decompress if needed
   if [[ $BACKUP_FILE == *.gz ]]; then
       gunzip -c $BACKUP_FILE > /tmp/restore.dump
       RESTORE_FILE="/tmp/restore.dump"
   else
       RESTORE_FILE=$BACKUP_FILE
   fi
   
   # Stop API to prevent connections
   docker stop rico-api
   
   # Copy dump to container
   docker cp $RESTORE_FILE rico-neo4j:/data/restore.dump
   
   # Restore database
   docker exec rico-neo4j neo4j-admin database load --from-path=/data --overwrite-destination neo4j restore.dump
   
   # Restart services
   docker restart rico-neo4j
   sleep 10
   docker start rico-api
   
   echo "Restore completed"
   EOF
   
   chmod +x /opt/rico-graph/restore.sh
   ```

4. **Test Backup and Restore**
   ```bash
   # Run manual backup
   /opt/rico-graph/backup.sh
   
   # Verify backup was created
   ls -lh /opt/rico-graph/backups/
   
   # Test restore (optional - will replace current data!)
   # /opt/rico-graph/restore.sh /opt/rico-graph/backups/neo4j_backup_[date].dump.gz
   ```

### Verification
```bash
# Check backup exists
ls -lh /opt/rico-graph/backups/*.dump.gz

# Verify cron job
crontab -l | grep backup

# Check backup log
tail /opt/rico-graph/logs/backup.log
```

### Before Moving On
- ✅ Backup script created and tested
- ✅ Daily backups scheduled
- ✅ Restore script available
- ✅ Retention policy in place (7 days)

---

## STEP 10: Web Application Integration

### Role
You are a full-stack developer integrating your web application with the production API. You need to configure secure communication and handle API responses properly.

### Context
Your web application needs to communicate securely with the API using HTTPS and proper authentication. Error handling and retry logic are important for reliability.

### Tasks

1. **Configure Web Application Environment**
   ```javascript
   // In your web application's environment configuration
   // (.env.production or similar)
   
   // If using domain with HTTPS
   RICO_API_URL=https://api.yourdomain.com
   RICO_API_KEY=your_production_api_key_here
   
   // If using IP (internal network only)
   RICO_API_URL=http://your.vps.ip:8000
   RICO_API_KEY=your_production_api_key_here
   
   // Never expose API key in client-side code!
   ```

2. **Create API Client (Example)**
   ```javascript
   // Example API client for your web app (Node.js/Express)
   
   const axios = require('axios');
   
   class RicoAPIClient {
     constructor() {
       this.baseURL = process.env.RICO_API_URL;
       this.apiKey = process.env.RICO_API_KEY;
       
       this.client = axios.create({
         baseURL: this.baseURL,
         timeout: 30000,
         headers: {
           'X-API-Key': this.apiKey,
           'Content-Type': 'application/json'
         }
       });
       
       // Add retry logic
       this.client.interceptors.response.use(
         response => response,
         async error => {
           if (error.code === 'ECONNABORTED' || error.response?.status >= 500) {
             // Retry once after 2 seconds
             await new Promise(resolve => setTimeout(resolve, 2000));
             return this.client.request(error.config);
           }
           return Promise.reject(error);
         }
       );
     }
     
     async ingestCarriers(csvContent, enableEnrichment = false) {
       try {
         const response = await this.client.post('/ingest/', {
           csv_content: Buffer.from(csvContent).toString('base64'),
           target_company: 'JB_HUNT',
           enable_enrichment: enableEnrichment,
           skip_invalid: true
         });
         
         return response.data;
       } catch (error) {
         console.error('Ingestion failed:', error.response?.data || error.message);
         throw error;
       }
     }
     
     async getCarriers(limit = 100) {
       try {
         const response = await this.client.get('/carriers', {
           params: { limit }
         });
         return response.data;
       } catch (error) {
         console.error('Failed to fetch carriers:', error);
         throw error;
       }
     }
   }
   
   module.exports = RicoAPIClient;
   ```

3. **Test Connection from Web App**
   ```bash
   # From your web application server
   # Test API connectivity
   curl -X GET "https://api.yourdomain.com/carriers" \
     -H "X-API-Key: your_production_api_key" \
     -v
   
   # Test from application code
   node -e "
   const axios = require('axios');
   axios.get('https://api.yourdomain.com/carriers', {
     headers: {'X-API-Key': 'your_production_api_key'}
   })
   .then(res => console.log('Success:', res.data))
   .catch(err => console.error('Error:', err.message));
   "
   ```

4. **Implement Error Handling**
   ```javascript
   // In your web application
   
   async function handleCarrierUpload(csvFile) {
     try {
       // Show loading state
       setLoading(true);
       
       const result = await apiClient.ingestCarriers(csvFile, true);
       
       if (result.status === 'processing') {
         // Handle async processing
         showNotification('Upload started. Processing in background...');
         // Could poll for status or use webhooks
       } else if (result.status === 'completed') {
         showNotification('Upload completed successfully');
         // Refresh data
         await refreshCarrierList();
       }
     } catch (error) {
       if (error.response?.status === 401) {
         showError('Authentication failed. Please check API key.');
       } else if (error.response?.status === 429) {
         showError('Rate limit exceeded. Please try again later.');
       } else {
         showError('Upload failed. Please try again.');
       }
     } finally {
       setLoading(false);
     }
   }
   ```

### Verification
From your web application:
- ✅ Can connect to API successfully
- ✅ Authentication with API key works
- ✅ Can upload CSV data via JSON
- ✅ Enrichment triggers when enabled
- ✅ Error handling works properly

### Before Moving On
Test all integration points between your web app and the API. Ensure proper error handling and user feedback.

---

## STEP 11: Go-Live Checklist

### Role
You are the project lead performing final verification before declaring the system production-ready.

### Context
All components should be working. This is the final checklist to ensure everything is secure, stable, and ready for production use.

### Tasks

1. **Security Checklist**
   ```bash
   # ✅ Firewall is active
   sudo ufw status
   
   # ✅ Fail2ban is protecting SSH
   sudo systemctl status fail2ban
   
   # ✅ SSL certificate is valid (if using domain)
   echo | openssl s_client -connect api.yourdomain.com:443 2>/dev/null | openssl x509 -noout -dates
   
   # ✅ API requires authentication
   curl -X GET "http://localhost:8000/carriers" -v
   # Should return 401 Unauthorized
   
   # ✅ Sensitive files are protected
   ls -la /opt/rico-graph/.env
   # Should show 600 permissions
   
   # ✅ Default passwords changed
   # Neo4j password is not default
   # API key is strong and unique
   ```

2. **Functionality Checklist**
   ```bash
   source /opt/rico-graph/.env
   
   # ✅ API is responding
   curl -X GET "http://localhost:8000/docs"
   
   # ✅ Database queries work
   curl -X GET "http://localhost:8000/carriers" \
     -H "X-API-Key: $API_KEY"
   
   # ✅ JSON ingestion works
   # Test with small dataset
   
   # ✅ Enrichment triggers
   # Check logs for SearchCarriers API calls
   
   # ✅ Web app can connect
   # Test from web application
   ```

3. **Reliability Checklist**
   ```bash
   # ✅ Auto-restart is configured
   docker inspect rico-api | grep -A 2 RestartPolicy
   
   # ✅ Health checks are running
   crontab -l | grep health_check
   
   # ✅ Backups are scheduled
   crontab -l | grep backup
   
   # ✅ Logs are rotating
   sudo logrotate -d /etc/logrotate.d/rico-api
   
   # ✅ Disk space is adequate
   df -h /opt/rico-graph
   ```

4. **Performance Check**
   ```bash
   # Check resource usage
   docker stats --no-stream
   
   # Check response times
   time curl -X GET "http://localhost:8000/carriers" \
     -H "X-API-Key: $API_KEY" \
     -o /dev/null -s
   
   # Monitor during load
   # Run sample ingestion and watch resources
   ```

5. **Documentation Checklist**
   - ✅ API endpoint documented (URL, authentication)
   - ✅ Credentials stored securely (password manager)
   - ✅ Backup/restore procedures documented
   - ✅ Monitoring/alerting documented
   - ✅ Integration examples provided to web app team

### Final Verification Commands
```bash
# Complete system check
echo "=== RICO Graph API System Status ==="
echo "Date: $(date)"
echo ""
echo "Services:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""
echo "API Health:"
curl -s -o /dev/null -w "HTTP Status: %{http_code}\n" -H "X-API-Key: $API_KEY" http://localhost:8000/carriers
echo ""
echo "Disk Usage:"
df -h /opt/rico-graph | grep -v Filesystem
echo ""
echo "Recent Logs:"
docker logs rico-api --tail 5
echo ""
echo "==================================="
```

### Production Ready Criteria
All items should be checked:
- ✅ **Security**: Firewall, SSL, authentication, secure credentials
- ✅ **Functionality**: All endpoints work, enrichment triggers
- ✅ **Reliability**: Auto-restart, health checks, backups
- ✅ **Monitoring**: Logs, health checks, disk monitoring
- ✅ **Documentation**: All procedures documented
- ✅ **Integration**: Web app successfully connected

---

## Post-Deployment

### Monitoring Period
- Monitor closely for first 48 hours
- Check logs daily for first week
- Review resource usage trends

### Maintenance Schedule
- **Daily**: Check logs for errors
- **Weekly**: Review disk usage and performance
- **Monthly**: Test backup restoration
- **Quarterly**: Update dependencies and security patches

### Emergency Procedures

**If API is down:**
```bash
docker ps
docker restart rico-api
docker logs rico-api --tail 50
```

**If database is corrupted:**
```bash
# Stop services
docker stop rico-api
# Restore from backup
/opt/rico-graph/restore.sh /opt/rico-graph/backups/[latest_backup].dump.gz
```

**If under attack:**
```bash
# Check access logs
tail -f /var/log/nginx/access.log
# Temporarily block IP
sudo ufw deny from [attacking_ip]
# Check fail2ban
sudo fail2ban-client status sshd
```

---

## Conclusion

Your RICO Graph API is now production-ready! The system is:
- **Secure**: Protected by firewall, SSL, and authentication
- **Reliable**: Auto-restart, health monitoring, and backups
- **Performant**: Optimized for production workloads
- **Maintainable**: Proper logging and monitoring

Remember to:
1. Keep credentials secure
2. Monitor logs regularly
3. Test backups periodically
4. Apply security updates promptly

For support or issues, refer to the documentation and logs first. Most issues can be diagnosed from Docker logs and system monitoring.

Good luck with your production deployment!