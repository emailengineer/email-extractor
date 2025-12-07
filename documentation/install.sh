#!/bin/bash
# Email Extraction System - One-Line Installer
# Usage: curl -sSL https://raw.githubusercontent.com/emailengineer/email-extractor/main/documentation/install.sh | bash

set -e

echo "=========================================="
echo "Email Extraction System Installer v1.0"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if running as root
if [ "$EUID" -eq 0 ]; then 
   echo -e "${RED}ERROR: Please do not run as root/sudo${NC}"
   echo "Run as regular user: bash install.sh"
   exit 1
fi

# Check Ubuntu version
if [ ! -f /etc/os-release ]; then
    echo -e "${RED}ERROR: Cannot detect operating system${NC}"
    exit 1
fi

. /etc/os-release
if [[ "$ID" != "ubuntu" ]]; then
    echo -e "${RED}ERROR: This installer is for Ubuntu only${NC}"
    echo "Detected: $ID $VERSION_ID"
    exit 1
fi

echo -e "${GREEN}✓${NC} Detected: Ubuntu $VERSION_ID"

# Update system
echo ""
echo "Updating system packages..."
sudo apt-get update -qq > /dev/null 2>&1
echo -e "${GREEN}✓${NC} System updated"

# Install prerequisites
echo ""
echo "Installing prerequisites..."
sudo apt-get install -y -qq curl git openssl > /dev/null 2>&1
echo -e "${GREEN}✓${NC} Prerequisites installed"

# Install Docker if not present
if ! command -v docker &> /dev/null; then
    echo ""
    echo "Installing Docker (this may take a few minutes)..."
    curl -fsSL https://get.docker.com -o /tmp/get-docker.sh
    sudo sh /tmp/get-docker.sh > /dev/null 2>&1
    sudo usermod -aG docker $USER
    rm /tmp/get-docker.sh
    echo -e "${GREEN}✓${NC} Docker installed"
else
    echo -e "${GREEN}✓${NC} Docker already installed ($(docker --version))"
fi

# Install Docker Compose if not present
if ! command -v docker-compose &> /dev/null; then
    echo ""
    echo "Installing Docker Compose..."
    DOCKER_COMPOSE_VERSION=$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep 'tag_name' | cut -d\" -f4)
    sudo curl -L "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" \
        -o /usr/local/bin/docker-compose > /dev/null 2>&1
    sudo chmod +x /usr/local/bin/docker-compose
    echo -e "${GREEN}✓${NC} Docker Compose installed"
else
    echo -e "${GREEN}✓${NC} Docker Compose already installed ($(docker-compose --version))"
fi

# Create installation directory
INSTALL_DIR="$HOME/email-extractor"
echo ""
echo "Installation directory: $INSTALL_DIR"

if [ -d "$INSTALL_DIR" ]; then
    echo -e "${YELLOW}WARNING: Directory exists${NC}"
    BACKUP_DIR="$INSTALL_DIR.backup.$(date +%Y%m%d_%H%M%S)"
    echo "Creating backup: $BACKUP_DIR"
    mv "$INSTALL_DIR" "$BACKUP_DIR"
    echo -e "${GREEN}✓${NC} Backup created"
fi

mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR"

# Detect GitHub username from script source
GITHUB_USERNAME="emailengineer"


# Clone repository
echo ""
echo "Downloading application from GitHub..."
echo "Repository: https://github.com/emailengineer/email-extractor"

if git clone "https://github.com/emailengineer/email-extractor.git" . > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Application downloaded"
else
    echo -e "${RED}ERROR: Failed to clone repository${NC}"
    echo "Please check if GitHub is accessible from this server"
    exit 1
fi


# Generate random secure passwords
echo ""
echo "Generating secure passwords..."
MYSQL_ROOT_PASS=$(openssl rand -base64 32 | tr -d "=+/" | cut -c1-25)
MYSQL_PASS=$(openssl rand -base64 32 | tr -d "=+/" | cut -c1-25)
echo -e "${GREEN}✓${NC} Passwords generated"

# Create logs directory
mkdir -p logs

# Change to script directory where docker-compose.yml is located
if [ ! -d "script" ]; then
    echo -e "${RED}ERROR: script directory not found${NC}"
    exit 1
fi

# Create .env file in script directory (where docker-compose runs)
cat > script/.env <<EOF
# Auto-generated on $(date)
# DO NOT COMMIT THIS FILE

# Database Configuration
MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASS
MYSQL_PASSWORD=$MYSQL_PASS

# Application Settings
MAX_CONCURRENT=1000
MAX_DEPTH=3
TIMEOUT=30
LOG_LEVEL=INFO

# Database Connection (for application)
DB_HOST=mysql
DB_PORT=3306
DB_USER=email_extractor
DB_NAME=email_extraction
EOF

chmod 600 script/.env
echo -e "${GREEN}✓${NC} Configuration created"

cd script

# Start services
echo ""
echo -e "${BLUE}Starting Docker services (this will take 2-5 minutes)...${NC}"
echo "Downloading images and starting containers..."

if docker-compose up -d > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Services started"
else
    echo -e "${RED}ERROR: Failed to start services${NC}"
    echo "Trying with verbose output..."
    docker-compose up -d
    exit 1
fi

# Wait for services to initialize
echo ""
echo "Waiting for services to initialize..."
for i in {1..30}; do
    printf "."
    sleep 1
done
echo ""

# Check if services are running
echo ""
echo "Checking service status..."
if docker-compose ps | grep -q "Up"; then
    echo -e "${GREEN}✓${NC} All services are running"
else
    echo -e "${YELLOW}WARNING: Some services may not be running${NC}"
    echo "Run 'docker-compose ps' to check status"
fi

# Wait for MySQL to be ready
echo ""
echo "Waiting for database to be ready..."
MYSQL_READY=false
for i in {1..60}; do
    if docker-compose exec -T mysql mysqladmin ping -h localhost -u root -p"$MYSQL_ROOT_PASS" &> /dev/null; then
        MYSQL_READY=true
        echo -e "${GREEN}✓${NC} Database is ready"
        break
    fi
    sleep 2
    printf "."
done
echo ""

if [ "$MYSQL_READY" = false ]; then
    echo -e "${YELLOW}WARNING: Database took longer than expected to start${NC}"
    echo "It may still be initializing. Check with: docker-compose logs mysql"
fi

# Test API
echo ""
echo "Testing API connection..."
sleep 5

API_ATTEMPTS=0
API_READY=false

while [ $API_ATTEMPTS -lt 10 ]; do
    if curl -s http://localhost:8000/ | grep -q "running" 2>/dev/null; then
        API_READY=true
        echo -e "${GREEN}✓${NC} API is responding"
        break
    fi
    sleep 3
    API_ATTEMPTS=$((API_ATTEMPTS + 1))
    printf "."
done
echo ""

if [ "$API_READY" = false ]; then
    echo -e "${YELLOW}WARNING: API is not responding yet${NC}"
    echo "It may still be starting. Check with: docker-compose logs api"
fi

# Create management script
cd "$INSTALL_DIR"
cat > manage.sh <<'MANAGE_EOF'
#!/bin/bash
# Easy management script for Email Extractor

cd "$(dirname "$0")/script"

case "$1" in
    start)
        echo "Starting services..."
        docker-compose start
        echo "✓ Services started"
        ;;
    stop)
        echo "Stopping services..."
        docker-compose stop
        echo "✓ Services stopped"
        ;;
    restart)
        echo "Restarting services..."
        docker-compose restart
        echo "✓ Services restarted"
        ;;
    status)
        docker-compose ps
        ;;
    logs)
        SERVICE="${2:-api}"
        echo "Showing logs for: $SERVICE (Ctrl+C to exit)"
        docker-compose logs -f "$SERVICE"
        ;;
    update)
        echo "Updating to latest version..."
        cd "$(dirname "$0")"
        git pull
        cd script
        docker-compose pull
        docker-compose up -d --build
        echo "✓ Updated successfully"
        ;;
    test)
        echo "Testing API..."
        curl -s http://localhost:8000/ | jq . || curl -s http://localhost:8000/
        ;;
    *)
        echo "Email Extractor Management"
        echo ""
        echo "Usage: ./manage.sh {command}"
        echo ""
        echo "Commands:"
        echo "  start    - Start all services"
        echo "  stop     - Stop all services"
        echo "  restart  - Restart all services"
        echo "  status   - Show service status"
        echo "  logs     - Show logs (default: api)"
        echo "  update   - Update to latest version"
        echo "  test     - Test API connection"
        echo ""
        echo "Examples:"
        echo "  ./manage.sh status"
        echo "  ./manage.sh logs api"
        echo "  ./manage.sh logs worker"
        exit 1
        ;;
esac
MANAGE_EOF

chmod +x manage.sh
echo -e "${GREEN}✓${NC} Management script created"

# Save credentials (return to install directory)
cd "$INSTALL_DIR"
CREDS_FILE="$INSTALL_DIR/CREDENTIALS.txt"
cat > "$CREDS_FILE" <<EOF
========================================
EMAIL EXTRACTION SYSTEM - CREDENTIALS
========================================
Generated: $(date)
Installation: $INSTALL_DIR

MYSQL CREDENTIALS
-----------------
Root Password: $MYSQL_ROOT_PASS
User: email_extractor
Password: $MYSQL_PASS
Database: email_extraction
Host: localhost
Port: 3306

API ACCESS
----------
URL: http://localhost:8000
Documentation: http://localhost:8000/docs
Interactive Docs: http://localhost:8000/redoc

QUICK START
-----------
# Check status
cd $INSTALL_DIR && ./manage.sh status

# View logs
cd $INSTALL_DIR && ./manage.sh logs

# Navigate to script directory for docker-compose commands
cd $INSTALL_DIR/script

# Test API
curl http://localhost:8000/

# Create search
curl -X POST http://localhost:8000/api/searches \\
  -H "Content-Type: application/json" \\
  -d '{"domains": ["example.com"]}'

DOCKER COMMANDS
---------------
cd $INSTALL_DIR/script

# View all containers
docker-compose ps

# View logs
docker-compose logs -f

# Restart everything
docker-compose restart

# Stop everything
docker-compose stop

# Start everything
docker-compose start

# Rebuild and restart
docker-compose up -d --build

MANAGEMENT SCRIPT
-----------------
cd $INSTALL_DIR

./manage.sh start      # Start services
./manage.sh stop       # Stop services
./manage.sh restart    # Restart services
./manage.sh status     # Check status
./manage.sh logs       # View logs
./manage.sh update     # Update to latest

IMPORTANT NOTES
---------------
1. Keep this file secure - it contains passwords
2. File location: $CREDS_FILE
3. Backup regularly: docker-compose exec mysql mysqldump -u root -p email_extraction > backup.sql

SUPPORT
-------
Documentation: https://github.com/$GITHUB_USERNAME/email-extractor
Issues: https://github.com/$GITHUB_USERNAME/email-extractor/issues

========================================
⚠️  KEEP THIS FILE SECURE AND PRIVATE ⚠️
========================================
EOF

chmod 600 "$CREDS_FILE"
echo -e "${GREEN}✓${NC} Credentials saved"

# Create quick test script
cat > test-api.sh <<'TEST_EOF'
#!/bin/bash
# Quick API test script

echo "Testing Email Extraction API..."
echo ""

# Test 1: Health check
echo "1. Health Check:"
curl -s http://localhost:8000/ | jq . 2>/dev/null || curl -s http://localhost:8000/
echo ""

# Test 2: Create search
echo ""
echo "2. Creating test search..."
RESPONSE=$(curl -s -X POST http://localhost:8000/api/searches \
  -H "Content-Type: application/json" \
  -d '{"batch_name": "API Test", "domains": ["example.com"]}')

echo "$RESPONSE" | jq . 2>/dev/null || echo "$RESPONSE"

SEARCH_ID=$(echo "$RESPONSE" | grep -o '"search_id":[0-9]*' | grep -o '[0-9]*' | head -1)

if [ -n "$SEARCH_ID" ]; then
    echo ""
    echo "✓ Search created with ID: $SEARCH_ID"
    echo ""
    echo "Wait 30 seconds, then check results:"
    echo "  curl http://localhost:8000/api/searches/$SEARCH_ID/emails"
else
    echo "✗ Failed to create search"
fi
TEST_EOF

chmod +x test-api.sh
echo -e "${GREEN}✓${NC} Test script created"

# Print final success message
echo ""
echo "=========================================="
echo -e "${GREEN}✓ INSTALLATION COMPLETE!${NC}"
echo "=========================================="
echo ""
echo -e "${BLUE}📁 Installation Location:${NC}"
echo "   $INSTALL_DIR"
echo ""
echo -e "${BLUE}🔗 API Access:${NC}"
echo "   http://localhost:8000"
echo "   http://localhost:8000/docs (Interactive Documentation)"
echo ""
echo -e "${BLUE}🔐 Credentials:${NC}"
echo "   cat $CREDS_FILE"
echo ""
echo -e "${BLUE}🚀 Quick Start:${NC}"
echo "   cd $INSTALL_DIR"
echo "   ./manage.sh status              # Check if everything is running"
echo "   ./test-api.sh                   # Run a quick test"
echo "   curl http://localhost:8000/     # Test API health"
echo ""
echo -e "${BLUE}📊 Management:${NC}"
echo "   ./manage.sh start|stop|restart|status|logs|update"
echo ""
echo -e "${BLUE}📖 Create Your First Search:${NC}"
echo "   curl -X POST http://localhost:8000/api/searches \\"
echo "     -H 'Content-Type: application/json' \\"
echo "     -d '{\"domains\": [\"example.com\"]}'"
echo ""

# Check if Docker group needs newgrp
if groups | grep -q docker; then
    echo -e "${GREEN}✓${NC} You're in the docker group"
else
    echo -e "${YELLOW}⚠️  You need to log out and back in, or run: newgrp docker${NC}"
fi

echo ""
echo "=========================================="
echo -e "${GREEN}Installation successful! 🎉${NC}"
echo "=========================================="
echo ""
echo "Need help? Check the documentation:"
echo "  https://github.com/$GITHUB_USERNAME/email-extractor"
echo ""
