# Email Extraction System

High-performance, automated email extraction from business domains.

## One-Line Installation

```bash
curl -sSL https://raw.githubusercontent.com/emailengineer/email-extractor/main/documentation/install.sh | bash
```

## Features

- ⚡ Maximum concurrency with unbounded DNS resolution
- 📧 Extracts standard and obfuscated email formats
- ✅ RFC-compliant validation and normalization
- 🔌 RESTful API for batch processing
- 🐳 Docker-based deployment

## Requirements

- Ubuntu 20.04 or later
- 4GB RAM minimum (8GB recommended)
- 20GB disk space
- Docker (auto-installed)

## Quick Start

After installation:
```bash
cd ~/email-extractor

# Check status
./manage.sh status

# View logs
./manage.sh logs

# Test API
curl http://localhost:8000/

# Create a search
curl -X POST http://localhost:8000/api/searches \
  -H "Content-Type: application/json" \
  -d '{"domains": ["example.com"]}'
```

## Management Commands
```bash
./manage.sh start    # Start services
./manage.sh stop     # Stop services
./manage.sh restart  # Restart services
./manage.sh status   # View status
./manage.sh logs     # View logs
./manage.sh update   # Update to latest version
```

## API Endpoints

- `GET /` - Health check
- `POST /api/searches` - Create new search
- `GET /api/searches` - List all searches
- `GET /api/searches/{id}` - Get search details
- `GET /api/searches/{id}/emails` - Get extracted emails
- `GET /api/searches/{id}/statistics` - Get statistics
- `PATCH /api/searches/{id}/pause` - Pause search
- `PATCH /api/searches/{id}/resume` - Resume search
- `DELETE /api/searches/{id}` - Cancel search

## Documentation

- API Docs: http://localhost:8000/docs
- Credentials: `cat ~/email-extractor/CREDENTIALS.txt`

## Support

Issues: https://github.com/emailengineer/email-extractor/issues

## License

Proprietary - Internal Use Only
