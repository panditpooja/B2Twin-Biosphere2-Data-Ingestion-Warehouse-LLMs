# B2Twin-Biosphere2 Data Ingestion ETL Pipeline for LLMs

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green.svg)](https://fastapi.tiangolo.com/)

A comprehensive data processing and API system designed to extract, transform, and serve rainforest sensor data from Biosphere 2's Oracle databases to modern web applications and Large Language Model (LLM) systems.

## 📋 Overview

The Biosphere Pipeline is a production-ready ETL (Extract, Transform, Load) system that bridges legacy Oracle database systems with modern application architectures. It processes environmental sensor data from Biosphere 2's rainforest monitoring systems, implementing intelligent data joining strategies and providing a RESTful API for downstream applications.

### Key Features

- 🔄 **Three-Phase ETL Pipeline**: Extract from Oracle → Transform & Stage in SQLite → Serve via REST API
- 📊 **Intelligent Data Joining**: Category-based table joining with optimized strategies for different data types
- 🚀 **Incremental Data Loading**: 30-day rolling window with timestamp-based incremental updates
- 🌐 **RESTful API**: FastAPI-based API with automatic OpenAPI documentation
- 🐳 **Containerized Deployment**: Docker support for portable, scalable deployment
- 📈 **Comprehensive Monitoring**: Health checks, statistics endpoints, and pipeline monitoring
- 🔒 **Production-Ready**: Error handling, logging, input validation, and graceful degradation
- 📝 **Auto-Documentation**: Swagger UI and ReDoc for interactive API documentation

## 🏗️ Architecture

```
Oracle Database (AWS RDS)
    ↓
[Phase 1: Extraction & Staging]
    ↓
SQLite Staging Database
    ↓
[Phase 2: Transformation & Aggregation]
    ↓
Joined Tables (5 Categories)
    ↓
[Phase 3: API Serving]
    ↓
FastAPI REST Server
    ↓
Client Applications / LLMs
```

### Data Flow

1. **Extraction Phase**: Connects to Oracle production database, extracts sensor data based on configuration, implements incremental loading with unique ID generation
2. **Transformation Phase**: Categorizes tables into 5 groups (type1, type2, less50, between50and100, other), applies intelligent join strategies
3. **Serving Phase**: Provides RESTful API access with filtering, pagination, and statistical analysis

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- Oracle database access (for data extraction)
- SQLite (included with Python)
- Docker (optional, for containerized deployment)

### Installation

#### Option 1: Local Installation

```bash
# Clone or navigate to the project directory
cd biosphere_pipeline

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements_api.txt

# Verify configuration
python scripts/test_config.py
```

#### Option 2: Docker Deployment

```bash
# Build and run with Docker Compose
docker-compose up -d

# Or build individual container
docker build -t biosphere-pipeline .
docker run -p 8080:8080 -v $(pwd)/data:/app/data biosphere-pipeline
```

### Configuration

Update database credentials in `scripts/config.py`:

```python
# Oracle Configuration
ORACLE_USER = "your_username"
ORACLE_HOST = "your_host"
ORACLE_SERVICE = "your_service"
ORACLE_PORT = 1521
ORACLE_SCHEMA = "BIO2CONTROLSALL"

# SQLite Configuration (automatically configured)
SQLITE_DB_PATH = "data/biosphere_staging.db"
```

## 📖 Usage

### Running the Pipeline

#### Full Pipeline (Extract + Transform)
```bash
python scripts/biosphere_pipeline.py --phase all
```

#### Extraction Only (Oracle → SQLite)
```bash
python scripts/biosphere_pipeline.py --phase extract
```

#### Transformation Only (SQLite → Joined Tables)
```bash
python scripts/biosphere_pipeline.py --phase transform
```

#### Dry Run (Test without execution)
```bash
python scripts/biosphere_pipeline.py --phase all --dry-run
```

### Running the API Server

#### Option 1: Direct Python
```bash
python scripts/api_server.py
```

#### Option 2: Using Uvicorn
```bash
uvicorn scripts.api_server:app --host 0.0.0.0 --port 8000
```

#### Option 3: Windows Batch Script
```bash
start_api.bat
```

The API will be available at:
- **API Base**: http://localhost:8000
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### Pipeline Monitoring

```bash
python scripts/pipeline_monitor.py
```

## 🌐 API Documentation

### Core Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | API information and available endpoints |
| `/health` | GET | Health check and database connectivity |
| `/tables` | GET | List all available joined tables with metadata |

### Data Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/data/{category}` | GET | Get data from specific table category |
| `/data/{category}/stats` | GET | Get statistical information for table |
| `/data/{category}/unique_ids` | GET | Get list of unique IDs in table |
| `/data/{category}/time_range` | GET | Get time range covered by table |

### Query Parameters

#### `/data/{category}` Parameters:
- `limit` (int, 1-10000): Number of records to return (default: 100)
- `offset` (int, ≥0): Number of records to skip (default: 0)
- `start_date` (string): Start date filter in YYYY-MM-DD format
- `end_date` (string): End date filter in YYYY-MM-DD format
- `unique_id` (int): Filter by specific unique_id

### Table Categories

The API provides access to 5 different joined table categories:

1. **type1**: High-frequency sensor data with consistent row counts
2. **type2**: Medium-frequency sensor data with consistent row counts
3. **less50**: Low-frequency data with <50 rows per table
4. **between50and100**: Medium-frequency data with 50-100 rows per table
5. **other**: Miscellaneous data with variable row counts

### Example API Usage

```bash
# Get basic data
curl "http://localhost:8000/data/type1?limit=10"

# Get data with date filter
curl "http://localhost:8000/data/type1?start_date=2025-09-28&limit=5"

# Get statistics
curl "http://localhost:8000/data/type1/stats"

# Get unique IDs
curl "http://localhost:8000/data/type1/unique_ids?limit=20"
```

### Response Format

```json
{
  "category": "type1",
  "table_name": "joined_rainforest_ids_type1",
  "data": [
    {
      "unique_id": 1,
      "timestamp": "2025-09-28T00:00:03",
      "rftescovfdout": 0.0,
      "rftescosuptmp": 69.18407440185547
    }
  ],
  "pagination": {
    "limit": 10,
    "offset": 0,
    "total_count": 2881,
    "has_more": true
  },
  "filters_applied": {
    "start_date": "2025-09-28",
    "end_date": null,
    "unique_id": null
  }
}
```

## 📁 Project Structure

```
biosphere_pipeline/
├── scripts/                          # Core Python modules
│   ├── config.py                     # Configuration management
│   ├── bio2Oracle.py                 # Oracle data extraction
│   ├── biosphere_pipeline.py        # Main pipeline orchestrator
│   ├── join_rainforest_tables.py     # Data joining engine
│   ├── api_server.py                 # FastAPI application
│   ├── api_client.py                 # API testing client
│   ├── pipeline_monitor.py           # Pipeline monitoring
│   └── test_config.py                # Configuration testing
├── data/                             # Data storage
│   ├── biosphere_staging.db         # SQLite staging database
│   ├── joined_tables/                # Processed CSV files
│   └── tables_list/                  # Configuration files
├── logs/                             # Application logs
├── main.py                           # Application entry point
├── requirements.txt                  # Python dependencies
├── requirements_api.txt              # API dependencies
├── Dockerfile                        # Container configuration
├── docker-compose.yml                # Docker Compose configuration
├── README.md                         # This file
├── API_README.md                    # API documentation
├── PROJECT_IMPLEMENTATION_GUIDE.md  # Implementation details
├── DEPLOYMENT_README.md              # Deployment guide
└── RUN_APPLICATION.md                # Run instructions
```

## 🔧 Technical Details

### Technology Stack

- **Language**: Python 3.8+
- **Web Framework**: FastAPI 0.100.0+
- **Database Libraries**: 
  - `oracledb` 1.4.0+ for Oracle connectivity
  - `SQLAlchemy` 2.0+ for database ORM
  - `pandas` 2.0+ for data manipulation
- **API Server**: Uvicorn 0.20.0+
- **Validation**: Pydantic 1.10.0
- **Containerization**: Docker, Docker Compose

### Key Features Implementation

#### Incremental Data Loading
- Maintains `staging_metadata` table with last processed timestamps
- 30-day rolling window for data retention
- Table-specific unique ID generation
- 10-second timestamp buffer for edge cases

#### Intelligent Data Joining
- **Type1 & Type2**: INNER JOIN on unique_id (consistent data)
- **Less50, Between50and100, Other**: OUTER JOIN (variable data)
- Dynamic column selection and renaming
- CSV and database output support

#### API Features
- Async/await for non-blocking request handling
- Automatic OpenAPI/Swagger documentation
- CORS middleware for web integration
- Comprehensive error handling
- Input validation with Pydantic models

## 🐳 Deployment

### Docker Deployment

```bash
# Build image
docker build -t biosphere-pipeline .

# Run container
docker run -p 8080:8080 \
  -v $(pwd)/data:/app/data \
  biosphere-pipeline

# Or use Docker Compose
docker-compose up -d
```

### Production Considerations

- Configure CORS origins appropriately
- Use environment variables for sensitive credentials
- Implement authentication for production API
- Set up log rotation and monitoring
- Configure database connection pooling
- Enable HTTPS for API endpoints

## 🧪 Testing

### Test Configuration
```bash
python scripts/test_config.py
```

### Test API
```bash
# Start API server first
python scripts/api_server.py

# In another terminal, run API client
python scripts/api_client.py
```

### Pipeline Testing
```bash
# Dry run to test without execution
python scripts/biosphere_pipeline.py --phase all --dry-run
```

## 🐛 Troubleshooting

### Common Issues

#### ModuleNotFoundError
```bash
# Activate virtual environment and install dependencies
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements_api.txt
```

#### Oracle Connection Issues
- Verify Oracle credentials in `scripts/config.py`
- Check network connectivity to Oracle host
- Ensure Oracle Instant Client is properly configured (or use thin mode)
- Test connection with `python scripts/test_config.py`

#### Database Not Found
- Ensure SQLite database directory exists: `mkdir -p data`
- Run pipeline extraction phase to create staging database
- Check file permissions on data directory

#### Port Already in Use
- Change port in `api_server.py` or use different port
- Stop other services on port 8000/8080
- Use `--port` flag with uvicorn: `uvicorn scripts.api_server:app --port 8001`

#### Table Not Found in API
- Run join script first: `python scripts/join_rainforest_tables.py`
- Verify tables exist in SQLite database
- Check table naming convention matches API expectations

### View Logs

```bash
# View recent logs
ls -lt logs/ | head -5

# Tail log file
tail -f logs/biosphere_pipeline_*.log
```

## 📚 Documentation

- **[API_README.md](API_README.md)**: Comprehensive API documentation
- **[PROJECT_IMPLEMENTATION_GUIDE.md](PROJECT_IMPLEMENTATION_GUIDE.md)**: Detailed implementation guide
- **[DEPLOYMENT_README.md](DEPLOYMENT_README.md)**: Deployment instructions
- **[RUN_APPLICATION.md](RUN_APPLICATION.md)**: Step-by-step run guide
- **[Biosphere_Pipeline_Capstone_Proposal.docx](Biosphere_Pipeline_Capstone_Proposal.docx)**: Capstone project proposal

## 🎯 Use Cases

- **Environmental Monitoring**: Real-time access to Biosphere 2 sensor data
- **Data Analytics**: Statistical analysis and data exploration via API
- **LLM Integration**: Structured data preparation for Large Language Models
- **Web Applications**: RESTful API for web and mobile applications
- **Research**: Historical data analysis and trend identification

## 🔮 Future Enhancements

- [ ] Real-time data streaming capabilities
- [ ] Redis-based caching for improved performance
- [ ] User authentication and authorization
- [ ] Advanced monitoring dashboard
- [ ] Machine learning integration for predictive analytics
- [ ] Data visualization endpoints
- [ ] Webhook support for event notifications
- [ ] GraphQL API alternative

## 📄 License

This project is part of a capstone project for academic purposes.

## 👥 Contributing

This is a capstone project. For questions or issues, please refer to the project documentation or contact the project team.

## 🙏 Acknowledgments

- Biosphere 2 for providing environmental sensor data
- FastAPI community for excellent documentation
- Oracle for database connectivity libraries
- Python open-source community

---

**Note**: This project processes real environmental sensor data from Biosphere 2. Ensure proper credentials and permissions before accessing production databases.
