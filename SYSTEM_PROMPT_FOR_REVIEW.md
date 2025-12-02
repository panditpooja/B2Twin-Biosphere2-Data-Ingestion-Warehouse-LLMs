# Biosphere 2 RAG System - System Prompt & Architecture Overview

## For Review with Joaquin - Foundation for Future Development

---

## 🎯 System Overview

**Biosphere 2 RAG-Powered Analysis System** is a Retrieval-Augmented Generation (RAG) application that enables intelligent querying of environmental sensor data from Biosphere 2. The system combines semantic search, vector embeddings, and AI-powered natural language understanding to answer complex questions about sensor readings, environmental conditions, and system operations.

### **Core Purpose:**
- Transform raw sensor CSV data into searchable, queryable knowledge
- Enable natural language questions about environmental monitoring
- Provide AI-powered insights with source attribution
- Support research and operational decision-making

---

## 🏗️ System Architecture

### **Technology Stack:**

1. **Backend Framework:** Flask (Python)
2. **AI/ML:**
   - **Embeddings:** SentenceTransformers (`all-MiniLM-L6-v2`)
   - **LLM:** Anthropic Claude API (Claude 3 Haiku)
   - **Vector Search:** FAISS (Facebook AI Similarity Search)
3. **Database:** SQLite (for metadata and embeddings storage)
4. **Frontend:** HTML/CSS/JavaScript (embedded in Flask template)
5. **Deployment:** Gunicorn WSGI server

### **Architecture Diagram:**

```
┌─────────────────────────────────────────────────────────────┐
│                    User Interface (Web UI)                    │
│  - Question input                                            │
│  - Answer display with sources                               │
│  - System status & statistics                                │
└──────────────────────┬──────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                    Flask Web Application                     │
│  - spectacular_rag_web_app.py                                 │
│  - API endpoints: /api/ask, /api/rag-stats, /api/system-status│
└──────────────────────┬──────────────────────────────────────┘
                        │
        ┌───────────────┴───────────────┐
        │                               │
        ▼                               ▼
┌───────────────────┐         ┌───────────────────┐
│  RAG Database     │         │  Simple Interface  │
│  (rag_database.py)│         │ (simple_interface) │
│                   │         │                   │
│  - Document chunks│         │  - Load CSV data  │
│  - Embeddings     │         │  - Parse sensors  │
│  - Vector index   │         │  - Create context │
│  - Semantic search│         │                   │
└───────────────────┘         └───────────────────┘
        │                               │
        └───────────────┬───────────────┘
                        │
        ┌───────────────┴───────────────┐
        │                               │
        ▼                               ▼
┌───────────────────┐         ┌───────────────────┐
│  SQLite Database  │         │  Sensor CSV Files │
│  biosphere2_rag.db│         │  (data/ folder)  │
│                   │         │                   │
│  - Documents table│         │  - 51 CSV files  │
│  - Embeddings     │         │  - Temperature   │
│  - Metadata       │         │  - Humidity      │
│  - FAISS index    │         │  - Fan systems   │
│                   │         │  - Valve controls│
└───────────────────┘         └───────────────────┘
```

---

## 🔄 Data Flow & Processing Pipeline

### **Step 1: Data Ingestion**
```
CSV Files (data/) 
    ↓
simple_interface.py
    ↓
Parse & Analyze:
- Extract sensor readings
- Calculate statistics (min, max, mean)
- Identify time ranges
- Sample data points
    ↓
Structured Sensor Data Dictionary
```

### **Step 2: Document Chunking**
```
Sensor Data Dictionary
    ↓
rag_database.create_document_chunks()
    ↓
For each sensor, create 4 chunk types:
1. Summary chunk (overview, time range, value range)
2. Sample data chunks (first 5 readings with timestamps)
3. Statistics chunk (min, max, mean values)
4. System overview (cross-sensor summary)
    ↓
~342 document chunks (for 51 sensors)
```

### **Step 3: Embedding Generation**
```
Document Chunks (text)
    ↓
SentenceTransformer Model (all-MiniLM-L6-v2)
    ↓
384-dimensional vectors
    ↓
Store in SQLite (embeddings table)
    ↓
Build FAISS vector index for fast similarity search
```

### **Step 4: Query Processing**
```
User Question: "What is the average temperature?"
    ↓
1. Encode question → 384-dim vector
    ↓
2. FAISS semantic search → Find top-K similar documents
    ↓
3. Retrieve full text from SQLite
    ↓
4. Build context string from retrieved documents
    ↓
5. Send to Claude API with context + question
    ↓
6. Return answer + source attribution
```

---

## 📊 Key Components

### **1. RAG Database (`rag_database.py`)**

**Purpose:** Core RAG system implementation

**Key Methods:**
- `create_document_chunks()` - Converts sensor data to searchable text chunks
- `add_documents()` - Stores chunks and generates embeddings (with duplicate prevention)
- `build_vector_index()` - Creates FAISS index for fast search
- `search()` - Semantic similarity search
- `get_context_for_question()` - Retrieves relevant context for AI

**Features:**
- Duplicate prevention (INSERT OR IGNORE)
- Validation and logging
- 384-dimensional embeddings
- FAISS vector similarity search

### **2. Web Application (`spectacular_rag_web_app.py`)**

**Purpose:** Flask web interface and API

**Routes:**
- `GET /` - Main UI page
- `POST /api/ask` - Process questions using RAG
- `GET /api/rag-stats` - Database statistics
- `GET /api/system-status` - System health

**Features:**
- Modern, responsive UI
- Real-time status updates
- Source attribution display
- Quick question buttons

### **3. Data Loader (`simple_interface.py`)**

**Purpose:** Load and process CSV sensor data

**Functions:**
- `load_all_sensor_data()` - Loads all CSV files from `data/` folder
- `create_comprehensive_context()` - Creates summary context

**Data Processing:**
- Handles UTF-8 and Latin-1 encoding
- Calculates statistics (min, max, mean)
- Extracts time ranges
- Samples data points

### **4. Validation Tool (`validate_embeddings.py`)**

**Purpose:** Monitor and maintain embedding health

**Functions:**
- `validate_rag_database()` - Check for duplicates and issues
- `clean_duplicates()` - Remove duplicate embeddings

**Usage:**
```bash
python validate_embeddings.py validate  # Check health
python validate_embeddings.py clean     # Remove duplicates
```

---

## 🗄️ Database Schema

### **Documents Table**
```sql
CREATE TABLE documents (
    id INTEGER PRIMARY KEY,
    doc_id TEXT UNIQUE,        -- e.g., "temperature_summary"
    content TEXT,               -- Full text content
    metadata TEXT,               -- JSON metadata
    embedding_id INTEGER,       -- Link to embedding
    created_at TIMESTAMP
)
```

### **Embeddings Table**
```sql
CREATE TABLE embeddings (
    id INTEGER PRIMARY KEY,
    doc_id TEXT UNIQUE,        -- One embedding per document
    embedding_vector BLOB       -- 384-dim vector as bytes
)
```

### **Sensor Readings Table**
```sql
CREATE TABLE sensor_readings (
    id INTEGER PRIMARY KEY,
    sensor_type TEXT,
    timestamp TEXT,
    value REAL,
    status TEXT,
    metadata TEXT,
    doc_id TEXT
)
```

---

## 🎯 Current System Capabilities

### **What It Can Do:**
✅ Answer questions about sensor data using natural language  
✅ Provide statistical summaries (min, max, average)  
✅ Identify trends across monitoring periods  
✅ Compare different sensors  
✅ Show source attribution with confidence scores  
✅ Handle 51 different sensor types  
✅ Process 342 document chunks with semantic search  

### **Example Questions That Work:**
- "What is the average temperature?"
- "What trends can you identify across the monitoring period?"
- "What is the temperature range across all sensors?"
- "What is the monitoring period for all sensors?"
- "Which sensor has the most readings?"

### **Limitations:**
- Data coverage: September 9 - October 9, 2025
- No real-time data updates (static CSV files)
- No time-series analysis (basic statistics only)
- No valve operation data correlation

---

## 🔧 Configuration & Environment

### **Required Environment Variables:**
```bash
ANTHROPIC_API_KEY=your_api_key_here
```

### **File Structure:**
```
project/
├── spectacular_rag_web_app.py  # Main app
├── rag_database.py              # RAG system
├── simple_interface.py          # Data loader
├── requirements.txt             # Dependencies
├── Procfile                     # Deployment config
├── Dockerfile                   # Docker config
├── data/                        # CSV sensor files (51 files)
│   └── *.csv
├── static/                      # Static assets
│   └── Biosphere_3_Wordmark_CLEAN.png
└── biosphere2_rag.db           # SQLite database (created at runtime)
```

---

## 🚀 Deployment Strategy

### **Current Deployment:**
- Platform: Jetstream Cloud (aicore-app-server)
- URL: `aicore-app-server.tra220030.projects.jetstream-cloud.org/biosphere-rag/`
- Process: Gunicorn WSGI server
- Database: Persistent SQLite file

### **Deployment Options:**
1. **Render/Heroku:** Use Procfile
2. **Docker:** Use Dockerfile
3. **Vercel:** Requires serverless adaptation
4. **Jetstream:** Current production deployment

---

## 📈 Future Building Strategies

### **Phase 1: Enhanced Data Processing**
- [ ] Real-time data ingestion from API
- [ ] Time-series analysis capabilities
- [ ] Data visualization (charts, graphs)
- [ ] Multi-period comparisons

### **Phase 2: Advanced RAG Features**
- [ ] Multi-modal embeddings (combine text + numeric data)
- [ ] Hierarchical document structure (sensors → systems → facility)
- [ ] Temporal context awareness (time-aware queries)
- [ ] Cross-sensor correlation analysis

### **Phase 3: User Experience**
- [ ] Interactive data exploration
- [ ] Saved queries and favorites
- [ ] Export capabilities (PDF reports, CSV exports)
- [ ] User authentication and access control

### **Phase 4: System Integration**
- [ ] API endpoints for programmatic access
- [ ] Webhook integrations
- [ ] Alert system (threshold monitoring)
- [ ] Dashboard for real-time monitoring

### **Phase 5: Advanced AI**
- [ ] Predictive analytics (forecasting)
- [ ] Anomaly detection
- [ ] Automated insights generation
- [ ] Multi-agent collaboration

---

## 🔍 Technical Details

### **Embedding Model:**
- **Model:** `all-MiniLM-L6-v2` (SentenceTransformers)
- **Dimensions:** 384
- **Purpose:** Convert text to semantic vectors
- **Performance:** ~50-100ms per document

### **Vector Search:**
- **Library:** FAISS (Facebook AI Similarity Search)
- **Index Type:** IndexFlatIP (Inner Product for cosine similarity)
- **Performance:** <10ms for similarity search
- **Top-K:** Default 5-10 results

### **LLM Integration:**
- **Provider:** Anthropic Claude API
- **Model:** Claude 3 Haiku
- **Max Tokens:** 300 (for concise answers)
- **Context Window:** Up to 5000 characters

### **Data Statistics:**
- **Sensors:** 51 different sensor types
- **Documents:** 342 document chunks
- **Embeddings:** 342 (1:1 ratio with documents)
- **Total Readings:** ~697,453 sensor data points
- **Time Coverage:** September 9 - October 9, 2025

---

## 🛠️ Development Workflow

### **Local Development:**
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set environment variable
export ANTHROPIC_API_KEY=your_key

# 3. Run application
python spectacular_rag_web_app.py

# 4. Access at http://localhost:5000
```

### **Testing:**
```bash
# Validate embeddings
python validate_embeddings.py validate

# Test RAG system
python -c "from rag_database import Biosphere2RAGDatabase; ..."
```

### **Deployment:**
```bash
# Build Docker image
docker build -t biosphere2-rag .

# Run container
docker run -p 5000:5000 -e ANTHROPIC_API_KEY=your_key biosphere2-rag

# Or use Procfile for platform deployment
gunicorn spectacular_rag_web_app:app --bind 0.0.0.0:$PORT
```

---

## 📝 Key Design Decisions

### **Why RAG Instead of Direct Database Queries?**
- **Semantic Understanding:** Natural language questions, not SQL
- **Flexibility:** Handles varied question phrasings
- **Context:** Provides relevant background information
- **Scalability:** Easy to add new sensors without schema changes

### **Why SentenceTransformers + FAISS?**
- **Open Source:** No vendor lock-in
- **Fast:** Local processing, no API calls for search
- **Efficient:** Handles thousands of documents easily
- **Customizable:** Full control over embedding and search

### **Why SQLite + FAISS?**
- **Lightweight:** No separate database server needed
- **Portable:** Single file database
- **Fast:** In-memory FAISS index, persistent SQLite storage
- **Simple:** Easy to backup and migrate

---

## 🎓 Learning Resources

### **RAG Concepts:**
- Retrieval-Augmented Generation combines information retrieval with LLMs
- Embeddings convert text to numerical vectors for similarity search
- FAISS enables fast similarity search on millions of vectors

### **Key Papers:**
- "Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks" (Lewis et al., 2020)
- "Dense Passage Retrieval for Open-Domain Question Answering" (Karpukhin et al., 2020)

### **Documentation:**
- SentenceTransformers: https://www.sbert.net/
- FAISS: https://github.com/facebookresearch/faiss
- Anthropic Claude: https://docs.anthropic.com/

---

## 🤝 Collaboration Notes

### **For Joaquin's Review:**
1. **Architecture:** Review the RAG pipeline and identify optimization opportunities
2. **Scalability:** Assess current limitations and plan for growth
3. **Integration:** Consider how this fits into broader Biosphere 2 data infrastructure
4. **Performance:** Evaluate embedding generation time and search speed
5. **Features:** Prioritize future enhancements based on user needs

### **Questions to Consider:**
- How can we integrate real-time data streams?
- What additional sensor types should we support?
- How do we handle multi-year data analysis?
- What visualization capabilities are needed?
- How can we improve answer quality and accuracy?

---

## 📞 Contact & Support

**System Maintainer:** [Your Name]  
**Repository:** [GitHub/Repository URL]  
**Documentation:** See `EMBEDDING_VALIDATION_GUIDE.md`, `DEMO_QUESTIONS_FOR_DEPLOYED_SYSTEM.md`

---

**Last Updated:** [Current Date]  
**Version:** 1.0  
**Status:** Production Ready


