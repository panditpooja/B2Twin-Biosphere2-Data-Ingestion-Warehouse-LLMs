# Biosphere 2 RAG Database Implementation
# Retrieval-Augmented Generation for Enhanced Sensor Data Analysis

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Any, Tuple
import sqlite3
from pathlib import Path
import logging
import sys

# Configure logging to stdout (platform requirement)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# For embeddings and vector operations
try:
    import openai
    from sentence_transformers import SentenceTransformer
    import faiss
    HAS_EMBEDDINGS = True
except ImportError:
    HAS_EMBEDDINGS = False
    logger.warning("Install required packages: pip install openai sentence-transformers faiss-cpu")

class Biosphere2RAGDatabase:
    """
    RAG Database for Biosphere 2 Sensor Data Analysis
    
    Features:
    - Vector embeddings of sensor data
    - Semantic search capabilities
    - Context-aware retrieval
    - Multi-modal data integration
    """
    
    def __init__(self, db_path: str = None, embedding_model: str = "all-MiniLM-L6-v2"):
        # Use /app/data for persistent storage (platform requirement)
        # For local development, use current directory if /app/data doesn't exist
        if db_path is None:
            if os.path.exists("/app/data"):
                data_dir = "/app/data"
            else:
                # Local development - use current directory
                data_dir = "."
            os.makedirs(data_dir, exist_ok=True)
            db_path = os.path.join(data_dir, "biosphere2_rag.db")
        self.db_path = db_path
        self.embedding_model_name = embedding_model
        self.embedding_model = None
        self.vector_index = None
        self.documents = []
        self.metadata = []
        
        # Initialize database
        self.init_database()
        
        # Load embedding model if available
        if HAS_EMBEDDINGS:
            self.load_embedding_model()
    
    def init_database(self):
        """Initialize SQLite database for metadata storage"""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = self.conn.cursor()
        
        # Create tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_id TEXT UNIQUE,
                content TEXT,
                metadata TEXT,
                embedding_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_type TEXT,
                timestamp TEXT,
                value REAL,
                status TEXT,
                metadata TEXT,
                doc_id TEXT,
                FOREIGN KEY (doc_id) REFERENCES documents (doc_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS embeddings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_id TEXT UNIQUE,
                embedding_vector BLOB,
                FOREIGN KEY (doc_id) REFERENCES documents (doc_id)
            )
        ''')
        
        # Check if embeddings table needs UNIQUE constraint (for existing databases)
        try:
            cursor.execute('''
                CREATE UNIQUE INDEX IF NOT EXISTS idx_embeddings_doc_id 
                ON embeddings(doc_id)
            ''')
        except sqlite3.OperationalError:
            # Index might already exist, that's fine
            pass
        
        self.conn.commit()
        logger.info(f"[SUCCESS] RAG Database initialized: {self.db_path}")
    
    def load_embedding_model(self):
        """Load sentence transformer model for embeddings"""
        try:
            self.embedding_model = SentenceTransformer(self.embedding_model_name)
            logger.info(f"[SUCCESS] Embedding model loaded: {self.embedding_model_name}")
        except Exception as e:
            logger.error(f"[ERROR] Error loading embedding model: {e}")
            self.embedding_model = None
    
    def create_document_chunks(self, sensor_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Create document chunks from sensor data for RAG processing
        
        Args:
            sensor_data: Dictionary containing sensor data
            
        Returns:
            List of document chunks with metadata
        """
        chunks = []
        
        for sensor_type, data in sensor_data.items():
            # Create different types of chunks for each sensor
            
            # 1. Summary chunk
            # Determine if this is a temperature sensor
            is_temperature = 'temp' in sensor_type.lower() or 'tmp' in sensor_type.lower()
            unit_suffix = "°F" if is_temperature else ""
            
            summary_chunk = {
                "doc_id": f"{sensor_type}_summary",
                "content": f"""
                {sensor_type.replace('_', ' ').title()} Sensor Summary:
                - Total readings: {data.get('total_readings', 0)}
                - Time range: {data.get('time_range', 'Unknown')}
                - Value range: {data.get('value_stats', {}).get('min', 'N/A')}{unit_suffix} to {data.get('value_stats', {}).get('max', 'N/A')}{unit_suffix}
                - Sensor type: {sensor_type}
                - Units: {"Fahrenheit (°F)" if is_temperature else "Sensor units"}
                """,
                "metadata": {
                    "sensor_type": sensor_type,
                    "chunk_type": "summary",
                    "total_readings": data.get('total_readings', 0),
                    "time_range": data.get('time_range', 'Unknown')
                }
            }
            chunks.append(summary_chunk)
            
            # 2. Sample data chunks
            if 'sample_data' in data and data['sample_data']:
                for i, sample in enumerate(data['sample_data'][:5]):  # First 5 samples
                    sample_chunk = {
                        "doc_id": f"{sensor_type}_sample_{i}",
                        "content": f"""
                        {sensor_type.replace('_', ' ').title()} Sample Data {i+1}:
                        - Timestamp: {sample.get(' TIMESTAMP', 'Unknown')}
                        - Value: {sample.get(' VALUE', 'N/A')}
                        - Status: {sample.get(' STATUS_TAG', 'Unknown')}
                        - ID: {sample.get('ID', 'Unknown')}
                        """,
                        "metadata": {
                            "sensor_type": sensor_type,
                            "chunk_type": "sample_data",
                            "sample_index": i,
                            "timestamp": sample.get(' TIMESTAMP', 'Unknown'),
                            "value": sample.get(' VALUE', None)
                        }
                    }
                    chunks.append(sample_chunk)
            
            # 3. Statistical analysis chunk
            if 'value_stats' in data:
                is_temperature = 'temp' in sensor_type.lower() or 'tmp' in sensor_type.lower()
                unit_suffix = "°F" if is_temperature else ""
                
                stats_chunk = {
                    "doc_id": f"{sensor_type}_statistics",
                    "content": f"""
                    {sensor_type.replace('_', ' ').title()} Statistical Analysis:
                    - Minimum value: {data['value_stats'].get('min', 'N/A')}{unit_suffix}
                    - Maximum value: {data['value_stats'].get('max', 'N/A')}{unit_suffix}
                    - Average value: {data['value_stats'].get('mean', 'N/A')}{unit_suffix}
                    - Units: {"Fahrenheit (°F)" if is_temperature else "Sensor units"}
                    - Data quality: {'Good' if data.get('total_readings', 0) > 100 else 'Limited'}
                    """,
                    "metadata": {
                        "sensor_type": sensor_type,
                        "chunk_type": "statistics",
                        "min_value": data['value_stats'].get('min'),
                        "max_value": data['value_stats'].get('max'),
                        "mean_value": data['value_stats'].get('mean')
                    }
                }
                chunks.append(stats_chunk)
        
        # 4. System overview chunk
        total_readings = sum(data.get('total_readings', 0) for data in sensor_data.values())
        system_chunk = {
            "doc_id": "system_overview",
            "content": f"""
            Biosphere 2 Environmental Monitoring System Overview:
            - Total sensors: {len(sensor_data)}
            - Total readings: {total_readings}
            - Monitoring period: September 21-28, 2025
            - System status: Operational
            - Data quality: Comprehensive
            """,
            "metadata": {
                "sensor_type": "system",
                "chunk_type": "overview",
                "total_sensors": len(sensor_data),
                "total_readings": total_readings
            }
        }
        chunks.append(system_chunk)
        
        return chunks
    
    def add_documents(self, chunks: List[Dict[str, Any]]):
        """Add document chunks to the database with duplicate prevention"""
        cursor = self.conn.cursor()
        
        # Check existing embeddings to avoid regenerating (refresh check for each batch)
        cursor.execute('SELECT doc_id FROM embeddings')
        existing_embeddings = set(row[0] for row in cursor.fetchall())
        logger.info(f"[RAG] Found {len(existing_embeddings)} existing embeddings in database")
        
        # Also check existing documents
        cursor.execute('SELECT doc_id FROM documents')
        existing_docs = set(row[0] for row in cursor.fetchall())
        logger.info(f"[RAG] Found {len(existing_docs)} existing documents in database")
        
        new_docs = 0
        new_embeddings = 0
        skipped_embeddings = 0
        duplicate_attempts = 0
        
        for chunk in chunks:
            try:
                doc_id = chunk['doc_id']
                
                # Check if document already exists
                doc_exists = doc_id in existing_docs
                
                # Insert or update document
                cursor.execute('''
                    INSERT OR REPLACE INTO documents (doc_id, content, metadata)
                    VALUES (?, ?, ?)
                ''', (
                    doc_id,
                    chunk['content'],
                    json.dumps(chunk['metadata'])
                ))
                
                if not doc_exists:
                    new_docs += 1
                    existing_docs.add(doc_id)  # Update our set
                
                # Generate embedding only if it doesn't exist
                if self.embedding_model:
                    if doc_id not in existing_embeddings:
                        # Double-check in database (race condition protection)
                        cursor.execute('SELECT COUNT(*) FROM embeddings WHERE doc_id = ?', (doc_id,))
                        count = cursor.fetchone()[0]
                        
                        if count == 0:
                            # Safe to create embedding
                            embedding = self.embedding_model.encode(chunk['content'])
                            
                            # Store embedding using INSERT OR IGNORE to prevent duplicates
                            # This is safer than INSERT OR REPLACE because it won't overwrite existing
                            try:
                                cursor.execute('''
                                    INSERT OR IGNORE INTO embeddings (doc_id, embedding_vector)
                                    VALUES (?, ?)
                                ''', (
                                    doc_id,
                                    embedding.tobytes()
                                ))
                                # Check if row was actually inserted
                                if cursor.rowcount > 0:
                                    new_embeddings += 1
                                    existing_embeddings.add(doc_id)  # Update our set
                                else:
                                    # Row was ignored (already exists) - refresh our set
                                    duplicate_attempts += 1
                                    logger.debug(f"[RAG] Embedding already exists for {doc_id} (ignored insert)")
                                    cursor.execute('SELECT doc_id FROM embeddings')
                                    existing_embeddings = set(row[0] for row in cursor.fetchall())
                            except sqlite3.IntegrityError as e:
                                # UNIQUE constraint violation - embedding already exists
                                duplicate_attempts += 1
                                logger.warning(f"[RAG] Duplicate embedding prevented for {doc_id}: {e}")
                                # Refresh existing_embeddings from database
                                cursor.execute('SELECT doc_id FROM embeddings')
                                existing_embeddings = set(row[0] for row in cursor.fetchall())
                        else:
                            # Embedding exists in database but not in our set (race condition)
                            duplicate_attempts += 1
                            logger.debug(f"[RAG] Embedding already exists in DB for {doc_id}, skipping")
                            existing_embeddings.add(doc_id)
                    else:
                        skipped_embeddings += 1
                        logger.debug(f"[RAG] Embedding already exists for {doc_id}, skipping")
                
                self.documents.append(chunk)
                
            except Exception as e:
                logger.error(f"[ERROR] Error adding document {chunk['doc_id']}: {e}")
                import traceback
                traceback.print_exc()
        
        self.conn.commit()
        
        # Final validation: check for actual duplicates
        cursor.execute('''
            SELECT doc_id, COUNT(*) as count 
            FROM embeddings 
            GROUP BY doc_id 
            HAVING COUNT(*) > 1
        ''')
        actual_duplicates = cursor.fetchall()
        
        if actual_duplicates:
            logger.warning(f"[WARNING] Found {len(actual_duplicates)} duplicate embeddings after insertion!")
            for doc_id, count in actual_duplicates[:5]:
                logger.warning(f"  - {doc_id}: {count} duplicates")
        
        logger.info(f"[SUCCESS] Added {len(chunks)} documents to RAG database")
        logger.info(f"  - New documents: {new_docs}")
        logger.info(f"  - New embeddings created: {new_embeddings}")
        logger.info(f"  - Embeddings skipped (already exist): {skipped_embeddings}")
        if duplicate_attempts > 0:
            logger.info(f"  - Duplicate attempts prevented: {duplicate_attempts}")
    
    def build_vector_index(self):
        """Build FAISS vector index for similarity search"""
        if not self.embedding_model:
            logger.error("[ERROR] No embedding model available")
            return
        
        cursor = self.conn.cursor()
        # Use DISTINCT to avoid duplicate doc_ids (safety check)
        cursor.execute('SELECT DISTINCT doc_id, embedding_vector FROM embeddings')
        results = cursor.fetchall()
        
        if not results:
            logger.error("[ERROR] No embeddings found")
            return
        
        # Extract embeddings (deduplicate by doc_id)
        embeddings = []
        doc_ids = []
        seen_doc_ids = set()
        
        for doc_id, embedding_bytes in results:
            if doc_id not in seen_doc_ids:
                embedding = np.frombuffer(embedding_bytes, dtype=np.float32)
                embeddings.append(embedding)
                doc_ids.append(doc_id)
                seen_doc_ids.add(doc_id)
            else:
                logger.warning(f"Duplicate embedding found for {doc_id}, skipping")
        
        if not embeddings:
            logger.error("[ERROR] No valid embeddings after deduplication")
            return
        
        # Build FAISS index
        embeddings_array = np.array(embeddings)
        dimension = embeddings_array.shape[1]
        
        self.vector_index = faiss.IndexFlatIP(dimension)  # Inner product for cosine similarity
        self.vector_index.add(embeddings_array)
        
        # Store doc_ids for retrieval
        self.doc_ids = doc_ids
        
        logger.info(f"[SUCCESS] Vector index built with {len(embeddings)} embeddings (from {len(results)} database entries)")
    
    def search(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """
        Search for relevant documents using semantic similarity
        
        Args:
            query: Search query
            top_k: Number of top results to return
            
        Returns:
            List of relevant documents with similarity scores
        """
        if not self.vector_index or not self.embedding_model:
            logger.error("[ERROR] Vector index or embedding model not available")
            return []
        
        # Encode query
        query_embedding = self.embedding_model.encode([query])
        
        # Search
        scores, indices = self.vector_index.search(query_embedding, top_k)
        
        # Retrieve documents
        results = []
        cursor = self.conn.cursor()
        
        for score, idx in zip(scores[0], indices[0]):
            if idx < len(self.doc_ids):
                doc_id = self.doc_ids[idx]
                
                cursor.execute('''
                    SELECT content, metadata FROM documents WHERE doc_id = ?
                ''', (doc_id,))
                
                result = cursor.fetchone()
                if result:
                    content, metadata_json = result
                    metadata = json.loads(metadata_json)
                    
                    results.append({
                        'doc_id': doc_id,
                        'content': content,
                        'metadata': metadata,
                        'similarity_score': float(score)
                    })
        
        return results
    
    def get_context_for_question(self, question: str, max_context_length: int = 2000) -> str:
        """
        Get relevant context for a question using RAG
        
        Args:
            question: User question
            max_context_length: Maximum context length in characters
            
        Returns:
            Relevant context string
        """
        # Search for relevant documents
        search_results = self.search(question, top_k=5)
        
        # Build context
        context_parts = []
        current_length = 0
        
        for result in search_results:
            if current_length + len(result['content']) <= max_context_length:
                context_parts.append(f"[{result['metadata'].get('sensor_type', 'unknown')}] {result['content']}")
                current_length += len(result['content'])
            else:
                break
        
        return "\n\n".join(context_parts)
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        cursor = self.conn.cursor()
        
        # Count documents
        cursor.execute('SELECT COUNT(*) FROM documents')
        doc_count = cursor.fetchone()[0]
        
        # Count embeddings
        cursor.execute('SELECT COUNT(*) FROM embeddings')
        embedding_count = cursor.fetchone()[0]
        
        # Count unique sensor types from document metadata
        cursor.execute('SELECT DISTINCT json_extract(metadata, "$.sensor_type") FROM documents WHERE json_extract(metadata, "$.sensor_type") IS NOT NULL')
        sensor_types = cursor.fetchall()
        sensor_type_count = len([s for s in sensor_types if s[0] is not None and s[0] != ''])
        
        # Count total sensor readings from metadata (sum of total_readings from all sensor summaries)
        total_readings = 0
        cursor.execute('SELECT metadata FROM documents WHERE json_extract(metadata, "$.chunk_type") = "summary"')
        summaries = cursor.fetchall()
        for (metadata_json,) in summaries:
            try:
                metadata = json.loads(metadata_json)
                if 'total_readings' in metadata:
                    total_readings += int(metadata.get('total_readings', 0))
            except:
                pass
        
        # If we can't get from metadata, try to count from sensor_readings table
        if total_readings == 0:
            cursor.execute('SELECT COUNT(*) FROM sensor_readings')
            reading_result = cursor.fetchone()
            if reading_result:
                total_readings = reading_result[0]
        
        return {
            'documents': doc_count,
            'embeddings': embedding_count,
            'sensor_types': sensor_type_count,
            'total_readings': total_readings,
            'vector_index_size': len(self.doc_ids) if hasattr(self, 'doc_ids') else 0
        }

# Example usage and testing
if __name__ == "__main__":
    print("[RAG INIT] Initializing Biosphere 2 RAG Database...")
    
    # Initialize RAG database
    rag_db = Biosphere2RAGDatabase()
    
    # Load sensor data (you would load your actual data here)
    from simple_interface import load_all_sensor_data, create_comprehensive_context
    
    print("[DATA LOAD] Loading sensor data...")
    sensor_data = load_all_sensor_data()
    
    print("[CHUNKS] Creating document chunks...")
    chunks = rag_db.create_document_chunks(sensor_data)
    
    print("[DOCS] Adding documents to database...")
    rag_db.add_documents(chunks)
    
    print("[INDEX] Building vector index...")
    rag_db.build_vector_index()
    
    print("[STATS] Database statistics:")
    stats = rag_db.get_database_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    print("\n[SEARCH] Testing RAG search...")
    test_queries = [
        "What is the temperature range?",
        "How many fan readings were recorded?",
        "What are the valve system statuses?",
        "Tell me about the monitoring period"
    ]
    
    for query in test_queries:
        print(f"\nQuery: {query}")
        results = rag_db.search(query, top_k=3)
        for i, result in enumerate(results):
            print(f"  {i+1}. {result['doc_id']} (score: {result['similarity_score']:.3f})")
            print(f"     {result['content'][:100]}...")
    
    print("\n[SUCCESS] RAG Database setup complete!")

