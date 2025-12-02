#!/usr/bin/env python3
"""
Validation script to check for duplicate embeddings and provide diagnostics
"""

import sqlite3
import os
from collections import Counter

def validate_rag_database(db_path="biosphere2_rag.db"):
    """Validate RAG database for duplicates and issues"""
    
    if not os.path.exists(db_path):
        print(f"[ERROR] Database file not found: {db_path}")
        return
    
    print(f"[INFO] Validating database: {db_path}")
    print(f"[INFO] File size: {os.path.getsize(db_path) / (1024*1024):.2f} MB")
    print()
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. Count documents
    cursor.execute('SELECT COUNT(*) FROM documents')
    doc_count = cursor.fetchone()[0]
    print(f"[DOCUMENTS] Total documents: {doc_count}")
    
    # 2. Count embeddings
    cursor.execute('SELECT COUNT(*) FROM embeddings')
    embedding_count = cursor.fetchone()[0]
    print(f"[EMBEDDINGS] Total embeddings: {embedding_count}")
    
    # 3. Check for duplicate doc_ids in embeddings
    cursor.execute('''
        SELECT doc_id, COUNT(*) as count 
        FROM embeddings 
        GROUP BY doc_id 
        HAVING COUNT(*) > 1
    ''')
    duplicates = cursor.fetchall()
    
    if duplicates:
        print(f"\n[WARNING] Found {len(duplicates)} duplicate doc_ids in embeddings table:")
        for doc_id, count in duplicates[:10]:  # Show first 10
            print(f"  - {doc_id}: {count} duplicates")
        if len(duplicates) > 10:
            print(f"  ... and {len(duplicates) - 10} more")
    else:
        print(f"\n[OK] No duplicate doc_ids found in embeddings table")
    
    # 4. Check for documents without embeddings
    cursor.execute('''
        SELECT COUNT(*) 
        FROM documents d
        LEFT JOIN embeddings e ON d.doc_id = e.doc_id
        WHERE e.doc_id IS NULL
    ''')
    docs_without_embeddings = cursor.fetchone()[0]
    
    if docs_without_embeddings > 0:
        print(f"\n[WARNING] {docs_without_embeddings} documents without embeddings")
    else:
        print(f"\n[OK] All documents have embeddings")
    
    # 5. Check for embeddings without documents
    cursor.execute('''
        SELECT COUNT(*) 
        FROM embeddings e
        LEFT JOIN documents d ON e.doc_id = d.doc_id
        WHERE d.doc_id IS NULL
    ''')
    embeddings_without_docs = cursor.fetchone()[0]
    
    if embeddings_without_docs > 0:
        print(f"[WARNING] {embeddings_without_docs} embeddings without corresponding documents")
    else:
        print(f"[OK] All embeddings have corresponding documents")
    
    # 6. Expected vs Actual
    print(f"\n[ANALYSIS]")
    print(f"  Documents: {doc_count}")
    print(f"  Embeddings: {embedding_count}")
    print(f"  Ratio: {embedding_count/doc_count:.2f} embeddings per document")
    
    if embedding_count > doc_count * 1.5:
        print(f"  [WARNING] Too many embeddings! Expected ~{doc_count}, got {embedding_count}")
        print(f"  [ACTION] Consider cleaning duplicates")
    elif embedding_count == doc_count:
        print(f"  [OK] Perfect 1:1 ratio")
    else:
        print(f"  [OK] Ratio is reasonable")
    
    # 7. Show sample doc_ids
    print(f"\n[SAMPLE] Sample document IDs (first 10):")
    cursor.execute('SELECT doc_id FROM documents LIMIT 10')
    for (doc_id,) in cursor.fetchall():
        print(f"  - {doc_id}")
    
    conn.close()
    print(f"\n[COMPLETE] Validation finished")

def clean_duplicates(db_path="biosphere2_rag.db", dry_run=True):
    """Remove duplicate embeddings, keeping only the most recent one"""
    
    if not os.path.exists(db_path):
        print(f"[ERROR] Database file not found: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Find duplicates
    cursor.execute('''
        SELECT doc_id, COUNT(*) as count 
        FROM embeddings 
        GROUP BY doc_id 
        HAVING COUNT(*) > 1
    ''')
    duplicates = cursor.fetchall()
    
    if not duplicates:
        print("[OK] No duplicates found. Nothing to clean.")
        conn.close()
        return
    
    print(f"[INFO] Found {len(duplicates)} doc_ids with duplicates")
    
    if dry_run:
        print("[DRY RUN] Would remove duplicates:")
        for doc_id, count in duplicates:
            print(f"  - {doc_id}: {count} entries (would keep 1, remove {count-1})")
        print("\n[INFO] Run with dry_run=False to actually remove duplicates")
    else:
        print("[CLEANING] Removing duplicate embeddings...")
        removed = 0
        
        for doc_id, count in duplicates:
            # Keep the one with the highest id (most recent), delete others
            cursor.execute('''
                DELETE FROM embeddings 
                WHERE doc_id = ? 
                AND id NOT IN (
                    SELECT MAX(id) FROM embeddings WHERE doc_id = ?
                )
            ''', (doc_id, doc_id))
            removed += cursor.rowcount
        
        conn.commit()
        print(f"[SUCCESS] Removed {removed} duplicate embeddings")
    
    conn.close()

if __name__ == "__main__":
    import sys
    
    # Check for command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "clean":
            dry_run = "--dry-run" in sys.argv or "-n" in sys.argv
            clean_duplicates(dry_run=dry_run)
        elif sys.argv[1] == "validate":
            validate_rag_database()
        else:
            print("Usage:")
            print("  python validate_embeddings.py validate  # Check for duplicates")
            print("  python validate_embeddings.py clean      # Show what would be cleaned (dry run)")
            print("  python validate_embeddings.py clean --no-dry-run  # Actually clean duplicates")
    else:
        # Default: just validate
        validate_rag_database()



