# 🔍 Embedding Validation Guide

## Why Embeddings Might Be Increasing

### **Expected Behavior:**
- **First run**: Creates embeddings for all document chunks (e.g., 342 documents → 342 embeddings)
- **Subsequent runs**: Should **NOT** create new embeddings if they already exist
- **Expected ratio**: 1 embedding per document chunk (1:1 ratio)

### **Problem Indicators:**
- Embeddings count keeps increasing on each restart
- Embeddings count >> Documents count (e.g., 342 docs but 1000+ embeddings)
- Multiple embeddings for the same `doc_id`

---

## 🔧 Root Causes

### **1. Database Path Issues**
**Problem**: App creates a new database file each time instead of reusing existing one.

**Check**:
```python
# In rag_database.py, check the db_path logic:
if os.path.exists("/app/data"):
    data_dir = "/app/data"  # Production
else:
    data_dir = "."  # Local development
```

**Solution**: Ensure database path is consistent across restarts.

### **2. Missing UNIQUE Constraint**
**Problem**: Database table doesn't have UNIQUE constraint on `doc_id`.

**Check**:
```sql
-- Run this in your database:
SELECT sql FROM sqlite_master WHERE name='embeddings';
-- Should show: doc_id TEXT UNIQUE
```

**Solution**: The code now enforces UNIQUE constraint (see `rag_database.py` line 102).

### **3. Race Conditions**
**Problem**: Multiple app instances or threads creating embeddings simultaneously.

**Solution**: Code now has double-checking (database query + in-memory set).

### **4. App Restart Without Persistence**
**Problem**: Database file gets deleted/recreated on each deployment.

**Solution**: Ensure database file persists between deployments.

---

## ✅ Validation Steps

### **Step 1: Run Validation Script**

```bash
python validate_embeddings.py validate
```

**Expected Output:**
```
[DOCUMENTS] Total documents: 342
[EMBEDDINGS] Total embeddings: 342
[OK] No duplicate doc_ids found in embeddings table
[OK] All documents have embeddings
[OK] All embeddings have corresponding documents
[ANALYSIS] Ratio: 1.00 embeddings per document
[OK] Perfect 1:1 ratio
```

**Problem Output:**
```
[DOCUMENTS] Total documents: 342
[EMBEDDINGS] Total embeddings: 1000
[WARNING] Found 50 duplicate doc_ids in embeddings table
[WARNING] Ratio: 2.92 embeddings per document
[WARNING] Too many embeddings!
```

### **Step 2: Check Database Location**

```python
import os
db_path = "biosphere2_rag.db"  # or "/app/data/biosphere2_rag.db"
if os.path.exists(db_path):
    print(f"Database exists: {db_path}")
    print(f"Size: {os.path.getsize(db_path) / (1024*1024):.2f} MB")
else:
    print(f"Database NOT found: {db_path}")
```

### **Step 3: Check for Duplicates**

```python
import sqlite3
conn = sqlite3.connect("biosphere2_rag.db")
cursor = conn.cursor()

# Find duplicates
cursor.execute('''
    SELECT doc_id, COUNT(*) as count 
    FROM embeddings 
    GROUP BY doc_id 
    HAVING COUNT(*) > 1
    ORDER BY count DESC
    LIMIT 10
''')
duplicates = cursor.fetchall()

if duplicates:
    print("Found duplicates:")
    for doc_id, count in duplicates:
        print(f"  {doc_id}: {count} entries")
else:
    print("No duplicates found!")
```

---

## 🛠️ Fixing Duplicates

### **Option 1: Clean Existing Duplicates**

```bash
# Dry run (see what would be removed)
python validate_embeddings.py clean

# Actually remove duplicates
python validate_embeddings.py clean --no-dry-run
```

### **Option 2: Rebuild Database**

```python
# Delete old database and rebuild
import os
if os.path.exists("biosphere2_rag.db"):
    os.remove("biosphere2_rag.db")
    print("Deleted old database")

# Restart app - it will rebuild from scratch
```

### **Option 3: Manual SQL Cleanup**

```sql
-- Keep only the most recent embedding for each doc_id
DELETE FROM embeddings 
WHERE id NOT IN (
    SELECT MAX(id) 
    FROM embeddings 
    GROUP BY doc_id
);
```

---

## 🔒 Prevention Measures (Already Implemented)

### **1. UNIQUE Constraint**
```python
# In rag_database.py line 102
CREATE TABLE IF NOT EXISTS embeddings (
    doc_id TEXT UNIQUE,  # ← Prevents duplicates at database level
    ...
)
```

### **2. Pre-Check Before Creation**
```python
# In rag_database.py line 244-245
cursor.execute('SELECT doc_id FROM embeddings')
existing_embeddings = set(row[0] for row in cursor.fetchall())

# Only create if doesn't exist
if chunk['doc_id'] not in existing_embeddings:
    # Create embedding
```

### **3. Double-Check in Database**
```python
# In rag_database.py (improved version)
cursor.execute('SELECT COUNT(*) FROM embeddings WHERE doc_id = ?', (doc_id,))
count = cursor.fetchone()[0]
if count == 0:
    # Safe to create
```

### **4. Exception Handling**
```python
# In rag_database.py
try:
    cursor.execute('INSERT INTO embeddings ...')
except sqlite3.IntegrityError:
    # UNIQUE constraint violation - already exists
    logger.warning("Duplicate prevented")
```

---

## 📊 Monitoring

### **Check Embedding Count on Each Restart**

Add this to your app startup:
```python
stats = rag_database.get_database_stats()
print(f"Documents: {stats['documents']}")
print(f"Embeddings: {stats['embeddings']}")
print(f"Ratio: {stats['embeddings']/stats['documents']:.2f}")

if stats['embeddings'] > stats['documents'] * 1.1:
    print("[WARNING] Too many embeddings detected!")
```

### **Expected Log Messages**

**Good (no duplicates):**
```
[RAG] Found 342 existing embeddings in database
[RAG] Found 342 existing documents in database
[SUCCESS] Added 342 documents to RAG database
  - New documents: 0
  - New embeddings created: 0
  - Embeddings skipped (already exist): 342
```

**Problem (creating duplicates):**
```
[RAG] Found 0 existing embeddings in database  ← Should not be 0 on restart!
[SUCCESS] Added 342 documents to RAG database
  - New embeddings created: 342  ← Creating new ones each time
```

---

## 🎯 Quick Diagnosis

**If embeddings keep increasing:**

1. ✅ **Check database path** - Is it the same file each time?
2. ✅ **Run validation script** - `python validate_embeddings.py validate`
3. ✅ **Check logs** - Look for "Found X existing embeddings" message
4. ✅ **Check for duplicates** - Use validation script
5. ✅ **Clean if needed** - `python validate_embeddings.py clean --no-dry-run`

---

## 📝 Summary

**Normal Behavior:**
- First run: Creates all embeddings
- Subsequent runs: Skips existing embeddings
- Ratio: 1:1 (1 embedding per document)

**Problem Behavior:**
- Each restart creates new embeddings
- Ratio > 1.5 (too many embeddings)
- Duplicate doc_ids in embeddings table

**Solution:**
- Use validation script to diagnose
- Clean duplicates if found
- Ensure database path is consistent
- Code now has improved duplicate prevention



