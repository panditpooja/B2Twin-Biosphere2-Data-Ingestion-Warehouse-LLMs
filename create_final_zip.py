#!/usr/bin/env python3
"""
Create final deployment zip file with all necessary files
"""

import zipfile
import os
from pathlib import Path

def create_deployment_zip():
    """Create a zip file with all necessary deployment files"""
    
    zip_filename = "biosphere2-rag-app-final-deployment.zip"
    
    # Files to include
    files_to_include = [
        # Main application files
        "spectacular_rag_web_app.py",
        "rag_database.py",
        "simple_interface.py",
        "requirements.txt",
        "Procfile",
        "Dockerfile",
        
        # Validation and utility scripts
        "validate_embeddings.py",
        
        # Demo question guides
        "DEMO_QUESTIONS_FOR_DEPLOYED_SYSTEM.md",
        "DEMO_QUICK_REFERENCE.txt",
        "EMBEDDING_VALIDATION_GUIDE.md",
    ]
    
    # Directories to include
    dirs_to_include = [
        "static",  # Logo image
        "data",    # CSV sensor data files
    ]
    
    print(f"Creating {zip_filename}...")
    print(f"Including {len(files_to_include)} files and {len(dirs_to_include)} directories...")
    
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Add individual files
        for file_path in files_to_include:
            if os.path.exists(file_path):
                zipf.write(file_path, file_path)
                print(f"  [OK] Added: {file_path}")
            else:
                print(f"  [MISSING] {file_path}")
        
        # Add directories
        for dir_path in dirs_to_include:
            if os.path.exists(dir_path):
                for root, dirs, files in os.walk(dir_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, '.')
                        zipf.write(file_path, arcname)
                        print(f"  [OK] Added: {arcname}")
            else:
                print(f"  [MISSING] Directory: {dir_path}")
    
    # Get file size
    file_size = os.path.getsize(zip_filename)
    file_size_mb = file_size / (1024 * 1024)
    
    print(f"\n[SUCCESS] Created {zip_filename}")
    print(f"  Size: {file_size_mb:.2f} MB ({file_size:,} bytes)")
    print(f"\nFiles included:")
    print(f"  - Main app: spectacular_rag_web_app.py")
    print(f"  - RAG database: rag_database.py")
    print(f"  - Data loader: simple_interface.py")
    print(f"  - Dependencies: requirements.txt")
    print(f"  - Deployment: Procfile, Dockerfile")
    print(f"  - Static assets: static/ folder")
    print(f"  - Sensor data: data/ folder")
    print(f"  - Demo guides: DEMO_QUESTIONS_FOR_DEPLOYED_SYSTEM.md, DEMO_QUICK_REFERENCE.txt")

if __name__ == "__main__":
    create_deployment_zip()

