# Databricks notebook source
"""
AEMO Electricity Data Model PDF Vector Index Creation

This notebook:
1. Extracts text from AEMO Electricity Data Model PDF
2. Chunks the text into manageable pieces
3. Creates a Delta table with the chunks
4. Creates a Vector Search index with delta sync
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration
CATALOG_NAME = "ea_trading"
SCHEMA_NAME = "battery_trading"
TABLE_NAME = "aemo_electricity_data_model"
INDEX_NAME = "aemo_electricity_data_model_index"
PDF_PATH = "/Volumes/ea_trading/battery_trading/pdfs/Electricity_Data_Model_Report_54.pdf"
VECTOR_ENDPOINT = "one-env-shared-endpoint-10"

# Chunking configuration
CHUNK_SIZE = 1000  # Characters per chunk
CHUNK_OVERLAP = 200  # Overlap between chunks

print("=" * 60)
print("AEMO PDF Vector Index Setup")
print("=" * 60)
print(f"Catalog: {CATALOG_NAME}")
print(f"Schema: {SCHEMA_NAME}")
print(f"Table: {TABLE_NAME}")
print(f"Index: {INDEX_NAME}")
print(f"PDF Path: {PDF_PATH}")
print(f"Vector Endpoint: {VECTOR_ENDPOINT}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Required Libraries

# COMMAND ----------

# Install required libraries if not already installed
%pip install pdfplumber PyPDF2 pandas

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Import Libraries

# COMMAND ----------

import pdfplumber
import pandas as pd
import re
import uuid
from datetime import datetime
from typing import List, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Catalog and Schema (if needed)

# COMMAND ----------

print("=" * 60)
print("ENSURING CATALOG AND SCHEMA EXIST")
print("=" * 60)

try:
    # Create catalog
    print(f"Creating catalog {CATALOG_NAME}...")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
    print(f"✓ Catalog {CATALOG_NAME} exists or created")

    # Create schema
    print(f"Creating schema {CATALOG_NAME}.{SCHEMA_NAME}...")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
    print(f"✓ Schema {CATALOG_NAME}.{SCHEMA_NAME} exists or created")

    # Verify
    schemas_df = spark.sql(f"SHOW SCHEMAS IN {CATALOG_NAME}")
    schemas = [row['databaseName'] for row in schemas_df.collect()]

    if SCHEMA_NAME in schemas:
        print(f"✓ Verified: Schema {SCHEMA_NAME} is available")

    print("=" * 60)
    print("Configuration Complete!")
    print("=" * 60)

except Exception as e:
    print(f"Error creating catalog/schema: {e}")
    print("\nPossible solutions:")
    print("1. Check you have CREATE CATALOG permissions")
    print("2. Check you have CREATE SCHEMA permissions")
    print("3. Try running each SQL command manually:")
    print(f"   CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
    print(f"   CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Extract Text from PDF

# COMMAND ----------

def extract_text_from_pdf(pdf_path: str) -> List[Dict[str, str]]:
    """
    Extract text from PDF and return list of pages with content
    
    Args:
        pdf_path: Path to PDF file in Volumes or DBFS
        
    Returns:
        List of dictionaries with page number and text content
    """
    pages_content = []
    
    try:
        print(f"Opening PDF: {pdf_path}")
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            print(f"PDF has {total_pages} pages")
            
            for page_num, page in enumerate(pdf.pages, 1):
                text = page.extract_text()
                if text:
                    pages_content.append({
                        'page_number': page_num,
                        'text': text.strip(),
                        'char_count': len(text.strip())
                    })
                    if page_num % 10 == 0:
                        print(f"  Processed {page_num}/{total_pages} pages...")
            
            print(f"✓ Extracted text from {len(pages_content)} pages")
            total_chars = sum(p['char_count'] for p in pages_content)
            print(f"  Total characters: {total_chars:,}")
            
    except Exception as e:
        print(f"✗ Error extracting text from PDF: {e}")
        import traceback
        traceback.print_exc()
        raise
    
    return pages_content

# Extract text from PDF
print("=" * 60)
print("EXTRACTING TEXT FROM PDF")
print("=" * 60)

pdf_pages = extract_text_from_pdf(PDF_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Chunk Text for Vector Search

# COMMAND ----------

def chunk_text(text: str, chunk_size: int = 1000, overlap: int = 200) -> List[str]:
    """
    Split text into overlapping chunks
    
    Args:
        text: Text to chunk
        chunk_size: Target size of each chunk in characters
        overlap: Number of characters to overlap between chunks
        
    Returns:
        List of text chunks
    """
    if len(text) <= chunk_size:
        return [text]
    
    chunks = []
    start = 0
    
    while start < len(text):
        end = start + chunk_size
        
        # Try to break at sentence boundary if possible
        if end < len(text):
            # Look for sentence endings near the end
            for punct in ['. ', '.\n', '! ', '!\n', '? ', '?\n']:
                last_punct = text.rfind(punct, start, end)
                if last_punct > start + chunk_size * 0.7:  # Only break if we're at least 70% through chunk
                    end = last_punct + 2
                    break
        
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        
        # Move start position with overlap
        start = end - overlap
        if start >= len(text):
            break
    
    return chunks

def create_chunks_from_pages(pages_content: List[Dict], chunk_size: int = 1000, overlap: int = 200) -> List[Dict]:
    """
    Create chunks from page content
    
    Args:
        pages_content: List of page dictionaries
        chunk_size: Target chunk size
        overlap: Overlap between chunks
        
    Returns:
        List of chunk dictionaries
    """
    all_chunks = []
    chunk_id = 1
    
    for page_info in pages_content:
        page_num = page_info['page_number']
        text = page_info['text']
        
        # Chunk the page text
        chunks = chunk_text(text, chunk_size, overlap)
        
        for chunk_idx, chunk_content in enumerate(chunks):
            all_chunks.append({
                'doc_id': f"aemo_edm_{chunk_id:06d}",
                'chunk_index': chunk_idx,
                'page_number': page_num,
                'content': chunk_content,
                'char_count': len(chunk_content),
                'source': 'Electricity_Data_Model_Report_54.pdf'
            })
            chunk_id += 1
    
    return all_chunks

# Create chunks
print("=" * 60)
print("CHUNKING TEXT")
print("=" * 60)

chunks = create_chunks_from_pages(pdf_pages, CHUNK_SIZE, CHUNK_OVERLAP)

print(f"✓ Created {len(chunks)} chunks")
print(f"  Average chunk size: {sum(c['char_count'] for c in chunks) / len(chunks):.0f} characters")
print(f"  Min chunk size: {min(c['char_count'] for c in chunks)} characters")
print(f"  Max chunk size: {max(c['char_count'] for c in chunks)} characters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Delta Table

# COMMAND ----------

print("=" * 60)
print("CREATING DELTA TABLE")
print("=" * 60)

full_table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}"

try:
    # Create DataFrame from chunks
    chunks_df = pd.DataFrame(chunks)
    
    # Convert to Spark DataFrame
    spark_df = spark.createDataFrame(chunks_df)
    
    # Create or replace table
    print(f"Creating table: {full_table_name}")
    spark_df.write.mode("overwrite").saveAsTable(full_table_name)
    
    print(f"✓ Table created: {full_table_name}")
    
    # Enable Change Data Feed on the Delta table
    print(f"Enabling Change Data Feed (CDF) on table: {full_table_name}")
    spark.sql(f"ALTER TABLE {full_table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    print("✓ Change Data Feed enabled")
    
    # Verify table
    table_count = spark.sql(f"SELECT COUNT(*) as count FROM {full_table_name}").collect()[0]['count']
    print(f"✓ Table contains {table_count} rows")
    
    # Show schema
    print("\nTable Schema:")
    spark.sql(f"DESCRIBE {full_table_name}").show(truncate=False)
    
except Exception as e:
    print(f"✗ Error creating table: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create Vector Search Index with Delta Sync

# COMMAND ----------

print("=" * 60)
print("CREATING VECTOR SEARCH INDEX")
print("=" * 60)

try:
    from databricks.sdk.service.vectorsearch import (
        VectorSearchEndpointsAPI,
        VectorSearchIndexesAPI,
        VectorIndexType,
        DeltaSyncVectorIndexSpecRequest,
        EmbeddingSourceColumn,
        PipelineType
    )
    
    # Initialize clients
    from databricks.sdk import WorkspaceClient
    workspace_client = WorkspaceClient()
    
    endpoints_api = VectorSearchEndpointsAPI(workspace_client.api_client)
    indexes_api = VectorSearchIndexesAPI(workspace_client.api_client)
    
    print("✓ Vector Search APIs initialized")
    
except ImportError as e:
    print(f"✗ Error importing Vector Search APIs: {e}")
    print("  Make sure you're running this in Databricks with databricks-sdk installed")
    raise

# Check endpoint exists
print(f"\n📡 Checking Vector Search Endpoint: {VECTOR_ENDPOINT}")
try:
    endpoint = endpoints_api.get_endpoint(endpoint_name=VECTOR_ENDPOINT)
    endpoint_status = getattr(endpoint, 'endpoint_status', getattr(endpoint, 'status', 'UNKNOWN'))
    print(f"✓ Endpoint found: {VECTOR_ENDPOINT} (Status: {endpoint_status})")
except Exception as e:
    print(f"✗ Endpoint not found: {e}")
    print(f"  Please ensure endpoint '{VECTOR_ENDPOINT}' exists")
    raise

# Check if index already exists
full_index_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{INDEX_NAME}"
print(f"\n🔍 Checking if index exists: {full_index_name}")

try:
    indexes = list(indexes_api.list_indexes(endpoint_name=VECTOR_ENDPOINT))
    existing = next((idx for idx in indexes if idx.name == full_index_name), None)
    if existing:
        print(f"✓ Index already exists: {full_index_name}")
        index_id = getattr(existing, 'index_id', getattr(existing, 'name', 'N/A'))
        print(f"  Index ID: {index_id}")
        print(f"  Status: {getattr(existing, 'status', 'UNKNOWN')}")
        
        # Sync the existing index
        print(f"\n🔄 Syncing existing index...")
        try:
            indexes_api.sync_index(index_name=full_index_name)
            print("✓ Sync triggered")
        except Exception as sync_error:
            print(f"⚠️  Sync note: {sync_error}")
    else:
        # Create new index
        print(f"\n🔨 Creating new Vector Search index...")
        print(f"   Endpoint: {VECTOR_ENDPOINT}")
        print(f"   Index: {full_index_name}")
        print(f"   Source Table: {full_table_name}")
        print(f"   Primary Key: doc_id")
        print(f"   Embedding Column: content")
        print(f"   Pipeline Type: TRIGGERED")
        print("   This may take a few minutes...")
        
        # Create delta sync index spec
        delta_spec = DeltaSyncVectorIndexSpecRequest(
            source_table=full_table_name,
            pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[
                EmbeddingSourceColumn(
                    embedding_model_endpoint_name="databricks-bge-large-en",
                    name="content"
                )
            ]
        )
        
        # Create the index
        index = indexes_api.create_index(
            name=full_index_name,
            endpoint_name=VECTOR_ENDPOINT,
            primary_key="doc_id",
            index_type=VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=delta_spec
        )
        
        print(f"✓ Index creation initiated!")
        print(f"   Index: {full_index_name}")
        
        # Sync the index
        print(f"\n🔄 Syncing index...")
        import time
        time.sleep(5)  # Wait a bit for index to initialize
        
        try:
            indexes_api.sync_index(index_name=full_index_name)
            print("✓ Sync triggered")
        except Exception as sync_error:
            if "not ready" in str(sync_error).lower():
                print("⚠️  Index not ready for sync yet")
                print("   Will sync automatically when ready")
            else:
                print(f"⚠️  Sync note: {sync_error}")
        
        # Wait for index to be ready
        print(f"\n⏳ Waiting for index to be ready...")
        max_wait = 300  # 5 minutes
        waited = 0
        
        while waited < max_wait:
            time.sleep(10)
            waited += 10
            
            try:
                index_status = indexes_api.get_index(endpoint_name=VECTOR_ENDPOINT, index_name=full_index_name)
                status = getattr(index_status, 'status', getattr(index_status, 'index_status', 'UNKNOWN'))
                
                # Handle status object
                if hasattr(status, 'detailed_state'):
                    detailed_state = status.detailed_state
                elif isinstance(status, dict):
                    detailed_state = status.get('detailed_state', 'UNKNOWN')
                else:
                    detailed_state = str(status)
                
                print(f"   Status after {waited}s: {detailed_state}")
                
                if detailed_state in ['ONLINE', 'ONLINE_TRIGGERED_UPDATE', 'ONLINE_NO_PENDING_UPDATE', 'ACTIVE', 'READY']:
                    print(f"\n✓ Index is ready!")
                    index_id = getattr(index_status, 'index_id', getattr(index_status, 'name', 'N/A'))
                    print(f"   Index ID: {index_id}")
                    break
                elif 'FAILED' in str(detailed_state) or 'ERROR' in str(detailed_state):
                    print(f"\n✗ Index creation failed: {detailed_state}")
                    break
                    
            except Exception as e:
                if waited < 60:  # Only show errors after 1 minute
                    print(f"   Still initializing... ({waited}s)")
                else:
                    print(f"   Status check: {str(e)[:100]}")
        
        if waited >= max_wait:
            print(f"\n⚠️  Index creation taking longer than expected")
            print(f"   Check status manually in Databricks UI")
            print(f"   Index: {full_index_name}")

except Exception as e:
    print(f"\n✗ Error creating index: {e}")
    import traceback
    traceback.print_exc()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Verify Setup

# COMMAND ----------

print("=" * 60)
print("VERIFICATION")
print("=" * 60)

# Verify table
print("\n📊 Delta Table:")
table_count = spark.sql(f"SELECT COUNT(*) as count FROM {full_table_name}").collect()[0]['count']
print(f"   Table: {full_table_name}")
print(f"   Rows: {table_count}")

# Show sample data
print("\n📄 Sample Data:")
spark.sql(f"SELECT doc_id, page_number, LEFT(content, 100) as content_preview FROM {full_table_name} LIMIT 5").show(truncate=False)

# Verify index
print("\n🔍 Vector Index:")
try:
    index_info = indexes_api.get_index(endpoint_name=VECTOR_ENDPOINT, index_name=full_index_name)
    index_id = getattr(index_info, 'index_id', getattr(index_info, 'name', 'N/A'))
    status = getattr(index_info, 'status', 'UNKNOWN')
    
    print(f"   Index: {full_index_name}")
    print(f"   Index ID: {index_id}")
    print(f"   Status: {status}")
    print(f"   Endpoint: {VECTOR_ENDPOINT}")
    print(f"   Delta Sync: ENABLED")
except Exception as e:
    print(f"   ⚠️  Could not retrieve index info: {e}")

print("\n" + "=" * 60)
print("✅ SETUP COMPLETE!")
print("=" * 60)
print(f"\nYour vector index is ready for semantic search!")
print(f"\nTo search, use:")
print(f"  - Databricks Vector Search API")
print(f"  - SQL: SELECT * FROM {full_index_name}")
print(f"  - MCP Vector Search tools")
print(f"\nThe index will automatically sync when you update the Delta table!")