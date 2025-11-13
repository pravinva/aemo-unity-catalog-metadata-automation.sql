# AEMO PDF Vector Index Setup Guide

This guide explains how to create a vector search index from the AEMO Electricity Data Model PDF.

## Overview

The script `create_aemo_vector_index.py` processes the AEMO PDF and creates:
1. **Delta Table**: Stores chunked text from the PDF
2. **Vector Index**: Enables semantic search with delta sync

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Vector Search endpoint: `one-env-shared-endpoint-10`
- Access to PDF at: `/Volumes/ea_trading/battery_trading/pdfs/Electricity_Data_Model_Report_54.pdf`
- Permissions:
  - CREATE CATALOG (or catalog already exists)
  - CREATE SCHEMA (or schema already exists)
  - CREATE TABLE
  - CREATE INDEX (Vector Search permissions)

## Configuration

The script uses the following configuration:

```python
CATALOG_NAME = "ea_trading"
SCHEMA_NAME = "battery_trading"
TABLE_NAME = "aemo_electricity_data_model"
INDEX_NAME = "aemo_electricity_data_model_index"
PDF_PATH = "/Volumes/ea_trading/battery_trading/pdfs/Electricity_Data_Model_Report_54.pdf"
VECTOR_ENDPOINT = "one-env-shared-endpoint-10"
```

### Chunking Parameters

- **CHUNK_SIZE**: 1000 characters per chunk
- **CHUNK_OVERLAP**: 200 characters overlap between chunks

Adjust these if needed for your use case.

## Usage

### Option 1: Run as Databricks Notebook

1. Upload `create_aemo_vector_index.py` to your Databricks workspace
2. Open it as a Databricks notebook
3. Run all cells sequentially
4. The script will:
   - Extract text from PDF
   - Create chunks
   - Create Delta table
   - Create Vector Search index with delta sync

### Option 2: Run via Databricks CLI

```bash
databricks workspace import create_aemo_vector_index.py /Users/your_user/aemo_vector_index.py --language PYTHON
databricks jobs run-now --job-id <your-job-id>
```

## What Gets Created

### Delta Table: `ea_trading.battery_trading.aemo_electricity_data_model`

**Schema:**
- `doc_id` (STRING) - Unique identifier for each chunk
- `chunk_index` (INT) - Index of chunk within page
- `page_number` (INT) - Source page number
- `content` (STRING) - Chunked text content (used for embeddings)
- `char_count` (INT) - Character count of chunk
- `source` (STRING) - Source PDF filename

### Vector Index: `ea_trading.battery_trading.aemo_electricity_data_model_index`

**Configuration:**
- **Endpoint**: `one-env-shared-endpoint-10`
- **Source Table**: `ea_trading.battery_trading.aemo_electricity_data_model`
- **Primary Key**: `doc_id`
- **Embedding Column**: `content`
- **Embedding Model**: `databricks-bge-large-en` (BGE-Large multilingual)
- **Pipeline Type**: `TRIGGERED` (updates on-demand)
- **Delta Sync**: ENABLED ✅

## Delta Sync Behavior

With delta sync enabled:
- **Automatic Updates**: When you INSERT/UPDATE/DELETE rows in the Delta table, the vector index updates automatically
- **Triggered Mode**: Updates run when you manually trigger them (via sync API or UI)
- **Consistency**: Maintains consistency between Delta table and vector index

## Searching the Index

### Using Databricks Vector Search API

```python
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()
index = vsc.get_index(
    endpoint_name="one-env-shared-endpoint-10",
    index_name="ea_trading.battery_trading.aemo_electricity_data_model_index"
)

# Search
results = index.similarity_search(
    query_text="What is the electricity data model?",
    columns=["content", "page_number"],
    num_results=5
)
```

### Using SQL (if supported)

```sql
SELECT * FROM ea_trading.battery_trading.aemo_electricity_data_model_index
WHERE query = 'What is the electricity data model?'
LIMIT 5
```

## Troubleshooting

### PDF Not Found

**Error**: `FileNotFoundError` or `PDF not accessible`

**Solutions**:
1. Verify PDF exists at: `/Volumes/ea_trading/battery_trading/pdfs/Electricity_Data_Model_Report_54.pdf`
2. Check Volume permissions
3. Ensure you're running in Databricks (not local Python)

### Endpoint Not Found

**Error**: `Endpoint not found: one-env-shared-endpoint-10`

**Solutions**:
1. Verify endpoint exists: Check Databricks UI → Vector Search → Endpoints
2. Check endpoint name spelling
3. Ensure you have access to the endpoint

### Index Creation Fails

**Error**: `Index creation failed` or timeout

**Solutions**:
1. Check endpoint is ONLINE
2. Verify table exists and has data
3. Check primary key column (`doc_id`) exists
4. Verify embedding column (`content`) exists
5. Check Databricks logs for detailed error

### Delta Sync Not Working

**Issue**: Changes to table don't reflect in index

**Solutions**:
1. Manually trigger sync:
   ```python
   indexes_api.sync_index(index_name="ea_trading.battery_trading.aemo_electricity_data_model_index")
   ```
2. Check index status in Databricks UI
3. Verify pipeline type is TRIGGERED or CONTINUOUS

## Updating the Index

### Adding New Documents

1. Insert new rows into the Delta table:
   ```sql
   INSERT INTO ea_trading.battery_trading.aemo_electricity_data_model
   VALUES ('new_doc_001', 0, 1, 'New content...', 100, 'new_source.pdf')
   ```

2. Trigger sync (if TRIGGERED mode):
   ```python
   indexes_api.sync_index(index_name="ea_trading.battery_trading.aemo_electricity_data_model_index")
   ```

### Updating Existing Documents

1. Update rows in Delta table:
   ```sql
   UPDATE ea_trading.battery_trading.aemo_electricity_data_model
   SET content = 'Updated content...'
   WHERE doc_id = 'aemo_edm_000001'
   ```

2. Trigger sync

### Rebuilding Index

If you need to rebuild the entire index:

1. Drop and recreate the index (via script or UI)
2. Or truncate table and reload:
   ```sql
   TRUNCATE TABLE ea_trading.battery_trading.aemo_electricity_data_model
   ```
   Then re-run the extraction script

## Performance Considerations

- **Chunk Size**: Smaller chunks (500-1000 chars) provide more granular search results
- **Chunk Overlap**: Overlap (100-200 chars) helps maintain context across chunks
- **Index Size**: Large PDFs will create many chunks - monitor index size
- **Sync Frequency**: TRIGGERED mode is more cost-effective than CONTINUOUS

## Next Steps

After creating the index:

1. **Test Search**: Try semantic searches on the AEMO data model
2. **Integrate**: Use in your applications via Vector Search API
3. **Monitor**: Check index status and sync frequency
4. **Optimize**: Adjust chunk size/overlap based on search quality

## Support

For issues:
1. Check Databricks Vector Search documentation
2. Review error logs in Databricks UI
3. Verify all prerequisites are met

