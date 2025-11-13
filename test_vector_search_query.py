#!/usr/bin/env python3
"""
Test Vector Search Query Locally

This script queries the AEMO Electricity Data Model vector index using Databricks SDK.
Run this locally to test vector search queries.

Requirements:
- databricks-sdk installed
- Databricks credentials configured (via environment variables or .databrickscfg)
"""

import sys
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import VectorSearchIndexesAPI

# Configuration - Update these to match your setup
CATALOG_NAME = "ea_trading"
SCHEMA_NAME = "battery_trading"
INDEX_NAME = "aemo_electricity_data_model_index"
VECTOR_ENDPOINT = "one-env-shared-endpoint-10"
FULL_INDEX_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.{INDEX_NAME}"

def test_vector_search_query():
    """Test vector search queries on the AEMO index"""
    
    print("=" * 70)
    print("🔍 Testing Vector Search Queries")
    print("=" * 70)
    print(f"\nConfiguration:")
    print(f"   Index: {FULL_INDEX_NAME}")
    print(f"   Endpoint: {VECTOR_ENDPOINT}")
    
    # Initialize workspace client
    try:
        print("\n📡 Connecting to Databricks...")
        workspace_client = WorkspaceClient()
        print("✓ Connected successfully")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        print("\n💡 Make sure you have Databricks credentials configured:")
        print("   - Environment variables: DATABRICKS_HOST, DATABRICKS_TOKEN")
        print("   - Or .databrickscfg file in your home directory")
        return False
    
    # Initialize Vector Search API
    try:
        print("\n🔧 Initializing Vector Search API...")
        indexes_api = VectorSearchIndexesAPI(workspace_client.api_client)
        print("✓ API initialized")
    except Exception as e:
        print(f"✗ Failed to initialize API: {e}")
        return False
    
    # Test queries
    test_queries = [
        "What is the electricity data model?",
        "How are tables structured in AEMO?",
        "What are the key data entities?",
        "Explain the settlement process",
        "What is dispatch price?"
    ]
    
    print(f"\n📝 Running {len(test_queries)} test queries...")
    print("=" * 70)
    
    successful_queries = 0
    failed_queries = 0
    
    for i, query_text in enumerate(test_queries, 1):
        print(f"\n{'=' * 70}")
        print(f"Query {i}/{len(test_queries)}: {query_text}")
        print("=" * 70)
        
        try:
            # Execute query directly with parameters
            print(f"   Executing query...")
            query_result = indexes_api.query_index(
                index_name=FULL_INDEX_NAME,
                query_text=query_text,
                columns=["doc_id", "content", "page_number", "source"],
                num_results=3
            )
            
            print(f"✓ Query successful!")
            
            # Parse and display results
            if query_result:
                # QueryVectorIndexResponse has: manifest, next_page_token, result
                # result contains: data_array, next_page_token
                result_obj = query_result.result if hasattr(query_result, 'result') else None
                
                if result_obj:
                    # Extract data_array from result
                    if hasattr(result_obj, 'data_array'):
                        data_array = result_obj.data_array
                    elif isinstance(result_obj, dict):
                        data_array = result_obj.get('data_array', [])
                    else:
                        data_array = []
                else:
                    data_array = []
                
                # Get column names from manifest
                manifest = query_result.manifest if hasattr(query_result, 'manifest') else None
                column_names = []
                if manifest and hasattr(manifest, 'columns'):
                    column_names = [col.name for col in manifest.columns]
                else:
                    # Default column order based on what we requested
                    column_names = ["doc_id", "content", "page_number", "source", "score"]
                
                print(f"\n   📊 Found {len(data_array)} results")
                
                if data_array:
                    print(f"\n   📄 Top Results:")
                    for idx, result_row in enumerate(data_array[:3], 1):
                        print(f"\n   {'-' * 65}")
                        print(f"   Result {idx}:")
                        
                        # Results come as arrays, map to column names
                        if isinstance(result_row, list) and len(result_row) > 0:
                            # Map array values to column names
                            result_dict = {}
                            for i, col_name in enumerate(column_names):
                                if i < len(result_row):
                                    result_dict[col_name] = result_row[i]
                            
                            doc_id = result_dict.get('doc_id', 'N/A')
                            page_num = result_dict.get('page_number', 'N/A')
                            content = result_dict.get('content', '')
                            source = result_dict.get('source', 'N/A')
                            score = result_dict.get('score', 'N/A')
                            
                            print(f"   ├─ Doc ID: {doc_id}")
                            print(f"   ├─ Page: {page_num}")
                            print(f"   ├─ Source: {source}")
                            print(f"   ├─ Score: {score}")
                            print(f"   └─ Content Preview:")
                            # Show first 300 chars of content
                            if content:
                                content_preview = content[:300]
                                # Add indentation for multi-line content
                                for line in content_preview.split('\n'):
                                    print(f"      {line}")
                                if len(content) > 300:
                                    print(f"      ... ({len(content) - 300} more characters)")
                            else:
                                print(f"      N/A")
                        elif isinstance(result_row, dict):
                            # Handle dict format if it occurs
                            doc_id = result_row.get('doc_id', 'N/A')
                            page_num = result_row.get('page_number', 'N/A')
                            content = result_row.get('content', '')
                            score = result_row.get('score', 'N/A')
                            
                            print(f"   ├─ Doc ID: {doc_id}")
                            print(f"   ├─ Page: {page_num}")
                            print(f"   ├─ Score: {score}")
                            print(f"   └─ Content: {content[:200]}...")
                        else:
                            print(f"   Result: {str(result_row)[:200]}...")
                    
                    successful_queries += 1
                else:
                    print("   ⚠️  No results returned")
                    print(f"   Full response structure: {type(query_result)}")
                    if hasattr(query_result, '__dict__'):
                        print(f"   Response attributes: {list(query_result.__dict__.keys())}")
                    failed_queries += 1
            else:
                print("   ⚠️  Empty response")
                failed_queries += 1
                    
        except Exception as query_error:
            print(f"✗ Query failed: {query_error}")
            import traceback
            traceback.print_exc()
            
            # Provide helpful error messages
            error_str = str(query_error).lower()
            if "not found" in error_str:
                print(f"\n   💡 Tip: Ensure index '{FULL_INDEX_NAME}' exists and is fully created")
            elif "not ready" in error_str or "not online" in error_str:
                print(f"\n   💡 Tip: Wait a few minutes for index to be ready, then try again")
            elif "authentication" in error_str or "unauthorized" in error_str:
                print(f"\n   💡 Tip: Check your Databricks authentication credentials")
            elif "endpoint" in error_str:
                print(f"\n   💡 Tip: Verify endpoint '{VECTOR_ENDPOINT}' exists and is accessible")
            else:
                print(f"\n   💡 Tip: Index may need to be fully synced before querying")
                print(f"      Try syncing the index first or wait a few minutes")
            
            failed_queries += 1
    
    # Summary
    print(f"\n{'=' * 70}")
    print("📊 Query Test Summary")
    print("=" * 70)
    print(f"   ✓ Successful: {successful_queries}/{len(test_queries)}")
    print(f"   ✗ Failed: {failed_queries}/{len(test_queries)}")
    
    if successful_queries > 0:
        print(f"\n✅ Vector search is working! {successful_queries} queries returned results.")
    else:
        print(f"\n⚠️  No queries succeeded. Check the errors above.")
        print(f"\nTroubleshooting:")
        print(f"   1. Verify index exists: {FULL_INDEX_NAME}")
        print(f"   2. Check index status is ONLINE")
        print(f"   3. Ensure index has been synced")
        print(f"   4. Verify endpoint: {VECTOR_ENDPOINT}")
        print(f"   5. Check authentication credentials")
    
    return successful_queries > 0


if __name__ == "__main__":
    success = test_vector_search_query()
    sys.exit(0 if success else 1)

