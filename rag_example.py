#!/usr/bin/env python3
"""
RAG Example - Vector Search + GPT OSS Model

This demonstrates the complete RAG pattern:
1. Vector Search retrieves relevant chunks
2. Chunks are formatted as context
3. Context + question sent to GPT OSS model endpoint
4. LLM generates answer using the context

Usage:
    python rag_example.py "What is dispatch price?"
    python rag_example.py "What is dispatch price?" --endpoint your-gpt-oss-endpoint
"""

import sys
import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import VectorSearchIndexesAPI
from databricks.sdk.service.serving import ServingEndpointsAPI, ChatMessage, ChatMessageRole

# Configuration
INDEX_NAME = "ea_trading.battery_trading.aemo_electricity_data_model_index"
# Default GPT OSS endpoint
DEFAULT_ENDPOINT = "databricks-gpt-oss-20b"


def query_llm_endpoint(prompt: str, endpoint_name: str, workspace_client: WorkspaceClient) -> str:
    """
    Query GPT OSS model endpoint using Databricks Serving Endpoints API
    
    Args:
        prompt: The prompt to send to the LLM
        endpoint_name: Name of the serving endpoint
        workspace_client: Databricks workspace client
        
    Returns:
        LLM response text
    """
    try:
        serving_api = ServingEndpointsAPI(workspace_client.api_client)
        
        # Format messages for chat completion
        # GPT OSS models use ChatMessage format
        messages = [
            ChatMessage(
                role=ChatMessageRole.SYSTEM,
                content="You are a helpful assistant answering questions about the AEMO Electricity Data Model."
            ),
            ChatMessage(
                role=ChatMessageRole.USER,
                content=prompt
            )
        ]
        
        # Query the endpoint
        # Some endpoints don't support temperature, so make it optional
        query_params = {
            "name": endpoint_name,
            "messages": messages,
            "max_tokens": 1000
        }
        
        # Try with temperature first, fallback without if it fails
        try:
            response = serving_api.query(**query_params, temperature=0.7)
        except Exception as temp_error:
            if "temperature" in str(temp_error).lower() or "unsupported" in str(temp_error).lower():
                # Retry without temperature
                response = serving_api.query(**query_params)
            else:
                raise
        
        # Parse QueryEndpointResponse
        # Response has choices attribute with ChatMessage objects
        if hasattr(response, 'choices') and response.choices:
            # Get first choice
            choice = response.choices[0]
            if hasattr(choice, 'message') and hasattr(choice.message, 'content'):
                content = choice.message.content
                
                # Handle structured responses (e.g., from GPT OSS models with reasoning)
                if isinstance(content, list):
                    # Extract text from structured response
                    text_parts = []
                    for item in content:
                        if isinstance(item, dict):
                            if item.get('type') == 'text' and 'text' in item:
                                text_parts.append(item['text'])
                            elif item.get('type') == 'reasoning':
                                # Skip reasoning, just extract summary if needed
                                if 'summary' in item and isinstance(item['summary'], list):
                                    for summary_item in item['summary']:
                                        if isinstance(summary_item, dict) and summary_item.get('type') == 'summary_text':
                                            # Optionally include reasoning summary
                                            pass
                    return '\n\n'.join(text_parts) if text_parts else str(content)
                elif isinstance(content, str):
                    return content
                else:
                    return str(content)
            elif hasattr(choice, 'text'):
                return choice.text
        
        # Fallback: try to extract from response object
        if hasattr(response, 'text'):
            return response.text
        
        # Last resort: string representation
        return str(response)
        
    except Exception as e:
        error_msg = f"Error querying LLM endpoint '{endpoint_name}': {str(e)}"
        print(f"   Error: {error_msg}")
        print(f"\n   Troubleshooting:")
        print(f"      1. Verify endpoint '{endpoint_name}' exists")
        print(f"      2. Check endpoint is in 'Ready' state")
        print(f"      3. Verify you have permissions to query the endpoint")
        print(f"      4. Check endpoint accepts OpenAI-compatible format")
        raise Exception(error_msg)


def rag_flow(user_question: str, endpoint_name: str = None):
    """
    Complete RAG (Retrieval Augmented Generation) flow
    
    Args:
        user_question: User's question
    """
    print("=" * 70)
    print("RAG Flow: Vector Search → LLM")
    print("=" * 70)
    
    # Initialize
    workspace_client = WorkspaceClient()
    indexes_api = VectorSearchIndexesAPI(workspace_client.api_client)
    
    # STEP 1: Vector Search - Retrieve relevant chunks
    print(f"\nSTEP 1 - RETRIEVE: Searching for relevant context...")
    print(f"   Query: '{user_question}'")
    
    query_result = indexes_api.query_index(
        index_name=INDEX_NAME,
        query_text=user_question,
        columns=["doc_id", "content", "page_number", "source"],
        num_results=3  # Get top 3 most relevant chunks
    )
    
    # Parse results
    contexts = []
    if query_result and query_result.result and query_result.result.data_array:
        manifest = query_result.manifest
        column_names = [col.name for col in manifest.columns] if manifest else ["doc_id", "content", "page_number", "source", "score"]
        
        for row in query_result.result.data_array:
            if isinstance(row, list):
                result_dict = {column_names[i]: row[i] for i in range(min(len(column_names), len(row)))}
                contexts.append({
                    'doc_id': result_dict.get('doc_id', 'N/A'),
                    'content': result_dict.get('content', ''),
                    'page': result_dict.get('page_number', 'N/A'),
                    'source': result_dict.get('source', 'N/A'),
                    'score': result_dict.get('score', 'N/A')
                })
    
    print(f"   ✓ Found {len(contexts)} relevant chunks")
    for i, ctx in enumerate(contexts, 1):
        print(f"      Chunk {i}: Page {ctx['page']}, Score: {ctx['score']:.3f}")
    
    # STEP 2: Format context for LLM
    print(f"\nSTEP 2 - FORMAT: Building context for LLM...")
    
    context_text = "\n\n---\n\n".join([
        f"[Source: Page {ctx['page']}]\n{ctx['content']}"
        for ctx in contexts
    ])
    
    # STEP 3: Build prompt with context + question
    # Number each context chunk for reference
    numbered_contexts = []
    for i, ctx in enumerate(contexts, 1):
        chunk_header = f"[Chunk {i} - Doc ID: {ctx['doc_id']}, Page {ctx['page']}, Score: {ctx['score']:.3f}]"
        numbered_contexts.append(f"{chunk_header}\n{ctx['content']}")
    
    context_text = "\n\n---\n\n".join(numbered_contexts)
    
    prompt = f"""You are a helpful assistant answering questions about the AEMO Electricity Data Model.

Use the following context from the AEMO Electricity Data Model PDF to answer the question. Each context chunk is numbered for reference.

Context:
{context_text}

Question: {user_question}

Instructions:
- Answer the question based on the context provided
- When referencing information, cite the chunk number, doc ID, and page number like: [Chunk 1, Doc ID: aemo_edm_000001, Page 267]
- If information comes from multiple chunks, cite all relevant chunks
- Be specific about which chunk(s) you're referencing
- You can also use shorter citations like: [Chunk 1, Page 267] or [Chunk 2] if the full citation is too verbose"""

    print(f"   ✓ Prompt built ({len(prompt)} characters)")
    
    # STEP 4: Send to LLM endpoint
    if not endpoint_name:
        endpoint_name = DEFAULT_ENDPOINT
    
    print(f"\nSTEP 3 - GENERATE: Sending to GPT OSS model endpoint...")
    print(f"   Endpoint: {endpoint_name}")
    
    try:
        answer = query_llm_endpoint(prompt, endpoint_name, workspace_client)
        
        # Display the answer
        print(f"\n{'=' * 70}")
        print("LLM Answer:")
        print("=" * 70)
        print(answer)
        
        # Show sources with chunk references
        print(f"\n{'=' * 70}")
        print("Vector Search Chunks Used:")
        print("=" * 70)
        for i, ctx in enumerate(contexts, 1):
            chunk_ref = f"Chunk {i}"
            print(f"\n{chunk_ref}:")
            print(f"   Page: {ctx['page']}")
            print(f"   Score: {ctx['score']:.3f}")
            print(f"   Doc ID: {ctx.get('doc_id', 'N/A')}")
            print(f"   Source: {ctx.get('source', 'N/A')}")
            print(f"   Content Preview:")
            # Show first 200 chars with line breaks
            preview = ctx['content'][:200]
            for line in preview.split('\n')[:3]:  # Show first 3 lines
                print(f"      {line}")
            if len(ctx['content']) > 200:
                print(f"      ... ({len(ctx['content']) - 200} more characters)")
        
    except Exception as e:
        print(f"\nWarning: Could not query LLM endpoint")
        print(f"   Error: {e}")
        print(f"\n   Showing what would be sent to LLM:")
        print(f"\n{'=' * 70}")
        print("Prompt sent to LLM:")
        print("=" * 70)
        print(prompt[:1000] + "..." if len(prompt) > 1000 else prompt)
    
    print(f"\n{'=' * 70}")
    print("RAG Flow Complete!")
    print("=" * 70)
    print("\nKey Points:")
    print("   1. Vector Search finds relevant chunks (semantic similarity)")
    print("   2. Chunks provide context to LLM (grounds answer in your data)")
    print("   3. LLM generates answer using context (RAG pattern)")
    print("   4. Answer is accurate because it's based on your PDF content")


def list_endpoints(workspace_client: WorkspaceClient):
    """List available serving endpoints"""
    try:
        serving_api = ServingEndpointsAPI(workspace_client.api_client)
        # List endpoints - method returns an iterator
        endpoints = []
        try:
            for endpoint in serving_api.list():
                endpoints.append(endpoint)
        except AttributeError:
            # Try alternative method name
            endpoints = list(serving_api.list_all())
        
        print("=" * 70)
        print("Available Serving Endpoints:")
        print("=" * 70)
        
        if not endpoints:
            print("   No endpoints found")
            return
        
        for endpoint in endpoints:
            state = 'UNKNOWN'
            if hasattr(endpoint, 'state'):
                state_obj = endpoint.state
                if hasattr(state_obj, 'config_update'):
                    state = state_obj.config_update
                elif hasattr(state_obj, 'ready'):
                    state = 'READY'
                else:
                    state = str(state_obj)
            
            print(f"\n   Name: {endpoint.name}")
            print(f"   State: {state}")
            if hasattr(endpoint, 'creator'):
                print(f"   Creator: {endpoint.creator}")
        
        print(f"\n{'=' * 70}")
        print(f"Total: {len(endpoints)} endpoint(s)")
        print(f"\nUsage:")
        print(f"   python rag_example.py 'your question' --endpoint <endpoint-name>")
        
    except Exception as e:
        print(f"Error listing endpoints: {e}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="RAG Example: Vector Search + GPT OSS Model")
    parser.add_argument("question", nargs="?", default=None, 
                       help="Question to ask about AEMO Electricity Data Model")
    parser.add_argument("--endpoint", "-e", default=None,
                       help=f"GPT OSS model endpoint name (default: {DEFAULT_ENDPOINT})")
    parser.add_argument("--list-endpoints", "-l", action="store_true",
                       help="List available serving endpoints")
    
    args = parser.parse_args()
    
    if args.list_endpoints:
        workspace_client = WorkspaceClient()
        list_endpoints(workspace_client)
    else:
        question = args.question if args.question else "What is dispatch price?"
        rag_flow(question, args.endpoint)

