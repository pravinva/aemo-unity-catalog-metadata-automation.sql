#!/usr/bin/env python3
"""
Chat with Vector Search - RAG Pattern

This script demonstrates how to use vector search results with an LLM
to create a chat interface that can answer questions about the AEMO PDF.

RAG Flow:
1. User asks a question
2. Vector search finds relevant chunks from PDF
3. Chunks are passed as context to LLM
4. LLM generates answer using the context
"""

import sys
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import VectorSearchIndexesAPI

# Configuration
CATALOG_NAME = "ea_trading"
SCHEMA_NAME = "battery_trading"
INDEX_NAME = "aemo_electricity_data_model_index"
FULL_INDEX_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.{INDEX_NAME}"

# LLM Configuration - Using Databricks Foundation Models
# You can use: databricks-meta-llama-3-1-70b-instruct, databricks-mixtral-8x7b-instruct, etc.
LLM_MODEL = "databricks-meta-llama-3-1-70b-instruct"
NUM_CONTEXT_CHUNKS = 3  # Number of chunks to retrieve for context


def retrieve_context(query: str, indexes_api: VectorSearchIndexesAPI, num_results: int = NUM_CONTEXT_CHUNKS):
    """
    Retrieve relevant context chunks using vector search
    
    Args:
        query: User's question
        indexes_api: Vector Search API client
        num_results: Number of chunks to retrieve
        
    Returns:
        List of context chunks with metadata
    """
    try:
        query_result = indexes_api.query_index(
            index_name=FULL_INDEX_NAME,
            query_text=query,
            columns=["doc_id", "content", "page_number", "source"],
            num_results=num_results
        )
        
        if not query_result or not query_result.result or not query_result.result.data_array:
            return []
        
        # Get column names from manifest
        manifest = query_result.manifest
        column_names = [col.name for col in manifest.columns] if manifest and hasattr(manifest, 'columns') else ["doc_id", "content", "page_number", "source", "score"]
        
        # Parse results
        contexts = []
        for result_row in query_result.result.data_array:
            if isinstance(result_row, list) and len(result_row) > 0:
                result_dict = {}
                for i, col_name in enumerate(column_names):
                    if i < len(result_row):
                        result_dict[col_name] = result_row[i]
                
                contexts.append({
                    'content': result_dict.get('content', ''),
                    'page': result_dict.get('page_number', 'N/A'),
                    'source': result_dict.get('source', 'N/A'),
                    'score': result_dict.get('score', 'N/A')
                })
        
        return contexts
        
    except Exception as e:
        print(f"Error retrieving context: {e}")
        return []


def build_prompt(user_question: str, contexts: list) -> str:
    """
    Build a prompt for the LLM with context from vector search
    
    Args:
        user_question: User's question
        contexts: List of context chunks from vector search
        
    Returns:
        Formatted prompt string
    """
    if not contexts:
        return f"""Answer the following question based on your knowledge about AEMO (Australian Energy Market Operator) electricity data models:

Question: {user_question}

Note: No specific context was found, so answer based on general knowledge."""
    
    # Build context section
    context_text = "\n\n".join([
        f"[Context {i+1} - Page {ctx['page']}, Score: {ctx['score']:.3f}]\n{ctx['content']}"
        for i, ctx in enumerate(contexts)
    ])
    
    prompt = f"""You are a helpful assistant answering questions about the AEMO (Australian Energy Market Operator) Electricity Data Model.

Use the following context from the AEMO Electricity Data Model PDF to answer the user's question. If the context doesn't contain enough information, you can use your general knowledge, but prioritize the provided context.

Context from AEMO Electricity Data Model:
{context_text}

Question: {user_question}

Instructions:
- Answer based on the provided context when possible
- Cite page numbers when referencing specific information
- If the context doesn't fully answer the question, say so and provide what you can
- Be concise but thorough
- Use clear, professional language

Answer:"""
    
    return prompt


def query_llm(prompt: str, workspace_client: WorkspaceClient, model: str = LLM_MODEL) -> str:
    """
    Query LLM using Databricks Foundation Models API
    
    Args:
        prompt: The prompt to send to the LLM
        workspace_client: Databricks workspace client
        model: Model name to use
        
    Returns:
        LLM response text
    """
    try:
        from databricks.sdk.service.serving import EndpointsAPI
        
        # Use Databricks Serving Endpoints API
        serving_api = EndpointsAPI(workspace_client.api_client)
        
        # For Databricks Foundation Models, use the /serving-endpoints/{name}/invocations endpoint
        # This is a simplified example - you may need to adjust based on your setup
        print(f"   Querying LLM: {model}")
        print(f"   Prompt length: {len(prompt)} characters")
        
        # Note: Actual implementation depends on your serving endpoint setup
        # This is a placeholder showing the pattern
        # You would typically use:
        # response = serving_api.query_endpoint(
        #     endpoint_name=model,
        #     inputs=[{"messages": [{"role": "user", "content": prompt}]}]
        # )
        
        # For now, return a placeholder response
        return "[LLM Response would go here - configure your serving endpoint]"
        
    except Exception as e:
        print(f"Error querying LLM: {e}")
        return f"Error: Could not query LLM. {str(e)}"


def chat_with_rag(user_question: str):
    """
    Complete RAG flow: Retrieve context and generate answer
    
    Args:
        user_question: User's question
    """
    print("=" * 70)
    print("🤖 Chat with Vector Search (RAG Pattern)")
    print("=" * 70)
    print(f"\nQuestion: {user_question}\n")
    
    # Initialize clients
    try:
        workspace_client = WorkspaceClient()
        indexes_api = VectorSearchIndexesAPI(workspace_client.api_client)
        print("✓ Connected to Databricks\n")
    except Exception as e:
        print(f"✗ Connection error: {e}")
        return
    
    # Step 1: Retrieve relevant context
    print("📚 Step 1: Retrieving relevant context from vector search...")
    contexts = retrieve_context(user_question, indexes_api)
    
    if not contexts:
        print("   ⚠️  No relevant context found")
        return
    
    print(f"   ✓ Found {len(contexts)} relevant chunks")
    for i, ctx in enumerate(contexts, 1):
        print(f"      Chunk {i}: Page {ctx['page']}, Score: {ctx['score']:.3f}, Length: {len(ctx['content'])} chars")
    
    # Step 2: Build prompt with context
    print(f"\n📝 Step 2: Building prompt with context...")
    prompt = build_prompt(user_question, contexts)
    print(f"   ✓ Prompt built ({len(prompt)} characters)")
    
    # Step 3: Query LLM
    print(f"\n🧠 Step 3: Querying LLM...")
    answer = query_llm(prompt, workspace_client)
    
    # Display results
    print(f"\n{'=' * 70}")
    print("📄 Answer:")
    print("=" * 70)
    print(answer)
    
    # Show sources
    print(f"\n{'=' * 70}")
    print("📚 Sources:")
    print("=" * 70)
    for i, ctx in enumerate(contexts, 1):
        print(f"\nSource {i}:")
        print(f"   Page: {ctx['page']}")
        print(f"   Score: {ctx['score']:.3f}")
        print(f"   Preview: {ctx['content'][:150]}...")


def main():
    """Main function - can be used interactively or with command line args"""
    
    if len(sys.argv) > 1:
        # Command line mode
        question = " ".join(sys.argv[1:])
        chat_with_rag(question)
    else:
        # Interactive mode
        print("=" * 70)
        print("🤖 Chat with AEMO Electricity Data Model")
        print("=" * 70)
        print("\nEnter questions about the AEMO Electricity Data Model.")
        print("Type 'quit' or 'exit' to stop.\n")
        
        while True:
            try:
                question = input("Question: ").strip()
                
                if question.lower() in ['quit', 'exit', 'q']:
                    print("\nGoodbye!")
                    break
                
                if not question:
                    continue
                
                chat_with_rag(question)
                print("\n" + "-" * 70 + "\n")
                
            except KeyboardInterrupt:
                print("\n\nGoodbye!")
                break
            except Exception as e:
                print(f"\nError: {e}\n")


if __name__ == "__main__":
    main()

