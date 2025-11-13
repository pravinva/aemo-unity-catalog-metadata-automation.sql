# RAG Integration Guide: Vector Search + LLM for Chat

## Overview

!** Vector search results are just chunks of text. To make them useful in a chat application, you need to combine them with an LLM using the **RAG (Retrieval Augmented Generation)** pattern.

## The RAG Pattern

```
User Question
    ↓
Vector Search (finds relevant chunks)
    ↓
Format chunks as context
    ↓
Send context + question to LLM
    ↓
LLM generates answer using context
    ↓
Return answer to user
```

## Why RAG?

1. **Vector Search Alone**: Returns raw chunks - not conversational
2. **LLM Alone**: May hallucinate or lack specific knowledge
3. **RAG (Vector Search + LLM)**: 
   - Finds relevant information (vector search)
   - Generates natural, conversational answers (LLM)
   - Grounds answers in your actual data (prevents hallucination)

## Implementation Options

### Option 1: Databricks Genie (Easiest)

**Databricks Genie** does RAG automatically! It:
- Has built-in vector search integration
- Handles context retrieval
- Generates answers using foundation models
- Provides chat interface

**Example:**
```python
# Genie handles RAG automatically
response = genie_client.ask(
    space_id="your-space",
    question="What is dispatch price?",
    # Genie automatically:
    # 1. Searches vector index
    # 2. Retrieves relevant chunks
    # 3. Sends to LLM with context
    # 4. Returns natural answer
)
```

### Option 2: Custom RAG with Databricks Foundation Models

Use Databricks Foundation Models API with your vector search:

```python
from databricks.sdk.service.serving import EndpointsAPI

# 1. Vector search
contexts = vector_search(query)

# 2. Build prompt
prompt = f"""Context: {contexts}
Question: {query}
Answer:"""

# 3. Query LLM
serving_api = EndpointsAPI(workspace_client.api_client)
response = serving_api.query_endpoint(
    endpoint_name="databricks-meta-llama-3-1-70b-instruct",
    inputs=[{"messages": [{"role": "user", "content": prompt}]}]
)
```

### Option 3: Custom RAG with OpenAI/Anthropic

```python
import openai

# 1. Vector search
contexts = vector_search(query)

# 2. Build messages
messages = [
    {"role": "system", "content": "You are a helpful assistant."},
    {"role": "user", "content": f"Context: {contexts}\n\nQuestion: {query}"}
]

# 3. Query OpenAI
response = openai.ChatCompletion.create(
    model="gpt-4",
    messages=messages
)
```

## Example Flow

### Input: User Question
```
"What is dispatch price?"
```

### Step 1: Vector Search Retrieves Chunks
```python
chunks = [
    {
        'content': 'DISPATCHPRICE records 5 minute dispatch intervals...',
        'page': 267,
        'score': 0.616
    },
    # ... more chunks
]
```

### Step 2: Format Context
```
Context:
[Source: Page 267]
DISPATCHPRICE records 5 minute dispatch intervals...

[Source: Page 311]
BIDTYPE VARCHAR2(10) Bid type Identifier...
```

### Step 3: Send to LLM
```
Prompt:
You are a helpful assistant answering questions about AEMO.

Context:
[Source: Page 267]
DISPATCHPRICE records 5 minute dispatch intervals...

Question: What is dispatch price?

Answer:
```

### Step 4: LLM Generates Answer
```
Based on the AEMO Electricity Data Model documentation:

Dispatch price refers to the price recorded in the DISPATCHPRICE table, 
which tracks 5-minute dispatch intervals. This table contains pricing 
information for each dispatch interval in the electricity market.

The information comes from page 267 of the Electricity Data Model Report.
```

## Files in This Repo

1. **`test_vector_search_query.py`**: Tests vector search (retrieval only)
2. **`rag_example.py`**: Shows RAG pattern (retrieval + LLM structure)
3. **`chat_with_vector_search.py`**: Full RAG implementation example

## Quick Start

### Test Vector Search (Retrieval Only)
```bash
python test_vector_search_query.py
```

### See RAG Pattern
```bash
python rag_example.py "What is dispatch price?"
```

### Full RAG Implementation
```bash
python chat_with_vector_search.py "What is dispatch price?"
```

## Production Recommendations

1. **Use Databricks Genie**: Simplest option, handles RAG automatically
2. **Use Databricks Foundation Models**: Good for custom control
3. **Add Caching**: Cache common queries to reduce LLM calls
4. **Add Streaming**: Stream LLM responses for better UX
5. **Add Citations**: Always cite page numbers/sources
6. **Add Error Handling**: Handle LLM failures gracefully

## Key Benefits of RAG

✅ **Accurate**: Answers grounded in your actual data  
✅ **Conversational**: Natural language responses  
✅ **Contextual**: Uses relevant information from your PDF  
✅ **Scalable**: Works with large documents  
✅ **Traceable**: Can cite sources (page numbers)

## Next Steps

1. Set up Databricks Genie (recommended)
2. Or configure Foundation Models endpoint
3. Integrate RAG into your chat application
4. Add streaming and caching for production

