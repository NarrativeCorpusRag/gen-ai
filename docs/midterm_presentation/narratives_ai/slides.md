---
theme: seriph
background: https://cover.sli.dev
title: Hybrid GraphRAG for Narrative Synthesis
info: |
  ## Hybrid GraphRAG System
  TU Wien - Generative AI Winter Project
class: text-center
drawings:
  persist: false
transition: slide-left
mdc: true
---

# Hybrid GraphRAG System
## for Narrative Corpus Synthesis

<div class="opacity-80 text-lg mb-8">
Hasudin Hodzic, Emily Jacob, Hernan Picatto
</div>

<div class="text-sm opacity-60">
194.207-2025W Generative AI | TU Wien
</div>

---
layout: default
---

# 🎯 The Challenge: Narrative Analysis

Standard RAG retrieves *facts*, but professionals (policymakers, analysts) need to understand **Narratives**.

<div class="grid grid-cols-2 gap-8 mt-8">

<div class="border-l-4 border-blue-500 pl-4">

### 1. Connection Discovery
*Addressing:* "I didn't realize these companies were related."

Finding implicit, multi-hop relationships between entities that vector search misses.

</div>

<div class="border-l-4 border-green-500 pl-4">

### 2. Synthesis
*Addressing:* "I have 1,000 articles but can't see the big picture."

Making sense of how a story (e.g., the *Nexperia* case) evolves over time across fragmented sources.

</div>

</div>

---
layout: image-right
image: https://images.unsplash.com/photo-1550751827-4bd374c3f58b?ixlib=rb-1.2.1&auto=format&fit=crop&w=1350&q=80
---

# 💡 The Solution: Hybrid GraphRAG

We fuse **Semantic Vector Search** with **Knowledge Graph Traversal**.

### Core Pipeline

1.  **Understand**: Parse raw CC-NEWS data; extract entities & relationships into a Knowledge Graph (Neo4j).
2.  **Connect**: Perform vector search for "entry nodes," then traverse the graph to find structural links.
3.  **Synthesize**: Use hierarchical community detection (Leiden) to summarize clusters of information.

<div class="mt-8 text-sm opacity-70">
"Going beyond semantic similarity to model real-world structure."
</div>

---
layout: two-cols
layoutClass: gap-8
---

# 🏗️ Engineering: Data Collection

We are processing the **Common Crawl News (CC-NEWS)** dataset.

- **Scale**: Petabytes of data
- **Nature**: Unstructured, messy, heterogeneous HTML
- **Tooling**: `FastWARC`, `Resiliparse`, `Boto3`

<br>

### Current Status
- [x] S3 Stream implemented
- [x] HTML $\to$ Text extraction
- [x] Metadata cleaning (URI, Timestamp)
- [x] Partitioning by Year/Month/Lang

::right::

```python {all|1,9|2-3|4-6|all}
# src/pipeline/data_collection.py (Simplified)
from fastwarc.warc import ArchiveIterator, WarcRecordType
from resiliparse.extract.html2text import extract_plain_text

# Streaming directly from S3 (No local storage needed)
stream = GZipStream(s3_response['Body'])

for record in ArchiveIterator(stream, record_types=WarcRecordType.response):
    uri = record.headers.get('WARC-Target-URI')
    
    # Robust encoding detection & text extraction
    html = bytes_to_str(record.reader.read())
    text = extract_plain_text(html)
    
    # Metadata extraction
    entry = {
        'uri': uri,
        'text': text,
        'timestamp': record.http_date,
        'host': get_surt_host(uri)
    }
```
---
layout: center 
class: text-center
---

# 📊 Evaluation Strategy
We define success as a measurable improvement over a Standard Vector-RAG Baseline.

<div class="grid grid-cols-2 gap-10 mt-6">

<div>

## 📚 Datasets

- NewsQA (100k pairs): Tests reasoning & synthesis across sentences.
- NewsQuizQA (20k pairs): Validates informedness and fact retrieval.

</div>

<div>

## 📏 Metrics

- Factual Correctness: Is the answer true? 
- Context Relevance: Signal-to-noise ratio in retrieval. 
- Faithfulness: Is the answer grounded in context? 
- Answer Relevance: Does it address the user's intent? 

</div>

</div>
---
layout: center 
class: text-center
---

# 🔍 Open Questions & Discussion
<div class="text-left mx-auto w-3/3">

## Entity Linking:
Bootleg vs. Relik vs. LLM? We are prioritizing Relik for speed, but how do we best validate accuracy on this noisy corpus?

## Evaluation Scope:
Should we expand to Multi-News for summarization specifically?

## UI for Graph Traversal:
Is a CLI sufficient for "Connection Discovery," or do we need a prototype visualizer (e.g., Open-WebUI integration)? 

## Hierarchical Summarization:
Is cluster-based summarization feasible within the winter project timeframe? 

</div>
---
layout: center 
class: text-center
---

# Thank You
<div class="text-6xl mb-4"> Questions? </div>

<img src="./static/project_qr.png" class="w-48 mx-auto my-6" />

<div class="opacity-50"> Project Repository: https://github.com/NarrativeCorpusRag/gen-ai </div>