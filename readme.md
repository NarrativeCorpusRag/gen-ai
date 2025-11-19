# Hybrid GraphRAG System for Narrative Corpus Synthesis

## Project Overview

This project implements a Hybrid Retrieval-Augmented Generation (RAG) pipeline to overcome the critical failure modes of traditional vector-only RAG systems: Connection Discovery and Synthesis.
We analyze the evolution of narratives across a complex, heterogeneous news corpus to provide deep, structural insights for high-stakes professional use (e.g., policy analysis and financial consulting).

The core technical achievement is the fusion of semantic vector search with structural graph traversal, leveraging a Knowledge Graph (KG) constructed from raw news articles.

### Project Goal

Our system is engineered to work at two specific types of analytical challenges:

**Connection Discovery (Multi-hop Reasoning):** Addressing the difficulty in finding implicit, structural relationships between entities (e.g., "I didn't realize these companies were related").

**Synthesis (Global Sense-making):** Providing a holistic, big-picture summary of an evolving situation across thousands of fragmented documents (e.g., "I have lots of notes but can't see the big picture").

## Local Setup and Installation

The main deliverable is a Python console script designed for local deployment and evaluation.

**Prerequisites:**

- Python (>=3.12)
- Docker ()
- Sufficient local RAM/Storage for the Knowledge Graph and the chosen LLM.

### Step-by-Step Installation

**Clone the Repository:**
``` bash
git clone https://github.com/NarrativeCorpusRag/gen-ai.git
cd hybrid-graphrag-synthesis
```

Start Database Services:
The project requires [Neo4j](http://neo4j.com/) (for the KG) and [LanceDB](https://lancedb.com/)


Execution Workflow

1. Data Ingestion and KG Construction 
\TODO
2. Run Evaluation against Baseline
\TODO
3. Manual Query (For Interactive Testing)

### Examples