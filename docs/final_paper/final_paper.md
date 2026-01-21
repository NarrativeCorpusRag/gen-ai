# Hybrid GraphRAG System for Narrative Corpus Synthesis

## Abstract

## Description

### Ideal User Journey

querying the system about a complex, multi-entity event. The system performs a semantic search to identify entry-point nodes in the graph, then traverses structurally-related information to retrieve both direct matches and hidden connections.

### Technical Approach and Reasoning

#### Data Preprocessing

Docling  with Trafilatura fallback - Docling handles structured HTML better but slow. Fallback chain maximizes extraction success rate.
Gopher + C4 + FineWeb quality filters stacked - Each catches different garbage: Gopher handles repetition/boilerplate, C4 catches code/JSON, FineWeb removes navigation fragments. All taken from datatrove
#### Chunking

512 words + 50 word overlap, it's usual length for embedding models (most trained on ~512 tokens).

#### Embeddings

FlagEmbedding because it's opensource, small and decent quality allegedly
#### Graph Extraction

Separate entity-linking + relation-extraction models - Entity model (relik-entity-linking) gives Wikipedia IDs for disambiguation; relation model (relik-relation-extraction-nyt) extracts triplets. Running both on same GPU keeps memory footprint reasonable while getting both capabilities.
Confidence threshold 0.5 on relations - Relik relation extraction is noisy as Emily mentioned . 
Text-based entity matching in relations - entity_text_to_id dict links relation heads/tails back to entity-linking results. This connects the two model outputs and enables Wikipedia ID propagation to relation endpoints.
chunk_id as the join key everywhere - Entities and relations link back to chunks, so graph traversal always ends at retrievable text. This enables "find related chunks via entity graph" pattern.
MongoDB Atlas Vector Search over alternatives; MongoDB Atlas gives us vector search + document store + graph-like queries in one place without adding Pinecone/Neo4j and save us if implementing our own clustering.
Hybrid RAG combines 2 retrieval signals - Vector (semantic similarity) + graph (entity relationships)


### Challenges and Technical Obstacles

Preprocessing CC-NEWS proved challenging due to messy HTML, also while news are updated daily. It required decent amount of work for custom WARC parsing and deduplication. KG construction involved trade-offs small models were fast but rigid LLMs better but slow and expensive. Evaluation lacked ground truth, and proper tools, (just sendingad hoc queries and see which one is beter) 

## Evaluation
