import os
from typing import Iterator, List, Dict, Any, Optional
from dataclasses import dataclass
import pandas as pd
from dagster import (
    asset, 
    AssetExecutionContext,
    MonthlyPartitionsDefinition, 
    Config,
    ConfigurableResource,
    MaterializeResult,
    MetadataValue,
)
from dagster_pyspark import PySparkResource
from pyspark.sql import functions as F
from pymongo import MongoClient, UpdateOne
from pymongo.operations import SearchIndexModel


monthly_partitions = MonthlyPartitionsDefinition(start_date="2025-01-01")

@dataclass
class RAGQueryResult:
    """Result from RAG query"""
    chunks: List[Dict[str, Any]]
    entities: List[Dict[str, Any]]
    relations: List[Dict[str, Any]]
    context_text: str
    metadata: Dict[str, Any]


class RAGQueryEngine:
    """
    Query engine supporting Traditional RAG, GraphRAG, and Hybrid approaches.
    """
    
    def __init__(
        self,
        mongodb_uri: str,
        database_name: str = "news_rag",
        embedding_model = None,  # Your FlagEmbedding model
    ):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client[database_name]
        self.chunks_collection = self.db["chunks"]
        self.entities_collection = self.db["entities"]
        self.relations_collection = self.db["relations"]
        self.embedding_model = embedding_model
    
    def embed_query(self, query: str) -> List[float]:
        """Generate embedding for query text"""
        if self.embedding_model is None:
            raise ValueError("Embedding model not initialized")
        return self.embedding_model.encode([query])[0].tolist()
    
    # =========================================================================
    # TRADITIONAL RAG: Vector Similarity Search
    # =========================================================================
    
    def traditional_rag(
        self,
        query: str,
        top_k: int = 5,
        filters: Optional[Dict] = None,
    ) -> RAGQueryResult:
        """
        Traditional RAG using MongoDB Atlas Vector Search.
        
        Args:
            query: User's question
            top_k: Number of chunks to retrieve
            filters: Optional filters (year, month, host)
        
        Returns:
            RAGQueryResult with relevant chunks
        """
        query_embedding = self.embed_query(query)
        
        # Build vector search pipeline
        vector_search_stage = {
            "$vectorSearch": {
                "index": "vector_index",
                "path": "embedding",
                "queryVector": query_embedding,
                "numCandidates": top_k * 10,
                "limit": top_k,
            }
        }
        
        # Add filters if provided
        if filters:
            vector_search_stage["$vectorSearch"]["filter"] = filters
        
        pipeline = [
            vector_search_stage,
            {
                "$project": {
                    "chunk_id": 1,
                    "doc_id": 1,
                    "chunk_text": 1,
                    "uri": 1,
                    "host": 1,
                    "year": 1,
                    "month": 1,
                    "score": {"$meta": "vectorSearchScore"},
                    "_id": 0,
                }
            }
        ]
        
        chunks = list(self.chunks_collection.aggregate(pipeline))
        
        # Build context text
        context_text = "\n\n---\n\n".join([
            f"[Source: {c.get('uri', 'unknown')}]\n{c['chunk_text']}"
            for c in chunks
        ])
        
        return RAGQueryResult(
            chunks=chunks,
            entities=[],
            relations=[],
            context_text=context_text,
            metadata={"method": "traditional_rag", "top_k": top_k}
        )
    
    # =========================================================================
    # GRAPH RAG: Entity and Relation-based Retrieval
    # =========================================================================
    
    def graph_rag(
        self,
        query: str,
        top_k_chunks: int = 3,
        max_hops: int = 2,
        max_related_chunks: int = 5,
    ) -> RAGQueryResult:
        """
        GraphRAG: Expand retrieval using entity relationships.
        
        Process:
        1. Vector search to find initial chunks
        2. Extract entities from those chunks
        3. Find related entities via relations
        4. Retrieve chunks containing related entities
        5. Combine for richer context
        
        Args:
            query: User's question
            top_k_chunks: Initial chunks from vector search
            max_hops: How many relation hops to traverse
            max_related_chunks: Max additional chunks from graph
        
        Returns:
            RAGQueryResult with chunks, entities, and relations
        """
        # Step 1: Initial vector search
        initial_result = self.traditional_rag(query, top_k=top_k_chunks)
        initial_chunk_ids = [c["chunk_id"] for c in initial_result.chunks]
        
        # Step 2: Get entities from initial chunks
        entities = list(self.entities_collection.find(
            {"chunk_id": {"$in": initial_chunk_ids}}
        ))
        entity_texts = list(set(e["text"] for e in entities))
        
        # Step 3: Find relations involving these entities
        relations = list(self.relations_collection.find({
            "$or": [
                {"head_text": {"$in": entity_texts}},
                {"tail_text": {"$in": entity_texts}}
            ]
        }).limit(50))
        
        # Step 4: Get related entity texts
        related_entity_texts = set()
        for rel in relations:
            related_entity_texts.add(rel["head_text"])
            related_entity_texts.add(rel["tail_text"])
        
        # Step 5: Find chunks containing related entities
        related_chunk_ids = set()
        if related_entity_texts:
            related_entities = self.entities_collection.find(
                {"text": {"$in": list(related_entity_texts)}}
            )
            for ent in related_entities:
                if ent["chunk_id"] not in initial_chunk_ids:
                    related_chunk_ids.add(ent["chunk_id"])
        
        # Step 6: Retrieve related chunks
        related_chunks = []
        if related_chunk_ids:
            related_chunks = list(self.chunks_collection.find(
                {"chunk_id": {"$in": list(related_chunk_ids)[:max_related_chunks]}},
                {"embedding": 0}  # Exclude embeddings from result
            ))
        
        # Combine all chunks
        all_chunks = initial_result.chunks + related_chunks
        
        # Build enhanced context with entity/relation info
        context_parts = []
        
        # Add initial chunks
        context_parts.append("=== Directly Relevant Content ===")
        for c in initial_result.chunks:
            context_parts.append(f"[Source: {c.get('uri', 'unknown')}]\n{c['chunk_text']}")
        
        # Add entity summary
        if entities:
            context_parts.append("\n=== Key Entities ===")
            entity_summary = ", ".join(sorted(set(e["text"] for e in entities[:20])))
            context_parts.append(entity_summary)
        
        # Add relation summary
        if relations:
            context_parts.append("\n=== Key Relationships ===")
            for rel in relations[:10]:
                context_parts.append(
                    f"- {rel['head_text']} --[{rel['relation']}]--> {rel['tail_text']}"
                )
        
        # Add related chunks
        if related_chunks:
            context_parts.append("\n=== Related Content (via entity graph) ===")
            for c in related_chunks:
                context_parts.append(f"[Source: {c.get('uri', 'unknown')}]\n{c['chunk_text']}")
        
        context_text = "\n\n".join(context_parts)
        
        return RAGQueryResult(
            chunks=all_chunks,
            entities=entities,
            relations=relations,
            context_text=context_text,
            metadata={
                "method": "graph_rag",
                "initial_chunks": len(initial_result.chunks),
                "related_chunks": len(related_chunks),
                "entities_found": len(entities),
                "relations_found": len(relations),
            }
        )
    
    # =========================================================================
    # HYBRID RAG: Combine Vector Search + Graph + Keywords
    # =========================================================================
    
    def hybrid_rag(
        self,
        query: str,
        top_k_vector: int = 3,
        top_k_keyword: int = 2,
        use_graph: bool = True,
        max_graph_chunks: int = 3,
        filters: Optional[Dict] = None,
    ) -> RAGQueryResult:
        """
        Hybrid RAG combining multiple retrieval strategies:
        1. Vector similarity search
        2. Keyword/text search
        3. Graph-based expansion
        
        Args:
            query: User's question
            top_k_vector: Chunks from vector search
            top_k_keyword: Chunks from keyword search
            use_graph: Whether to include graph expansion
            max_graph_chunks: Max chunks from graph traversal
            filters: Optional filters
        
        Returns:
            RAGQueryResult with combined results
        """
        all_chunks = []
        all_chunk_ids = set()
        entities = []
        relations = []
        
        # Strategy 1: Vector Search
        query_embedding = self.embed_query(query)
        
        vector_pipeline = [
            {
                "$vectorSearch": {
                    "index": "vector_index",
                    "path": "embedding",
                    "queryVector": query_embedding,
                    "numCandidates": top_k_vector * 10,
                    "limit": top_k_vector,
                }
            },
            {
                "$addFields": {
                    "retrieval_method": "vector",
                    "score": {"$meta": "vectorSearchScore"}
                }
            },
            {"$project": {"embedding": 0}}
        ]
        
        vector_chunks = list(self.chunks_collection.aggregate(vector_pipeline))
        for c in vector_chunks:
            if c["chunk_id"] not in all_chunk_ids:
                all_chunks.append(c)
                all_chunk_ids.add(c["chunk_id"])
        
        # Strategy 2: Text/Keyword Search (using Atlas Search)
        text_pipeline = [
            {
                "$search": {
                    "index": "text_index",
                    "text": {
                        "query": query,
                        "path": "chunk_text"
                    }
                }
            },
            {"$limit": top_k_keyword},
            {
                "$addFields": {
                    "retrieval_method": "keyword",
                    "score": {"$meta": "searchScore"}
                }
            },
            {"$project": {"embedding": 0}}
        ]
        
        try:
            keyword_chunks = list(self.chunks_collection.aggregate(text_pipeline))
            for c in keyword_chunks:
                if c["chunk_id"] not in all_chunk_ids:
                    all_chunks.append(c)
                    all_chunk_ids.add(c["chunk_id"])
        except Exception as e:
            # Text index might not exist
            pass
        
        # Strategy 3: Graph Expansion
        if use_graph and all_chunk_ids:
            # Get entities from retrieved chunks
            entities = list(self.entities_collection.find(
                {"chunk_id": {"$in": list(all_chunk_ids)}}
            ))
            entity_texts = list(set(e["text"] for e in entities))
            
            # Find relations
            if entity_texts:
                relations = list(self.relations_collection.find({
                    "$or": [
                        {"head_text": {"$in": entity_texts}},
                        {"tail_text": {"$in": entity_texts}}
                    ]
                }).limit(30))
                
                # Get related chunk IDs
                related_entity_texts = set()
                for rel in relations:
                    related_entity_texts.add(rel["head_text"])
                    related_entity_texts.add(rel["tail_text"])
                
                if related_entity_texts:
                    related_entities = self.entities_collection.find(
                        {"text": {"$in": list(related_entity_texts)}}
                    )
                    related_chunk_ids = set()
                    for ent in related_entities:
                        if ent["chunk_id"] not in all_chunk_ids:
                            related_chunk_ids.add(ent["chunk_id"])
                    
                    # Retrieve graph-related chunks
                    if related_chunk_ids:
                        graph_chunks = list(self.chunks_collection.find(
                            {"chunk_id": {"$in": list(related_chunk_ids)[:max_graph_chunks]}},
                            {"embedding": 0}
                        ))
                        for c in graph_chunks:
                            c["retrieval_method"] = "graph"
                            if c["chunk_id"] not in all_chunk_ids:
                                all_chunks.append(c)
                                all_chunk_ids.add(c["chunk_id"])
        
        context_parts = []
        
        vector_results = [c for c in all_chunks if c.get("retrieval_method") == "vector"]
        keyword_results = [c for c in all_chunks if c.get("retrieval_method") == "keyword"]
        graph_results = [c for c in all_chunks if c.get("retrieval_method") == "graph"]
        
        if vector_results:
            context_parts.append("=== Semantically Similar Content ===")
            for c in vector_results:
                context_parts.append(f"[{c.get('uri', 'unknown')}]\n{c['chunk_text']}")
        
        if keyword_results:
            context_parts.append("\n=== Keyword-Matched Content ===")
            for c in keyword_results:
                context_parts.append(f"[{c.get('uri', 'unknown')}]\n{c['chunk_text']}")
        
        if relations:
            context_parts.append("\n=== Knowledge Graph Relationships ===")
            for rel in relations[:10]:
                context_parts.append(
                    f"• {rel['head_text']} → {rel['relation']} → {rel['tail_text']}"
                )
        
        if graph_results:
            context_parts.append("\n=== Graph-Connected Content ===")
            for c in graph_results:
                context_parts.append(f"[{c.get('uri', 'unknown')}]\n{c['chunk_text']}")
        
        context_text = "\n\n".join(context_parts)
        
        return RAGQueryResult(
            chunks=all_chunks,
            entities=entities,
            relations=relations,
            context_text=context_text,
            metadata={
                "method": "hybrid_rag",
                "vector_chunks": len(vector_results),
                "keyword_chunks": len(keyword_results),
                "graph_chunks": len(graph_results),
                "total_chunks": len(all_chunks),
                "entities_found": len(entities),
                "relations_found": len(relations),
            }
        )
    
    def close(self):
        """Close MongoDB connection"""
        self.client.close()
