from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from dagster import (
    MonthlyPartitionsDefinition, 
    get_dagster_logger
)
from pymongo import MongoClient
import os
from openai import OpenAI
from google import genai

monthly_partitions = MonthlyPartitionsDefinition(start_date="2025-01-01")

@dataclass
class RAGQueryResult:
    """Result from RAG query"""
    chunks: List[Dict[str, Any]]
    entities: List[Dict[str, Any]]
    relations: List[Dict[str, Any]]
    context_text: str
    metadata: Dict[str, Any]
    answer: Optional[str] = None 


class RAGQueryEngine:
    """
    Query engine supporting Traditional RAG, GraphRAG, and Hybrid approaches.
    """
    
    def __init__(
        self,
        mongodb_uri: str,
        database_name: str = "news_rag",
        embedding_model = None,  # FlagEmbedding model
        llm_provider: str = "openai",  # or "anthropic"
        llm_model: str = "gpt-4-turbo-preview",
    ):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client[database_name]
        self.chunks_collection = self.db["chunks"]
        self.entities_collection = self.db["entities"]
        self.relations_collection = self.db["relations"]
        self.embedding_model = embedding_model
        self.relik_entity_model = None
        
        self.llm_provider = llm_provider
        self.llm_model = llm_model
        self._llm_client = None
    
    def _get_llm_client(self):
        """Lazy load LLM client"""
        if self._llm_client is None:
            if self.llm_provider == "openai":
                self._llm_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
            elif self.llm_provider == "gemini":
                genai.configure(api_key=os.environ.get("GOOGLE_API_KEY"))
                self._llm_client = genai.GenerativeModel(self.llm_model)            
        return self._llm_client
    
    def generate_answer(
        self,
        question: str,
        context: str,
        system_prompt: Optional[str] = None,
    ) -> str:
        """
        Generate answer using LLM with retrieved context.
        """
        if system_prompt is None:
            system_prompt = """You are a knowledgeable assistant that answers questions based on the provided context.

                Rules:
                1. Only use information from the provided context to answer
                2. If the context doesn't contain enough information, say so
                3. Cite sources when possible using the URLs provided
                4. Be concise but comprehensive
                5. If entity relationships are provided, use them to give a more complete answer"""

            prompt_template = """
                Context:
                {context}

                Question: {question}

                Answer:"""

        formatted_prompt = prompt_template.format(context=context, question=question)
        
        client = self._get_llm_client()
        
        if self.llm_provider == "openai":
            response = client.chat.completions.create(
                model=self.llm_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": formatted_prompt}
                ],
                temperature=0.7,
                max_tokens=1024,
            )
            return response.choices[0].message.content
        elif self.llm_provider == "gemini":
            full_prompt = f"{system_prompt}\n\n{formatted_prompt}"
            response = client.generate_content(
                full_prompt,
                generation_config={
                    "temperature": 0.7,
                    "max_output_tokens": 1024,
                }
            )
            return response.text
        raise ValueError(f"Unknown LLM provider: {self.llm_provider}")
    
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
        generate_answer: bool = True, 
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
        get_dagster_logger().debug(query_embedding)
        
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
        
        answer = None
        if generate_answer and context_text:
            answer = self.generate_answer(query, context_text)
        
        return RAGQueryResult(
            chunks=chunks,
            entities=[],
            relations=[],
            context_text=context_text,
            metadata={"method": "traditional_rag", "top_k": top_k},
            answer=answer,
        )
    
    # =========================================================================
    # GRAPH RAG: Entity and Relation-based Retrieval
    # =========================================================================
    def graph_rag(
        self,
        query: str,
        max_hops: int = 2,
        max_chunks: int = 10,
        generate_answer: bool = True,
    ) -> RAGQueryResult:
        """
        Pure GraphRAG: No vector search. Entity extraction → Graph traversal.
        
        Process:
        1. Extract entities from query using Relik (Sapienza model)
        2. Find those entities in MongoDB
        3. Use $graphLookup to traverse relationships
        4. Retrieve chunks containing connected entities
        """
        
        # Step 1: Extract entities from QUERY using Relik
        query_entities = self._extract_entities_from_query(query)
        query_entity_texts = [e["text"] for e in query_entities]
        
        if not query_entity_texts:
            # Fallback: no entities found in query
            return RAGQueryResult(
                chunks=[], entities=[], relations=[],
                context_text="No entities found in query.",
                metadata={"method": "graph_rag", "error": "no_query_entities"}
            )
        
        # Step 2: Find matching entities in our graph
        matched_entities = list(self.entities_collection.find(
            {"text": {"$in": query_entity_texts}}
        ))
        
        # Step 3: Graph traversal using $graphLookup
        pipeline = [
            # Start from relations where our entities are the head
            {"$match": {"head_text": {"$in": query_entity_texts}}},
            
            # Traverse the graph
            {"$graphLookup": {
                "from": "relations",
                "startWith": "$tail_text",
                "connectFromField": "tail_text",
                "connectToField": "head_text",
                "as": "connected_relations",
                "maxDepth": max_hops,
                "depthField": "hop_depth"
            }},
            
            {"$limit": 100}
        ]
        
        graph_results = list(self.relations_collection.aggregate(pipeline))
        
        # Step 4: Collect all connected entities and their chunk_ids
        connected_chunk_ids = set()
        all_relations = []
        
        for result in graph_results:
            connected_chunk_ids.add(result.get("chunk_id"))
            all_relations.append(result)
            
            for connected in result.get("connected_relations", []):
                connected_chunk_ids.add(connected.get("chunk_id"))
                all_relations.append(connected)
        
        # Also get chunks from directly matched entities
        for ent in matched_entities:
            connected_chunk_ids.add(ent["chunk_id"])
        
        connected_chunk_ids.discard(None)
        
        # Step 5: Retrieve the actual chunks
        chunks = list(self.chunks_collection.find(
            {"chunk_id": {"$in": list(connected_chunk_ids)[:max_chunks]}},
            {"embedding": 0}
        ))
        
        # Build context
        context_parts = []
        
        context_parts.append(f"=== Query Entities: {', '.join(query_entity_texts)} ===")
        
        if all_relations:
            context_parts.append("\n=== Graph Relationships ===")
            seen = set()
            for rel in all_relations[:15]:
                key = (rel["head_text"], rel["relation"], rel["tail_text"])
                if key not in seen:
                    seen.add(key)
                    hop = rel.get("hop_depth", 0)
                    context_parts.append(
                        f"[hop {hop}] {rel['head_text']} --[{rel['relation']}]--> {rel['tail_text']}"
                    )
        
        if chunks:
            context_parts.append("\n=== Connected Content ===")
            for c in chunks:
                context_parts.append(f"[{c.get('uri', 'unknown')}]\n{c['chunk_text']}")
        
        context_text = "\n\n".join(context_parts)
        
        answer = None
        if generate_answer and chunks:
            answer = self.generate_answer(query, context_text)
        
        return RAGQueryResult(
            chunks=chunks,
            entities=matched_entities,
            relations=all_relations,
            context_text=context_text,
            metadata={
                "method": "graph_rag",
                "query_entities": query_entity_texts,
                "matched_entities": len(matched_entities),
                "relations_traversed": len(all_relations),
            },
            answer=answer,
        )


    def _extract_entities_from_query(self, query: str) -> List[Dict]:
        """
        Extract entities from query using Relik (Sapienza model).
        This is the key difference from hybrid - we use NER on the query itself.
        """
        if self.relik_entity_model is None:
            # Lazy load
            from relik import Relik
            self.relik_entity_model = Relik.from_pretrained(
                "sapienzanlp/relik-entity-linking-small"
            )
        
        result = self.relik_entity_model(query)
        
        entities = []
        if hasattr(result, 'spans') and result.spans:
            for span in result.spans:
                entities.append({
                    "text": span.text,
                    "label": getattr(span, 'label', 'ENTITY'),
                    "wikipedia_id": getattr(span, 'id', None),
                })
        
        return entities

    def hybrid_rag(
        self,
        query: str,
        top_k_chunks: int = 3,
        max_hops: int = 2,
        max_related_chunks: int = 5,
        generate_answer: bool = True,
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
        
        answer = None
        if generate_answer and all_chunks:
            answer = self.generate_answer(query, context_text)
        
        return RAGQueryResult(
            chunks=all_chunks,
            entities=entities,
            relations=relations,
            context_text=context_text,
            metadata={
                "method": "hybrid_rag",
                "initial_chunks": len(initial_result.chunks),
                "related_chunks": len(related_chunks),
                "entities_found": len(entities),
                "relations_found": len(relations),
            },
            answer=answer,
        )
    
    def close(self):
        """Close MongoDB connection"""
        self.client.close()
