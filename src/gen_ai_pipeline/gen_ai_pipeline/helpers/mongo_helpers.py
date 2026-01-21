
def get_vector_search_index_definition(embedding_dimensions: int = 1024) -> dict:
    """
    MongoDB Atlas Vector Search index definition for chunks collection.
    
    Create this index in MongoDB Atlas UI or via API:
    Index name: "vector_index"
    """
    return {
        "name": "vector_index",
        "type": "vectorSearch",
        "definition": {
            "fields": [
                {
                    "type": "vector",
                    "path": "embedding",
                    "numDimensions": embedding_dimensions,
                    "similarity": "cosine"
                },
                # Filter fields for hybrid search
                {
                    "type": "filter",
                    "path": "year"
                },
                {
                    "type": "filter", 
                    "path": "month"
                },
                {
                    "type": "filter",
                    "path": "host"
                }
            ]
        }
    }


def get_text_search_index_definition() -> dict:
    """
    MongoDB Atlas Search index for text/keyword search.
    Useful for hybrid retrieval combining vector + keyword search.
    
    Index name: "text_index"
    """
    return {
        "name": "text_index",
        "definition": {
            "mappings": {
                "dynamic": False,
                "fields": {
                    "chunk_text": {
                        "type": "string",
                        "analyzer": "lucene.standard"
                    },
                    "year": {"type": "number"},
                    "month": {"type": "number"},
                    "host": {"type": "string"}
                }
            }
        }
    }
