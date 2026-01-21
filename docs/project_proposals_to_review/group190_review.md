# Group 190: Retrieval-Augmented Movie Recommendation System

## Summary
The project aims to solve the problem of shallow and surface-level movie recommendations that fail to capture deeper narrative and semantic connections between films. It does so by combining vector-based retrieval of rich movie descriptions with LLM-generated personalised recommendations and explanations. The system targets cinephiles, streaming platform users seeking context-aware recommendations, and developers/researchers who are interested in exploring the possibilities of RAG in Recommender Systems.

## Clarity & Coherence
- **Users & Goals:** \
Rating: 4/5.
Mostly clear — the target users (casual viewers and curators) are identified, and the high-level goals (better relevance + explainability) are stated.

- **Problem Definition:** \
Rating: 4/5.
Clear in intent (current recommenders lack grounded explanations), but missing measurable objectives and success criteria (e.g., precision@k, NDCG, user satisfaction targets).

- **Solution Concept:** \
Rating: 4/5.
The RAG pipeline is easy to understand, and the explanation of what the LLM adds (personalised natural-language reasoning) is clear. The task "connect" mentions data being combined to expose relationships, but doesn't clearly state what is actually retrieved or combined.

- **Technical Approach:** \
Rating: 5\5.
Identifies the 3 main challenges: retrieval quality, prompt engineering, and evaluation, and mentions the specific tools/models (FAISS/Milvus, MiniLM, LLaMA-3, LangChain). 

## Relevance & Appropriateness
Generative AI is used here not to replace recommendations, but to explain and contextualize them, with evidence. This is one of the strongest use cases for RAG. Traditional Recommenders lack this ability to reason a recommendation. The addition of RAG structure avoids hallucination by grounding generation in retrieved documents. 

## General Feedback
1. The system appears to be primarily retrieval-based, with the LLM acting mainly as a rationalizer rather than the core recommender.
2. Your evaluation compares recommendation accuracy, but not the quality of generated explanations. Consider measuring factuality through human verification. 

Overall, the proposal has a strong, relevant idea. Strengthen it by adding concrete datasets, evaluation metrics, retrieval architecture details, and explicit measures to prevent hallucination in generated explanations. Its effectiveness depends on careful prompt engineering, retrieval quality, and embedding selection. 
