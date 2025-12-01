# Group 22: Literature Assistant

## Summary

The proposal addresses the problem of managing and synthesising academic literature for students and researchers.  
The system aims to enrich metadata automatically, retrieve new relevant publications, and connect existing literature to the user's research draft.  
The target users are students and researchers who want to reduce manual work and improve the structure and relevance of their literature review.

## Clarity and Coherence

### Users and Goals – 5/5
The target users are clearly defined, and the goals such as learning, productivity, decision-making, and synthesis are relevant and coherent.  
The workflows match realistic usage of tools like Zotero or Google Scholar.  

### Problem Definition – 5/5
The three core challenges (incomplete metadata, difficulties in retrieving new publications, and lack of synthesis) are well explained and specific.  
The example query (find studies using methodology X with results similar to publication Z) clearly shows what current keyword-based tools cannot do.  
Overall, the problem statement is concise and convincing.

### Solution Concept – 4/5
The three main tasks (understand and enrich, retrieve and recommend, connect and synthesise) are intuitive and easy to follow.  
The examples you provide, such as “This paper may support the methodology section,” help show clearly how the system is expected to make recommendations. It could be useful to explain a bit more where the system is simply retrieving information from the documents and where the model starts adding its own interpretations or reasoning.

### Technical Approach – 4/5
The identified technical challenges, such as PDF parsing, semantic retrieval, and embedding-based connection discovery, are appropriate for the project scope.  
The plan to combine external API search with embedding-based re-ranking is reasonable for this type of system.  
It may help to briefly mention how the system handles noisy PDFs or parsing failures, and how it differentiates between user-generated text (drafts, notes) and academic sources when building embeddings.

## Relevance and Appropriateness

The use of generative AI is well justified in this project.  
LLMs are suitable for summarising papers, enriching metadata, and generating contextual explanations such as "why this paper is relevant for your project."  
RAG and vector search are also appropriate for retrieval and discovering connections between publications.  
One aspect that could be addressed more explicitly is hallucination: relevance summaries or suggestions should be grounded in the retrieved text to avoid generating claims that are not supported by the documents.

## General Feedback

1. **Clarify the boundary between retrieval and generation.**  
   You could specify which outputs come directly from the literature and which parts are model-generated interpretations.

2. **Mention how hallucinations will be reduced.**  
   Even a short note such as "the LLM will be constrained by retrieved context and will avoid generating statements without evidence from the documents" would strengthen the proposal.

3. **UI and integration.**  
   Since many researchers already use Zotero or Overleaf, it could be helpful to mention how the system might integrate with these tools in the future, even if this is outside the first prototype.
