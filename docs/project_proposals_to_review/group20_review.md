# Group 20: CookCompass
## Summary
The core objective match users inventory and dietary constraints with the unstructured content of the open web.
CookCompass solves the problem of inefficient and unsafe recipe discovery for individuals with dietary restrictions by utilizing a Retrieval-Augmented Generation (RAG) architecture to filter, verify, and synthesize recipes from the Food.
Targets users with hard constraints providing a conversational interface that transforms rigid database.

## Clarity & Coherence
Rate the clarity of each section and identify gaps:
- Users & Goals: Are the target users and their workflows clearly defined?
There are two well define target groups: material constrains and dietary constrains
There is no clear understanding on how the RAG will, for example, differenciate or avice celiac versus lactose intolerance just accessing a database is... scarce, that is more a MCP than rag
there is no clear definition of goals what is 'quickly' what is 'seamless'
it is just 'retrieve' recipies from a database why not implement elastic search, I don't see were the need of 'generation' is here
not sure why the report mention that they would compete(?) with youtube
- Problem Definition: Is the core challenge well-articulated and specific?
there are 1 problem identified: need of recipies, and 2 causes:
a. seo bloated pages and
b. hallucination 

'a' cannot be solved by rag, b is not clear why would it be solved efficiently by rag
"Searching the web is frustrating" is not a technical problem

- Solution Concept: Is the high-level approach understandable without deep technical knowledge?
not well baked idea it has literally repeated sentence in 'connect' and 'generate'
no clear rag arquitecture, what will be retrieve why is an LLM needed. not all tasks demand a hammer, 
I guess LLM is used because of the conversation mode, but LLMs are stateless how are you going to implement the interface? how test different LLMs? do we need guardrails on LLMs (I remember something about pizza with glue because reddit)[https://www.businessinsider.com/google-ai-glue-pizza-i-tried-it-2024-5]?

- Technical Approach: Are the main technical challenges clearly identified?
mostly buzzwords
what's baseline ? I think compare non gen vs gen would probably be the best
how will allergy and material constraints be represented and matched?
how to validate
complain on latency but what's the target? target is conditional to what hardware?


## Relevance & Appropriateness
Does the use of generative AI make sense in the proposed project?
in a personal belief not at all LLM, as suggested, only introduce the risk of degenerate well retrieved recipies. It will require extensive pormpt tuning and guardrails
## General Feedback

How could this project be strengthened? (Pointers and references are welcome, even without concrete proposals – e.g., relevant papers, GitHub repositories, or existing products)

pick one primary user segment and design everything around their most critical tasks.
design a small set of representative query types (e.g. “allergy-avoidance”) and use them as running examples throughout the plan

mention concrete technologies libraries

clearly define what is static (I guess recipies) and what is dynamic, how does LLM affect dynamic content? how to avoid degeneracy

compare against non GenAI

clearly define safety policy fallbacks (and its updates)
