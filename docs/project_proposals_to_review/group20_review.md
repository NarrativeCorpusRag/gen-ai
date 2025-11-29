# Group 20: CookCompass
## Summary
The core objective match users inventory and dietary constraints with the unstructured content of the open web.
CookCompass solves the problem of inefficient and unsafe recipe discovery for individuals with dietary restrictions by utilizing a Retrieval-Augmented Generation (RAG) architecture to filter, verify, and synthesize recipes from the Food.
Targets users with hard constraints providing a conversational interface that transforms rigid database.

## Clarity & Coherence
The project shows who the software targets.
It's for students on a budget and people with dietary restrictions.
The target groups differ, but the plan doesn’t clearly show how the technical setup will better meet their needs than a standard database query.
Treating a medical condition like Celiac disease the same as a preference for quick meals is risky.
The plan talks about using a dataset, but it doesn't explain how the RAG architecture will manage hard constraints and soft preferences.
A simple vector retrieval might confuse "gluten-free" with "low gluten," which is a critical failure mode.

The problem definition also appears to be somewhat misaligned with the proposed solution.
The authors argue that web searching is frustrating due to SEO bloat, but their solution is to search a static CSV dataset.
If the data source is already clean and structured, the "SEO problem" is irrelevant to their technical architecture.

Also the goal of "eliminating guesswork" goes against the natural uncertainty of Large Language Models and what would be the key feature of the app.
This is true even acknowledging that without strict guardrails LLMs often hallucinate.
This includes the well-known "glue on pizza" incident.
The authors should address the lack of safety protocols in the plan.

The solution concept has as editing mistake, the descriptions for "Connect" and "Generate" are the same.
This repetition suggests the "Generate" phase hasn't been fully thought through.
It is unclear why an LLM is necessary for the straightforward task of retrieving a recipe.
If the goal is "generation," the system should create new recipes.
It should also explain why a substitution works.
It shouldn't just fetch existing text.

### Technical Approach and Challenges

The technical section relies heavily on buzzwords without defining the underlying logic.
The plan mentions "Sparse, Dense, and Hybrid Retrieval" as strategies to explore.
However, it doesn’t explain how these will work with the structured data in the `Food.com` dataset.
Ingredients are "atomic items."
If that understanding is correct, keyword matching (Sparse) or SQL-like filtering is better for allergies than semantic embedding (Dense).

There is also a missing baseline.
The evaluation plan focuses on Mean Average Precision (mAP), but it doesn't establish what they are comparing against.
A good technical evaluation should compare their GenAI method to a regular non-GenAI search, such as Elasticsearch.
This will show if the LLM really adds value instead of just increasing latency (which was one of the problems mentioned) and costs.
The plan states "reasonable latency" as a success criterion.
But, it doesn't explain what "reasonable" means in milliseconds or what hardware limits apply.
This lack of clarity makes it hard to measure success.

## Relevance & Appropriateness
The core question on this project is: Does this actually need GenAI?

In its current state, the proposal describes a database retrieval task masquerading as a generative AI project.
If a user requests "egg-free lava cake," a simple SQL query that filters out ingredients containing 'egg' is always accurate.
In contrast, an LLM retrieval might fail.
GenAI is useful when the system handles complex reasoning, like suggesting substitutes for eggs based on other ingredients.
But controlling that those ingredients are 'reasonable' will demand more than a dense retrieval.
Right now, though, the focus is mainly on retrieval.

Furthermore, introducing an LLM brings significant risks regarding "degenerate" recipes.
If the model gets creative without knowing about cooking, it may suggest swaps that don't work or taste bad.
The plan notes "hallucinations" as an issue, but the RAG solution doesn’t really address this.
In fact, RAG might even cause hallucinations if the retrieved context is unrelated or contradictory.

## General Feedback

To transform this from a generic search tool into a robust GenAI project, the group should narrow their focus significantly.
Instead of being a "CookCompass" for everyone, they should focus on one key area.
For example, they could choose "Allergy-Safe Adaptation" and design the whole process around that.
Concrete steps to strengthen the plan include:
- Define a Non-GenAI Baseline:
They must acknowledge that existing search technologies exist.
The project is only interesting if they show their RAG system works better than a standard Elasticsearch setup.
This is especially true when dealing with vague or complex natural language queries.
- Implement "Guardrails":
The plan needs a dedicated section on safety, specifically for allergies.
They would benefit exploring frameworks like NVIDIA NeMo Guardrails or Guardrails AI.
This way, the model won’t give a peanut recipe to someone allergic to nuts, no matter how relevant it seems.
- Clarify the "Generation" Aspect:
The value add of the LLM is not finding the recipe; it is adapting it. 
- The workflow must clearly separate two types of content:
    1. Static content:
the recipe from the database.
    2. Dynamic content:
the LLM's explanation on how to adjust that recipe for the user's needs.
How context is managed within chat
- Technology Stack: 
Move beyond concepts to concrete libraries.
Including tools like Dagster for orchestration, specific vector dbs, LLM framewors for data structuring will strengthen the proposal.