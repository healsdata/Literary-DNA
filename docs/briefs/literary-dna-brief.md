# Literary DNA — Product Brief

## What It Is

A web app that analyzes a piece of writing and identifies which classic authors from Project Gutenberg it most closely resembles — stylistically, not thematically. Paste in anything: a blog post, a cover letter, a README, a hot take. Get back a ranked list of your literary doppelgangers with the specific passages that match your voice as evidence.

The goal is something genuinely fun and shareable. "Your writing is 73% early Hemingway, 27% Jules Verne" is a thing you forward to a friend.

## What It's Not

- Not a plagiarism detector
- Not a topic or sentiment analyzer
- Not a "who wrote this?" forensics tool
- Not trying to index all of Gutenberg (see Corpus Strategy below)

## The Core Experience

1. User pastes text (a few sentences minimum, no hard max but sweet spot is a paragraph or two)
2. App embeds the input and runs similarity search against pre-indexed author passages
3. Returns top 2-3 author matches with:
   - A match percentage or score
   - 1-2 example passages from that author that drove the match
   - A brief, fun characterization of why ("You share Hemingway's clipped declarative sentences and preference for concrete nouns over abstraction")
4. Shareable result — either a static URL or a copyable summary

## Corpus Strategy

Rather than indexing all 70,000 Gutenberg texts, the corpus is intentionally curated:

- **~30-50 authors** with highly distinct, recognizable voices
- **~20-30 passages per author**, ~400-600 tokens each
- Passages selected for stylistic density — narrative prose preferred over heavy dialogue, chapter headers, or plot-summary sections
- Authors chosen to represent meaningful voice diversity across era, genre, and style
- The curation is a feature, not a limitation — the README will explain the editorial rationale

Author discovery and text retrieval via the Gutendex public API (`gutendex.com`), not manual downloads.

Target corpus size: ~1,500-2,000 total embeddings. Manageable, fast, cheap to host.

## Technical Approach

- **Embeddings:** Text embedding model via Amazon Bedrock (Titan Embeddings v2 or equivalent) — this is intentional, the project is partly a learning exercise in Bedrock tooling
- **Vector store:** pgvector (PostgreSQL extension) or a lightweight alternative like ChromaDB for local dev — decision TBD during implementation
- **Similarity:** Cosine similarity search against pre-indexed author passage embeddings
- **LLM layer:** Claude via Bedrock API — takes the top matching passages + input text and generates the human-readable "why you match" explanation
- **Backend:** Go — ingestion pipeline, embedding calls, similarity search, API
- **Frontend:** Keep it simple — likely a single-page React or plain HTML/JS interface. The input box and results are the whole product.
- **Ingestion pipeline:** Separate CLI/script that handles Gutendex API → text extraction → chunking → embedding → vector store load. Should be re-runnable and idempotent.

## Chunking Considerations

Chunking strategy is one of the interesting engineering decisions here — worth documenting as you go:
- Sentence-level chunks give style signal but lose rhythm
- Paragraph-level is probably the right default
- Fixed token windows with overlap as fallback
- Filtering out non-prose (dialogue-heavy, lists, legal boilerplate at start of Gutenberg files) matters for quality

## What Success Looks Like

- Someone pastes their own writing and gets a result that feels surprising but accurate
- The example passages feel like genuine evidence, not random quotes
- Fast enough to feel interactive (under 5 seconds end-to-end)
- Public GitHub repo with a README that explains the corpus curation decisions and what you learned about embeddings/RAG along the way

## What This Is Really For

Personal project to learn:
- Amazon Bedrock (embedding models + LLM API)
- Vector databases and similarity search in practice
- Chunking strategy tradeoffs
- Building a retrieval pipeline from an open dataset

Openly a learning exercise. The README should say so. That's part of the appeal.

## Out of Scope (for v1)

- User accounts or saved history
- Fine-tuning or training custom models
- Mobile-optimized UI
- More than English-language authors
- Real-time corpus updates
