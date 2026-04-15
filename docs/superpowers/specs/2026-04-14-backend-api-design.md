# Backend API — Design Spec

**Project:** Literary DNA  
**Date:** 2026-04-14  
**Status:** Approved  

---

## Overview

A Go HTTP server exposing a single endpoint that accepts a piece of user-submitted writing, finds the most stylistically similar authors in the corpus, and streams back both the match results and a Claude-generated explanation of why each author matches.

The server is stateless — no result storage, no user sessions. It shares the `Store` interface and Bedrock client code with the ingestion pipeline as part of the same `literary-dna` binary.

---

## Endpoint

```
POST /analyze
Content-Type: application/json
Accept: text/event-stream
```

**Request body:**
```json
{"text": "the user's writing..."}
```

**Validation:**
- `text` must be present and non-empty
- Minimum 20 whitespace-delimited words
- Returns `400 Bad Request` with `{"error": "..."}` if validation fails

**Failure responses:**
- `400` — invalid input (see above)
- `500` — upstream failure (Bedrock unreachable, store unavailable) before the stream opens

Once the SSE stream opens, errors are communicated via `event: error` rather than HTTP status codes, since headers have already been sent.

---

## SSE Stream Format

The response is a `text/event-stream`. Three event types are emitted in order:

### `event: matches`

Sent immediately after the similarity search completes (~500ms from request). Contains the top 3 author matches with scores and evidence passages.

```
event: matches
data: {"matches":[{"author_id":"hemingway","author_name":"Ernest Hemingway","score":0.84,"passages":["He was an old man who fished alone...","The hills across the valley of the Ebro..."]}]}
```

SSE `data:` fields must be single-line. The frontend JSON-parses the `data` value on receipt.

The frontend renders author cards and evidence passages on receipt of this event, before the explanation arrives.

### `event: explanation`

One or more chunks streamed as Claude generates the explanation. Each chunk contains a text delta.

```
event: explanation
data: {"delta": "Your writing shares Hemingway's clipped declarative sentences"}

event: explanation
data: {"delta": " and preference for concrete nouns over abstraction..."}
```

### `event: done`

Signals the stream is complete.

```
event: done
data: {}
```

### `event: error`

Sent if Claude fails after the `matches` event has already been emitted. The frontend degrades gracefully — shows match results without an explanation.

```
event: error
data: {"message": "explanation unavailable"}
```

---

## Request Flow

```
POST /analyze
    │
    ├─ Validate input (min 20 words)
    │
    ├─ Embed input text
    │   └─ Bedrock: amazon.titan-embed-text-v2:0 → 1024-dim vector
    │
    ├─ Similarity search
    │   └─ Store.Search(vec, topK=15) → top 15 passage matches
    │
    ├─ Aggregate by author
    │   └─ Group passages by author_id
    │      Score each author by highest single-passage similarity
    │      Take top 3 authors; keep best 2 passages per author
    │
    ├─ Emit: event: matches
    │
    ├─ Call Claude (streaming)
    │   └─ Bedrock Converse API (streaming)
    │      Model: anthropic.claude-3-5-sonnet-20241022-v2:0
    │      Input: user text + top 3 authors + their best 2 passages each
    │
    └─ Emit: event: explanation (one chunk per Claude token event)
       Emit: event: done
```

---

## Author Aggregation

After retrieving the top 15 passages from the vector store:

1. Group passages by `author_id`
2. Score each author by the similarity score of their highest-ranked passage
3. Select the top 3 authors by this score
4. For each selected author, take their top 2 passages (by similarity score) as evidence

This approach surfaces 3 distinct authors rather than returning 15 results that might all be from a single dominant author.

If fewer than 3 distinct authors appear in the top 15 results (e.g. the corpus has very few authors with strong signal for this input), the response returns however many authors are present. The frontend handles 1, 2, or 3 matches without special-casing.

---

## Claude Prompt

The prompt instructs Claude to produce a 2-3 sentence stylistic characterization per author. It emphasises:
- Specific stylistic features only: sentence structure, diction, rhythm, syntax, prose texture
- No mention of theme, subject matter, or biographical facts
- Concrete and specific, not generic ("uses short sentences" → "clipped declarative sentences with few subordinate clauses")

The prompt includes the user's full input text and the top 2 evidence passages per author. The explanation covers all 3 authors in a single response.

**Model:** `anthropic.claude-3-5-sonnet-20241022-v2:0` via Bedrock  
**Invocation:** Bedrock streaming Converse API  
**Max output tokens:** 512 (sufficient for 3 × 2-3 sentence characterizations)

---

## Store Interface

The server uses the same `Store` interface defined in the ingestion pipeline spec. Only `Search` is called at request time; `Upsert` and `DeleteByAuthor` are ingestion-only.

```go
type Store interface {
    Upsert(ctx context.Context, passages []Passage) error
    DeleteByAuthor(ctx context.Context, authorID string) error
    Search(ctx context.Context, vec []float32, topK int) ([]Match, error)
    Close() error
}
```

---

## Configuration

All configuration via environment variables:

| Variable | Description |
|----------|-------------|
| `AWS_REGION` | AWS region for Bedrock calls |
| `STORE_BACKEND` | `chroma` (local) or `pgvector` (prod) |
| `CHROMA_URL` | ChromaDB URL (local dev) |
| `DATABASE_URL` | PostgreSQL connection string (prod) |
| `CORS_ORIGIN` | Allowed origin for CORS (e.g. `http://localhost:3000`) |
| `PORT` | HTTP server port (default: `8080`) |

---

## Error Handling

| Failure point | Behaviour |
|--------------|-----------|
| Invalid input | `400` with JSON error body |
| Bedrock embed fails | `500` before stream opens |
| Store unavailable | `500` before stream opens |
| Claude fails after matches sent | `event: error` on stream; frontend shows matches only |
| Claude times out | Same as above; 30s timeout on Claude call |

---

## Implementation Notes

- Standard library only (`net/http`) — no external router dependency
- SSE flushing: handler checks that `ResponseWriter` implements `http.Flusher` and calls `Flush()` after each event
- CORS headers set on all responses to support browser-based frontend
- Request context cancellation propagated to Bedrock calls so abandoned requests don't waste API quota

---

## Testing

**Unit tests:**
- Aggregation logic: grouping, author scoring, top-N selection, 2-passage-per-author slicing
- Input validation edge cases (empty, short, whitespace-only)

**Integration tests:**
- `/analyze` handler tested with a mock `Store` and mock Bedrock client
- Verifies SSE event sequence: `matches` → one or more `explanation` → `done`
- Verifies `event: error` is emitted when Claude mock returns an error after search succeeds

**Not tested in CI:**
- Real Bedrock calls (requires AWS credentials and incurs cost)
- Real vector store (covered by ingestion pipeline integration tests)

---

## Out of Scope

- Authentication or rate limiting (v1 is public, corpus is read-only)
- Result caching (each request is independent)
- More than 3 author matches
- Non-streaming fallback
