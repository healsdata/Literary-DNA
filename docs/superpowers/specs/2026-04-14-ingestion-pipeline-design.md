# Ingestion Pipeline — Design Spec

**Project:** Literary DNA  
**Date:** 2026-04-14  
**Status:** Approved  

---

## Overview

A Go CLI for building and maintaining the Literary DNA corpus. It fetches raw texts from Project Gutenberg via the Gutendex API, chunks them into prose passages, scores and selects the best candidates automatically, embeds them via Amazon Bedrock, and loads them into the vector store.

The pipeline is a subcommand group on the main `literary-dna` binary. All stages are idempotent and restartable. The vector store backend is swappable: ChromaDB for local development, pgvector on Amazon RDS for production.

---

## Commands

```
literary-dna ingest fetch   # Download raw texts from Gutendex
literary-dna ingest chunk   # Chunk, score, and select passages
literary-dna ingest embed   # Embed selected passages via Bedrock
literary-dna ingest load    # Upsert embeddings into the vector store
```

Run stages individually or chain them. Each stage reads from and writes to well-defined files so any stage can be re-run independently.

---

## Data Flow

```
authors.yaml
    │
    ▼
[fetch] ──► corpus/raw/<author-id>.txt (one file per author)
    │
    ▼
[chunk] ──► corpus/candidates.json (scored, selected passages)
    │
    ▼
[embed] ──► corpus/embedded.json (passages + vectors)
    │
    ▼
[load]  ──► vector store (ChromaDB / pgvector)
```

---

## Author Config — `authors.yaml`

The corpus is defined in a single config file at the project root. Each author entry specifies which Gutenberg texts to draw from and how many passages to target.

```yaml
authors:
  - id: hemingway
    name: Ernest Hemingway
    gutenberg_ids: [67138, 4517]
    target_passages: 25
  - id: austen
    name: Jane Austen
    gutenberg_ids: [1342, 161]
    target_passages: 25
```

- `id` — slug used as the author identifier throughout the pipeline and in the vector store
- `gutenberg_ids` — one or more Gutenberg text IDs to draw passages from
- `target_passages` — how many passages to select for this author (default: 25)

The curation of which authors to include and which Gutenberg texts to use is an editorial decision documented separately in the project README.

---

## Stage 1: `fetch`

Reads `authors.yaml`, calls the Gutendex API (`gutendex.com/books/<id>`) to resolve download URLs, and saves raw `.txt` files to `corpus/raw/`.

**Idempotency:** Skips authors whose file already exists. Use `--force` to re-fetch.

**Output:** `corpus/raw/<author-id>.txt` — concatenated raw text from all listed Gutenberg IDs for that author, separated by a marker line.

---

## Stage 2: `chunk`

Splits each raw text into passage candidates, scores them for prose quality, and selects the top N per author.

### Splitting

Passages are split at double-newline paragraph boundaries. Chunks outside 200–700 tokens are dropped outright. Chunks over the maximum are split at sentence boundaries with 10% token overlap.

### Scoring

Each candidate is scored 0.0–1.0. Penalties are applied for:

| Signal | Penalty |
|--------|---------|
| High dialogue ratio (>30% of lines starting with `"` or `—`) | −0.3 |
| Short average sentence length (<8 words) | −0.2 |
| Gutenberg boilerplate markers (`Project Gutenberg`, `CHAPTER`, standalone roman numerals) | discard |
| Very low lexical diversity (type/token ratio <0.4) | −0.15 |

### Selection

After scoring, the top `target_passages` candidates per author are selected. The rest are discarded.

**Dry run:** Pass `--dry-run` to print selected passages to stdout without writing `candidates.json`. Use this to eyeball the selection before committing to embed.

**Output:** `corpus/candidates.json` — array of passage objects:

```json
{
  "id": "hemingway-0042",
  "author_id": "hemingway",
  "author_name": "Ernest Hemingway",
  "text": "...",
  "score": 0.87,
  "source_file": "corpus/raw/hemingway.txt",
  "char_offset": 14823
}
```

---

## Stage 3: `embed`

Reads `corpus/candidates.json` and sends each passage to the Amazon Bedrock Titan Embeddings V2 model. Writes the resulting vectors alongside passage metadata.

**Idempotency:** Tracks embedded passage IDs in `corpus/embedded.json`. Skips passages already present on re-run.

**Error handling:** Bedrock API errors are retried with exponential backoff (3 attempts, starting at 500ms). Unrecoverable failures log the passage ID and continue. A summary of failed passages is printed at the end.

**Output:** `corpus/embedded.json` — same shape as `candidates.json` with an added `"vector": [float32, ...]` field per passage.

---

## Stage 4: `load`

Reads `corpus/embedded.json` and upserts all passages into the configured vector store. Idempotent on passage ID — safe to run multiple times.

### Store Interface

Both backends implement a common Go interface:

```go
type Store interface {
    Upsert(ctx context.Context, passages []Passage) error
    Search(ctx context.Context, vec []float32, topK int) ([]Match, error)
    Close() error
}
```

Backend is selected via the `STORE_BACKEND` environment variable or `--store` flag:

| Value | Implementation | Use case |
|-------|---------------|----------|
| `chroma` | `ChromaStore` | Local development |
| `pgvector` | `PgvectorStore` | Production (RDS) |

Connection strings are provided via environment variables (`CHROMA_URL`, `DATABASE_URL`).

---

## Idempotency Summary

| Stage | Idempotency mechanism |
|-------|-----------------------|
| `fetch` | Skips if output file exists; `--force` to override |
| `chunk` | Deterministic — same input always produces same output |
| `embed` | Skips passage IDs already in `embedded.json` |
| `load` | Upsert by passage ID |

---

## Error Handling

- Stage-level failures (e.g. Gutendex API unreachable) exit non-zero with a clear message
- Per-passage failures during `embed` are logged and skipped; pipeline continues
- All stages validate their input files at startup and fail fast if expected files are missing

---

## Testing

**Unit tests** cover the chunking and scoring logic. `testdata/` contains fixture `.txt` files with known-good prose passages and known-bad content (dialogue-heavy, boilerplate) to verify scoring behaviour.

**Integration tests** exercise each `Store` implementation:
- `ChromaStore` — runs against a local ChromaDB instance in CI
- `PgvectorStore` — runs against pgvector via Docker Compose

**End-to-end test** uses a small 2-3 author subset defined in `testdata/authors-test.yaml` and runs all four pipeline stages against the local store backend.

---

## Out of Scope

- Real-time or scheduled corpus updates
- Non-English authors
- Parallel embedding (may add later if throughput is a bottleneck)
- Web-based review UI
