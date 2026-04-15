# Ingestion Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `literary-dna ingest` CLI — four subcommands (fetch, chunk, embed, load) that populate a vector store with scored prose passages from Project Gutenberg authors.

**Architecture:** Single Go binary with a `cmd/ingest/` package for subcommand wiring and `internal/` packages for pipeline logic, corpus types, store backends, and the Bedrock client. File-backed intermediate state between stages (candidates.json, embedded.json). Store backend is swappable via `STORE_BACKEND` env var.

**Tech Stack:** Go 1.22, Cobra CLI, gopkg.in/yaml.v3, AWS SDK for Go v2 (Bedrock), amikos-tech/chroma-go, jackc/pgx v5, pgvector/pgvector-go

---

## File Map

```
go.mod
main.go
authors.yaml
cmd/
  root.go                          # root cobra command + Execute()
  ingest/
    ingest.go                      # "ingest" subcommand group
    fetch.go                       # "ingest fetch" subcommand + flags
    chunk.go                       # "ingest chunk" subcommand + flags
    embed.go                       # "ingest embed" subcommand + flags
    load.go                        # "ingest load" subcommand + flags
internal/
  config/
    config.go                      # Author, Config types + LoadConfig()
    config_test.go
  corpus/
    types.go                       # Passage, EmbeddedPassage, Match types
    candidates.go                  # candidates.json read/write
    candidates_test.go
    embedded.go                    # embedded.json read/write
    embedded_test.go
  pipeline/
    fetch.go                       # Gutendex API client + download logic
    chunk.go                       # splitting, scoring, selection logic
    chunk_test.go
    embed.go                       # Bedrock embedding logic
    load.go                        # store load logic
    pipeline_test.go               # end-to-end integration test
  store/
    store.go                       # Store interface
    chroma.go                      # ChromaStore implementation
    chroma_test.go
    pgvector.go                    # PgvectorStore implementation
    pgvector_test.go
    factory.go                     # NewStore() based on env/flag
  bedrock/
    client.go                      # BedrockClient + Embedder interface
testdata/
  authors-test.yaml                # 2-author test fixture
  fixtures/
    prose-good.txt                 # clean narrative prose (scores ~0.9)
    prose-dialogue.txt             # >60% dialogue (scores ~0.7, penalised)
    prose-boilerplate.txt          # contains "Project Gutenberg" (discarded)
```

---

## Task 1: Project Scaffold

**Files:**
- Create: `go.mod`
- Create: `main.go`
- Create: `cmd/root.go`
- Create: `cmd/ingest/ingest.go`

- [ ] **Step 1: Initialise the Go module**

```bash
cd D:/Documents/source/literary-dna
go mod init literary-dna
```

Expected: `go.mod` created with `module literary-dna` and `go 1.22` (or current version).

- [ ] **Step 2: Install dependencies**

```bash
go get github.com/spf13/cobra@v1.8.1
go get gopkg.in/yaml.v3@v3.0.1
go get github.com/aws/aws-sdk-go-v2@v1.30.0
go get github.com/aws/aws-sdk-go-v2/config@v1.27.0
go get github.com/aws/aws-sdk-go-v2/service/bedrockruntime@v1.13.0
go get github.com/amikos-tech/chroma-go@v0.1.4
go get github.com/jackc/pgx/v5@v5.6.0
go get github.com/pgvector/pgvector-go@v0.2.1
```

Expected: `go.sum` populated, `go.mod` updated with `require` block.

- [ ] **Step 3: Create `cmd/root.go`**

```go
package cmd

import "github.com/spf13/cobra"

var rootCmd = &cobra.Command{
    Use:   "literary-dna",
    Short: "Find your literary doppelgangers",
}

func Root() *cobra.Command { return rootCmd }

func Execute() error { return rootCmd.Execute() }
```

- [ ] **Step 4: Create `cmd/ingest/ingest.go`**

```go
package ingest

import "github.com/spf13/cobra"

var Cmd = &cobra.Command{
    Use:   "ingest",
    Short: "Corpus ingestion pipeline commands",
}
```

- [ ] **Step 5: Create `main.go`**

```go
package main

import (
    "fmt"
    "os"

    "literary-dna/cmd"
    "literary-dna/cmd/ingest"
)

func main() {
    cmd.Root().AddCommand(ingest.Cmd)
    if err := cmd.Execute(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

- [ ] **Step 6: Verify the binary builds and runs**

```bash
go build ./...
./literary-dna --help
```

Expected output:
```
Find your literary doppelgangers

Usage:
  literary-dna [command]

Available Commands:
  ingest      Corpus ingestion pipeline commands
  ...
```

- [ ] **Step 7: Commit**

```bash
git add go.mod go.sum main.go cmd/
git commit -m "feat: scaffold literary-dna binary with ingest subcommand group"
```

---

## Task 2: Config Types and YAML Parsing

**Files:**
- Create: `internal/config/config.go`
- Create: `internal/config/config_test.go`
- Create: `testdata/authors-test.yaml`

- [ ] **Step 1: Create `testdata/authors-test.yaml`**

```yaml
authors:
  - id: hemingway
    name: Ernest Hemingway
    gutenberg_ids: [67138]
    target_passages: 5
  - id: austen
    name: Jane Austen
    gutenberg_ids: [1342]
    target_passages: 5
```

- [ ] **Step 2: Write the failing test**

```go
// internal/config/config_test.go
package config_test

import (
    "testing"

    "literary-dna/internal/config"
)

func TestLoadConfig(t *testing.T) {
    cfg, err := config.LoadConfig("../../testdata/authors-test.yaml")
    if err != nil {
        t.Fatalf("LoadConfig: %v", err)
    }
    if len(cfg.Authors) != 2 {
        t.Fatalf("want 2 authors, got %d", len(cfg.Authors))
    }
    a := cfg.Authors[0]
    if a.ID != "hemingway" {
        t.Errorf("want id=hemingway, got %q", a.ID)
    }
    if a.Name != "Ernest Hemingway" {
        t.Errorf("want name=Ernest Hemingway, got %q", a.Name)
    }
    if len(a.GutenbergIDs) != 1 || a.GutenbergIDs[0] != 67138 {
        t.Errorf("want gutenberg_ids=[67138], got %v", a.GutenbergIDs)
    }
    if a.TargetPassages != 5 {
        t.Errorf("want target_passages=5, got %d", a.TargetPassages)
    }
}

func TestLoadConfig_DefaultTargetPassages(t *testing.T) {
    cfg, err := config.LoadConfig("../../testdata/authors-test.yaml")
    if err != nil {
        t.Fatalf("LoadConfig: %v", err)
    }
    // Both test authors have explicit target_passages, so just verify non-zero
    for _, a := range cfg.Authors {
        if a.TargetPassages <= 0 {
            t.Errorf("author %q has non-positive target_passages: %d", a.ID, a.TargetPassages)
        }
    }
}

func TestLoadConfig_FileNotFound(t *testing.T) {
    _, err := config.LoadConfig("nonexistent.yaml")
    if err == nil {
        t.Error("want error for missing file, got nil")
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

```bash
go test ./internal/config/... -v
```

Expected: FAIL — `package config not found`

- [ ] **Step 4: Create `internal/config/config.go`**

```go
package config

import (
    "fmt"
    "os"

    "gopkg.in/yaml.v3"
)

const DefaultTargetPassages = 25

type Author struct {
    ID             string `yaml:"id"`
    Name           string `yaml:"name"`
    GutenbergIDs   []int  `yaml:"gutenberg_ids"`
    TargetPassages int    `yaml:"target_passages"`
}

type Config struct {
    Authors []Author `yaml:"authors"`
}

func LoadConfig(path string) (*Config, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, fmt.Errorf("read config %q: %w", path, err)
    }
    var cfg Config
    if err := yaml.Unmarshal(data, &cfg); err != nil {
        return nil, fmt.Errorf("parse config %q: %w", path, err)
    }
    for i := range cfg.Authors {
        if cfg.Authors[i].TargetPassages <= 0 {
            cfg.Authors[i].TargetPassages = DefaultTargetPassages
        }
    }
    return &cfg, nil
}
```

- [ ] **Step 5: Run test to verify it passes**

```bash
go test ./internal/config/... -v
```

Expected: PASS — all three tests green.

- [ ] **Step 6: Commit**

```bash
git add internal/config/ testdata/authors-test.yaml
git commit -m "feat: add config types and YAML loading"
```

---

## Task 3: Corpus Types and JSON I/O

**Files:**
- Create: `internal/corpus/types.go`
- Create: `internal/corpus/candidates.go`
- Create: `internal/corpus/candidates_test.go`
- Create: `internal/corpus/embedded.go`
- Create: `internal/corpus/embedded_test.go`

- [ ] **Step 1: Create `internal/corpus/types.go`**

```go
package corpus

// Passage is a scored prose passage selected during chunking.
// Written to corpus/candidates.json.
type Passage struct {
    ID          string  `json:"id"`
    AuthorID    string  `json:"author_id"`
    AuthorName  string  `json:"author_name"`
    Text        string  `json:"text"`
    Score       float64 `json:"score"`
    SourceFile  string  `json:"source_file"`
    GutenbergID int     `json:"gutenberg_id"`
    CharOffset  int     `json:"char_offset"`
}

// EmbeddedPassage is a Passage with its Bedrock embedding attached.
// Written to corpus/embedded.json.
type EmbeddedPassage struct {
    Passage
    Vector []float32 `json:"vector"`
}

// Match is returned by Store.Search — a passage that scored highly
// against the query vector.
type Match struct {
    PassageID  string
    AuthorID   string
    AuthorName string
    Text       string
    Score      float32
}
```

- [ ] **Step 2: Write failing tests for candidates I/O**

```go
// internal/corpus/candidates_test.go
package corpus_test

import (
    "os"
    "path/filepath"
    "testing"

    "literary-dna/internal/corpus"
)

func TestCandidatesRoundtrip(t *testing.T) {
    passages := []corpus.Passage{
        {
            ID:          "hemingway-0001",
            AuthorID:    "hemingway",
            AuthorName:  "Ernest Hemingway",
            Text:        "He was an old man.",
            Score:       0.87,
            SourceFile:  "corpus/raw/hemingway/67138.txt",
            GutenbergID: 67138,
            CharOffset:  100,
        },
    }

    dir := t.TempDir()
    path := filepath.Join(dir, "candidates.json")

    if err := corpus.WriteCandidates(path, passages); err != nil {
        t.Fatalf("WriteCandidates: %v", err)
    }

    got, err := corpus.ReadCandidates(path)
    if err != nil {
        t.Fatalf("ReadCandidates: %v", err)
    }
    if len(got) != 1 {
        t.Fatalf("want 1 passage, got %d", len(got))
    }
    if got[0].ID != "hemingway-0001" {
        t.Errorf("want id=hemingway-0001, got %q", got[0].ID)
    }
    if got[0].Score != 0.87 {
        t.Errorf("want score=0.87, got %f", got[0].Score)
    }
}

func TestReadCandidates_Missing(t *testing.T) {
    _, err := corpus.ReadCandidates("nonexistent.json")
    if err == nil {
        t.Error("want error for missing file, got nil")
    }
}
```

- [ ] **Step 3: Write failing test for embedded I/O**

```go
// internal/corpus/embedded_test.go
package corpus_test

import (
    "path/filepath"
    "testing"

    "literary-dna/internal/corpus"
)

func TestEmbeddedRoundtrip(t *testing.T) {
    passages := []corpus.EmbeddedPassage{
        {
            Passage: corpus.Passage{
                ID:       "hemingway-0001",
                AuthorID: "hemingway",
                Text:     "He was an old man.",
            },
            Vector: []float32{0.1, 0.2, 0.3},
        },
    }

    dir := t.TempDir()
    path := filepath.Join(dir, "embedded.json")

    if err := corpus.WriteEmbedded(path, passages); err != nil {
        t.Fatalf("WriteEmbedded: %v", err)
    }

    got, err := corpus.ReadEmbedded(path)
    if err != nil {
        t.Fatalf("ReadEmbedded: %v", err)
    }
    if len(got) != 1 {
        t.Fatalf("want 1 passage, got %d", len(got))
    }
    if len(got[0].Vector) != 3 || got[0].Vector[0] != 0.1 {
        t.Errorf("unexpected vector: %v", got[0].Vector)
    }
}

func TestEmbeddedIDSet(t *testing.T) {
    passages := []corpus.EmbeddedPassage{
        {Passage: corpus.Passage{ID: "a-001"}},
        {Passage: corpus.Passage{ID: "a-002"}},
    }
    dir := t.TempDir()
    path := filepath.Join(dir, "embedded.json")
    if err := corpus.WriteEmbedded(path, passages); err != nil {
        t.Fatalf("WriteEmbedded: %v", err)
    }
    ids, err := corpus.ReadEmbeddedIDs(path)
    if err != nil {
        t.Fatalf("ReadEmbeddedIDs: %v", err)
    }
    if !ids["a-001"] || !ids["a-002"] {
        t.Errorf("missing expected IDs in set: %v", ids)
    }
}
```

- [ ] **Step 4: Run tests to verify they fail**

```bash
go test ./internal/corpus/... -v
```

Expected: FAIL — package not found.

- [ ] **Step 5: Create `internal/corpus/candidates.go`**

```go
package corpus

import (
    "encoding/json"
    "fmt"
    "os"
)

func WriteCandidates(path string, passages []Passage) error {
    data, err := json.MarshalIndent(passages, "", "  ")
    if err != nil {
        return fmt.Errorf("marshal candidates: %w", err)
    }
    return os.WriteFile(path, data, 0644)
}

func ReadCandidates(path string) ([]Passage, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, fmt.Errorf("read candidates %q: %w", path, err)
    }
    var passages []Passage
    if err := json.Unmarshal(data, &passages); err != nil {
        return nil, fmt.Errorf("parse candidates %q: %w", path, err)
    }
    return passages, nil
}
```

- [ ] **Step 6: Create `internal/corpus/embedded.go`**

```go
package corpus

import (
    "encoding/json"
    "fmt"
    "os"
)

func WriteEmbedded(path string, passages []EmbeddedPassage) error {
    data, err := json.MarshalIndent(passages, "", "  ")
    if err != nil {
        return fmt.Errorf("marshal embedded: %w", err)
    }
    return os.WriteFile(path, data, 0644)
}

func ReadEmbedded(path string) ([]EmbeddedPassage, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, fmt.Errorf("read embedded %q: %w", path, err)
    }
    var passages []EmbeddedPassage
    if err := json.Unmarshal(data, &passages); err != nil {
        return nil, fmt.Errorf("parse embedded %q: %w", path, err)
    }
    return passages, nil
}

// ReadEmbeddedIDs returns the set of passage IDs already in embedded.json.
// Returns an empty (non-nil) set if the file does not exist yet.
func ReadEmbeddedIDs(path string) (map[string]bool, error) {
    ids := make(map[string]bool)
    passages, err := ReadEmbedded(path)
    if os.IsNotExist(err) {
        return ids, nil
    }
    if err != nil {
        return nil, err
    }
    for _, p := range passages {
        ids[p.ID] = true
    }
    return ids, nil
}
```

- [ ] **Step 7: Run tests to verify they pass**

```bash
go test ./internal/corpus/... -v
```

Expected: PASS — all five tests green.

- [ ] **Step 8: Commit**

```bash
git add internal/corpus/
git commit -m "feat: add corpus types and candidates/embedded JSON I/O"
```

---

## Task 4: Store Interface

**Files:**
- Create: `internal/store/store.go`

- [ ] **Step 1: Create `internal/store/store.go`**

```go
package store

import (
    "context"

    "literary-dna/internal/corpus"
)

// Store is implemented by ChromaStore (local dev) and PgvectorStore (prod).
type Store interface {
    // Upsert inserts or updates passages in the vector store.
    Upsert(ctx context.Context, passages []corpus.EmbeddedPassage) error

    // DeleteByAuthor removes all passages for the given author ID.
    // Called before Upsert when re-loading an author.
    DeleteByAuthor(ctx context.Context, authorID string) error

    // Search returns the topK most similar passages to vec.
    Search(ctx context.Context, vec []float32, topK int) ([]corpus.Match, error)

    // Close releases any held resources.
    Close() error
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/store/...
```

Expected: no output (success).

- [ ] **Step 3: Commit**

```bash
git add internal/store/store.go
git commit -m "feat: define Store interface"
```

---

## Task 5: Chunk Splitting

**Files:**
- Create: `internal/pipeline/chunk.go` (splitting functions only)
- Create: `internal/pipeline/chunk_test.go`
- Create: `testdata/fixtures/prose-good.txt`

- [ ] **Step 1: Create test fixtures**

`testdata/fixtures/prose-good.txt` — 300 words of clean narrative prose:

```
The old man had been watching the sea for forty years, long enough to know its moods the way a musician knows a difficult score. Every morning he walked down to the dock before the fishing boats went out, not to fish himself but to read the water. The colour of it, the pattern of the swells, the way the light moved on the surface — these things told him what the day would bring. He had learned this from his father and his father's father before him, men who had lived and died within sight of the same horizon.

On the morning in question the sea was the colour of old pewter, flat and still under a sky that promised nothing. The village behind him was just waking. He could hear the baker beginning his work, the sound of a shutter being pushed open, a dog somewhere in the lanes above the harbour. He did not turn around. Whatever happened behind him felt like a different world entirely, one that he had only a provisional relationship with. His real life had always been here, at the edge of things, where the land ran out of certainty.

He was thinking about his son, who had left for the city the previous autumn and had not written. This was not unusual. His son had always been better at leaving than at keeping in touch, better at new beginnings than at the maintenance of old attachments. The old man did not blame him for this. He understood it as something that had been in the family a long time, a restlessness that skipped generations the way certain physical traits did.
```

- [ ] **Step 2: Write failing tests for splitting**

```go
// internal/pipeline/chunk_test.go
package pipeline_test

import (
    "strings"
    "testing"

    "literary-dna/internal/pipeline"
)

func TestSplitParagraphs_Basic(t *testing.T) {
    // Two paragraphs separated by double newline, each 200+ words
    para := strings.Repeat("word ", 250) // 250 words
    text := para + "\n\n" + para
    chunks := pipeline.SplitParagraphs(text)
    if len(chunks) != 2 {
        t.Fatalf("want 2 chunks, got %d", len(chunks))
    }
}

func TestSplitParagraphs_DropShort(t *testing.T) {
    short := strings.Repeat("word ", 50)  // 50 words — too short
    long := strings.Repeat("word ", 250)  // 250 words — valid
    text := short + "\n\n" + long
    chunks := pipeline.SplitParagraphs(text)
    if len(chunks) != 1 {
        t.Fatalf("want 1 chunk (short dropped), got %d", len(chunks))
    }
}

func TestSplitParagraphs_DropEmpty(t *testing.T) {
    text := "\n\n" + strings.Repeat("word ", 250) + "\n\n"
    chunks := pipeline.SplitParagraphs(text)
    if len(chunks) != 1 {
        t.Fatalf("want 1 chunk, got %d", len(chunks))
    }
}

func TestSplitParagraphs_SplitOverlong(t *testing.T) {
    // 900 words in a single paragraph — should be split into 2 chunks
    text := strings.Repeat("word. ", 900) // 900 words, each a sentence
    chunks := pipeline.SplitParagraphs(text)
    if len(chunks) < 2 {
        t.Fatalf("want >=2 chunks for overlong paragraph, got %d", len(chunks))
    }
    for i, c := range chunks {
        wc := len(strings.Fields(c))
        if wc < 200 {
            t.Errorf("chunk %d has only %d words (minimum 200)", i, wc)
        }
    }
}

func TestWordCount(t *testing.T) {
    tests := []struct {
        text string
        want int
    }{
        {"hello world", 2},
        {"  hello   world  ", 2},
        {"", 0},
        {"one", 1},
    }
    for _, tt := range tests {
        got := pipeline.WordCount(tt.text)
        if got != tt.want {
            t.Errorf("WordCount(%q) = %d, want %d", tt.text, got, tt.want)
        }
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
go test ./internal/pipeline/... -v -run "TestSplit|TestWordCount"
```

Expected: FAIL — package not found.

- [ ] **Step 4: Create `internal/pipeline/chunk.go` with splitting functions**

```go
package pipeline

import (
    "strings"
)

const (
    minWords    = 200
    maxWords    = 700
    targetWords = 600 // split target for overlong chunks
    overlapWords = 60 // ~10% of targetWords
)

// WordCount returns the number of whitespace-delimited words in s.
func WordCount(s string) int {
    s = strings.TrimSpace(s)
    if s == "" {
        return 0
    }
    return len(strings.Fields(s))
}

// SplitParagraphs splits text at double-newline boundaries and filters
// chunks outside the [minWords, maxWords] range. Overlong chunks are
// split at sentence boundaries with overlapWords of overlap.
func SplitParagraphs(text string) []string {
    paragraphs := strings.Split(text, "\n\n")
    var result []string
    for _, p := range paragraphs {
        p = strings.TrimSpace(p)
        if p == "" {
            continue
        }
        wc := WordCount(p)
        if wc < minWords {
            continue
        }
        if wc <= maxWords {
            result = append(result, p)
        } else {
            result = append(result, splitWithOverlap(p)...)
        }
    }
    return result
}

// splitWithOverlap splits an overlong chunk into sub-chunks of ~targetWords
// with overlapWords of overlap between consecutive chunks. Each sub-chunk
// ends at a sentence boundary where possible.
func splitWithOverlap(text string) []string {
    words := strings.Fields(text)
    var chunks []string
    start := 0

    for start < len(words) {
        end := start + targetWords
        if end >= len(words) {
            chunk := strings.Join(words[start:], " ")
            if WordCount(chunk) >= minWords {
                chunks = append(chunks, chunk)
            }
            break
        }

        // Find sentence boundary near end by scanning backwards
        chunk := strings.Join(words[start:end], " ")
        lastBoundary := strings.LastIndexAny(chunk, ".!?")
        if lastBoundary > 0 && lastBoundary > len(chunk)-300 {
            chunk = strings.TrimSpace(chunk[:lastBoundary+1])
        }
        if WordCount(chunk) >= minWords {
            chunks = append(chunks, chunk)
        }

        // Next chunk starts with overlap
        overlapStart := end - overlapWords
        if overlapStart <= start {
            overlapStart = start + 1
        }
        start = overlapStart
    }
    return chunks
}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
go test ./internal/pipeline/... -v -run "TestSplit|TestWordCount"
```

Expected: PASS — all five tests green.

- [ ] **Step 6: Commit**

```bash
git add internal/pipeline/chunk.go internal/pipeline/chunk_test.go testdata/fixtures/prose-good.txt
git commit -m "feat: add paragraph splitting with overlong chunk handling"
```

---

## Task 6: Chunk Scoring

**Files:**
- Modify: `internal/pipeline/chunk.go` (add scoring functions)
- Modify: `internal/pipeline/chunk_test.go` (add scoring tests)
- Create: `testdata/fixtures/prose-dialogue.txt`
- Create: `testdata/fixtures/prose-boilerplate.txt`

- [ ] **Step 1: Create dialogue fixture**

`testdata/fixtures/prose-dialogue.txt` — a passage where >60% of lines start with `"` or `—`:

```
The two men met at the corner of the street.
"I thought you weren't coming," said the first.
"I almost didn't," said the other.
"What changed your mind?"
"Nothing changed my mind. I just came anyway."
"That's the same thing."
"Is it?"
"You're here, aren't you?"
"I'm here."
"Then something changed."
"Or nothing did and I'm still not sure why I came."
"You always were like this."
"Like what?"
"Complicated about simple things."
"Simple things aren't simple."
"Most of them are."
"Name one."
"Coming when you said you would."
"I said I might come. There's a difference."
"There isn't, really."
"There is to me."
The first man lit a cigarette and said nothing for a while.
"Fine," he said at last.
"Fine," said the other.
```

- [ ] **Step 2: Create boilerplate fixture**

`testdata/fixtures/prose-boilerplate.txt`:

```
Project Gutenberg EBook of The Old Man and the Sea

This eBook is for the use of anyone anywhere in the United States and
most other parts of the world at no cost and with almost no restrictions
whatsoever. You may copy it, give it away or re-use it under the terms
of the Project Gutenberg License included with this eBook or online at
www.gutenberg.org. If you are not located in the United States, you'll
have to check the laws of the country where you are located before using
this ebook. Title: The Old Man and the Sea Author: Ernest Hemingway
Release Date: May 1, 2021 Language: English
```

- [ ] **Step 3: Write failing scoring tests**

Add to `internal/pipeline/chunk_test.go`:

```go
func TestScorePassage_GoodProse(t *testing.T) {
    text := strings.Repeat("The old man walked slowly down the long road to the harbour. ", 20)
    score, discard := pipeline.ScorePassage(text)
    if discard {
        t.Error("want discard=false for good prose")
    }
    if score < 0.8 {
        t.Errorf("want score>=0.8 for good prose, got %f", score)
    }
}

func TestScorePassage_Boilerplate(t *testing.T) {
    text := "Project Gutenberg EBook of something. " + strings.Repeat("word ", 200)
    _, discard := pipeline.ScorePassage(text)
    if !discard {
        t.Error("want discard=true for boilerplate text")
    }
}

func TestScorePassage_Chapter(t *testing.T) {
    text := "CHAPTER IV\n" + strings.Repeat("word ", 200)
    _, discard := pipeline.ScorePassage(text)
    if !discard {
        t.Error("want discard=true for chapter heading")
    }
}

func TestScorePassage_RomanNumeral(t *testing.T) {
    text := "XIV.\n" + strings.Repeat("word ", 200)
    _, discard := pipeline.ScorePassage(text)
    if !discard {
        t.Error("want discard=true for standalone roman numeral")
    }
}

func TestScorePassage_HighDialogue(t *testing.T) {
    // >60% dialogue lines
    lines := make([]string, 10)
    for i := 0; i < 7; i++ {
        lines[i] = `"Hello," he said.`
    }
    for i := 7; i < 10; i++ {
        lines[i] = "He looked at the floor."
    }
    text := strings.Join(lines, "\n") + "\n" + strings.Repeat("The man thought carefully. ", 30)
    score, discard := pipeline.ScorePassage(text)
    if discard {
        t.Error("want discard=false for high-dialogue passage")
    }
    if score > 0.75 {
        t.Errorf("want score<=0.75 for high-dialogue passage, got %f", score)
    }
}

func TestScorePassage_MidDialogue(t *testing.T) {
    // 30-60% dialogue lines
    lines := make([]string, 10)
    for i := 0; i < 4; i++ {
        lines[i] = `"Yes," she said.`
    }
    for i := 4; i < 10; i++ {
        lines[i] = "She walked across the room and sat down by the window."
    }
    text := strings.Join(lines, "\n") + "\n" + strings.Repeat("The afternoon light was golden. ", 20)
    score, discard := pipeline.ScorePassage(text)
    if discard {
        t.Error("want discard=false for mid-dialogue passage")
    }
    if score > 0.9 || score < 0.8 {
        t.Errorf("want score in [0.8,0.9] for mid-dialogue passage, got %f", score)
    }
}

func TestScorePassage_ShortSentences(t *testing.T) {
    // avg sentence length < 8 words
    text := strings.Repeat("He ran. She fell. It hurt. They left. Rain came. Birds flew. Sun set. ", 30)
    score, discard := pipeline.ScorePassage(text)
    if discard {
        t.Error("want discard=false for short-sentence passage")
    }
    if score > 0.85 {
        t.Errorf("want score<=0.85 for short-sentence passage, got %f", score)
    }
}
```

- [ ] **Step 4: Run tests to verify they fail**

```bash
go test ./internal/pipeline/... -v -run "TestScore"
```

Expected: FAIL — `ScorePassage` undefined.

- [ ] **Step 5: Add scoring functions to `internal/pipeline/chunk.go`**

Add to the end of `chunk.go`:

```go
import (
    "regexp"
    "strings"
)

var boilerplatePatterns = []*regexp.Regexp{
    regexp.MustCompile(`(?i)project gutenberg`),
    regexp.MustCompile(`(?im)^\s*chapter\s+`),
    regexp.MustCompile(`(?m)^\s*[IVXLCDM]+\.?\s*$`),
}

// ScorePassage returns a prose quality score in [0.0, 1.0] and whether the
// passage should be discarded outright. A score > 0.5 is a viable candidate.
func ScorePassage(text string) (score float64, discard bool) {
    for _, re := range boilerplatePatterns {
        if re.MatchString(text) {
            return 0, true
        }
    }

    score = 1.0

    // Dialogue ratio penalty
    lines := strings.Split(strings.TrimSpace(text), "\n")
    dialogueCount := 0
    for _, line := range lines {
        trimmed := strings.TrimSpace(line)
        if strings.HasPrefix(trimmed, `"`) || strings.HasPrefix(trimmed, "—") {
            dialogueCount++
        }
    }
    if len(lines) > 0 {
        ratio := float64(dialogueCount) / float64(len(lines))
        switch {
        case ratio > 0.6:
            score -= 0.3
        case ratio > 0.3:
            score -= 0.15
        }
    }

    // Short average sentence length penalty
    if avgSentenceWords(text) < 8 {
        score -= 0.2
    }

    return score, false
}

// avgSentenceWords returns the average word count per sentence.
func avgSentenceWords(text string) float64 {
    re := regexp.MustCompile(`[.!?]+\s+`)
    sentences := re.Split(text, -1)
    if len(sentences) == 0 {
        return 0
    }
    total := 0
    for _, s := range sentences {
        total += len(strings.Fields(s))
    }
    return float64(total) / float64(len(sentences))
}
```

Note: move the `import` block to the top of the file, merging with the existing `"strings"` import.

- [ ] **Step 6: Run all chunk tests**

```bash
go test ./internal/pipeline/... -v -run "TestScore|TestSplit|TestWordCount"
```

Expected: PASS — all tests green.

- [ ] **Step 7: Commit**

```bash
git add internal/pipeline/chunk.go internal/pipeline/chunk_test.go testdata/fixtures/
git commit -m "feat: add passage scoring with boilerplate/dialogue/sentence-length signals"
```

---

## Task 7: Chunk Selection and `chunk` Subcommand

**Files:**
- Modify: `internal/pipeline/chunk.go` (add `ProcessAuthor`, `SelectTop`)
- Modify: `internal/pipeline/chunk_test.go` (add selection tests)
- Create: `cmd/ingest/chunk.go`

- [ ] **Step 1: Write failing selection tests**

Add to `internal/pipeline/chunk_test.go`:

```go
func TestSelectTop(t *testing.T) {
    passages := []pipeline.ScoredChunk{
        {Text: "a", Score: 0.9, Discard: false},
        {Text: "b", Score: 0.6, Discard: false},
        {Text: "c", Score: 0.3, Discard: false}, // below threshold (0.5)
        {Text: "d", Score: 0.0, Discard: true},   // discarded
        {Text: "e", Score: 0.8, Discard: false},
    }
    got := pipeline.SelectTop(passages, 2)
    if len(got) != 2 {
        t.Fatalf("want 2 selected, got %d", len(got))
    }
    if got[0].Score != 0.9 {
        t.Errorf("want first score=0.9, got %f", got[0].Score)
    }
    if got[1].Score != 0.8 {
        t.Errorf("want second score=0.8, got %f", got[1].Score)
    }
}

func TestSelectTop_FewerThanN(t *testing.T) {
    passages := []pipeline.ScoredChunk{
        {Text: "a", Score: 0.9, Discard: false},
    }
    got := pipeline.SelectTop(passages, 5)
    if len(got) != 1 {
        t.Fatalf("want 1 selected (only 1 available), got %d", len(got))
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./internal/pipeline/... -v -run "TestSelectTop"
```

Expected: FAIL — `ScoredChunk` and `SelectTop` undefined.

- [ ] **Step 3: Add selection types and functions to `internal/pipeline/chunk.go`**

Add to `chunk.go`:

```go
import (
    "fmt"
    "os"
    "sort"

    "literary-dna/internal/config"
    "literary-dna/internal/corpus"
)

// ScoredChunk is a passage candidate with its quality score.
type ScoredChunk struct {
    Text       string
    Score      float64
    Discard    bool
    CharOffset int
}

// SelectTop filters out discarded and below-threshold chunks, then returns
// the top n by score.
func SelectTop(chunks []ScoredChunk, n int) []ScoredChunk {
    const scoreThreshold = 0.5
    var viable []ScoredChunk
    for _, c := range chunks {
        if !c.Discard && c.Score >= scoreThreshold {
            viable = append(viable, c)
        }
    }
    sort.Slice(viable, func(i, j int) bool {
        return viable[i].Score > viable[j].Score
    })
    if n < len(viable) {
        return viable[:n]
    }
    return viable
}

// ProcessAuthor reads all raw text files for an author, splits and scores
// all chunks, and returns the top targetPassages passages as corpus.Passage
// objects ready to write to candidates.json.
func ProcessAuthor(author config.Author, corpusDir string) ([]corpus.Passage, error) {
    var allChunks []ScoredChunk
    var sourceMap []struct{ file string; id int; offset int }

    for _, gid := range author.GutenbergIDs {
        path := fmt.Sprintf("%s/raw/%s/%d.txt", corpusDir, author.ID, gid)
        data, err := os.ReadFile(path)
        if err != nil {
            return nil, fmt.Errorf("read %q: %w", path, err)
        }
        text := string(data)
        chunks := SplitParagraphs(text)
        offset := 0
        for _, chunk := range chunks {
            score, discard := ScorePassage(chunk)
            allChunks = append(allChunks, ScoredChunk{
                Text:       chunk,
                Score:      score,
                Discard:    discard,
                CharOffset: offset,
            })
            sourceMap = append(sourceMap, struct{ file string; id int; offset int }{
                file:   path,
                id:     gid,
                offset: offset,
            })
            offset += len(chunk)
        }
    }

    selected := SelectTop(allChunks, author.TargetPassages)
    passages := make([]corpus.Passage, 0, len(selected))
    for i, chunk := range selected {
        // Find the source info for this chunk by matching text
        src := struct{ file string; id int; offset int }{}
        for j, c := range allChunks {
            if c.Text == chunk.Text {
                src = sourceMap[j]
                break
            }
        }
        passages = append(passages, corpus.Passage{
            ID:          fmt.Sprintf("%s-%04d", author.ID, i+1),
            AuthorID:    author.ID,
            AuthorName:  author.Name,
            Text:        chunk.Text,
            Score:       chunk.Score,
            SourceFile:  src.file,
            GutenbergID: src.id,
            CharOffset:  src.offset,
        })
    }
    return passages, nil
}
```

- [ ] **Step 4: Run selection tests**

```bash
go test ./internal/pipeline/... -v -run "TestSelectTop"
```

Expected: PASS.

- [ ] **Step 5: Create `cmd/ingest/chunk.go`**

```go
package ingest

import (
    "fmt"
    "os"

    "github.com/spf13/cobra"

    "literary-dna/internal/config"
    "literary-dna/internal/corpus"
    "literary-dna/internal/pipeline"
)

var (
    chunkDryRun    bool
    chunkAuthorID  string
)

var chunkCmd = &cobra.Command{
    Use:   "chunk",
    Short: "Chunk, score, and select passages from raw texts",
    RunE:  runChunk,
}

func init() {
    chunkCmd.Flags().BoolVar(&chunkDryRun, "dry-run", false, "Print selected passages without writing candidates.json")
    chunkCmd.Flags().StringVar(&chunkAuthorID, "author", "", "Process only this author ID")
    Cmd.AddCommand(chunkCmd)
}

func runChunk(cmd *cobra.Command, args []string) error {
    cfg, err := config.LoadConfig("authors.yaml")
    if err != nil {
        return err
    }

    authors := cfg.Authors
    if chunkAuthorID != "" {
        authors = filterAuthors(authors, chunkAuthorID)
        if len(authors) == 0 {
            return fmt.Errorf("author %q not found in authors.yaml", chunkAuthorID)
        }
    }

    var allPassages []corpus.Passage
    for _, author := range authors {
        passages, err := pipeline.ProcessAuthor(author, "corpus")
        if err != nil {
            return fmt.Errorf("process %q: %w", author.ID, err)
        }
        fmt.Fprintf(os.Stderr, "  %s: selected %d/%d passages\n", author.ID, len(passages), author.TargetPassages)
        allPassages = append(allPassages, passages...)
    }

    if chunkDryRun {
        for _, p := range allPassages {
            fmt.Printf("=== %s (score: %.2f) ===\n%s\n\n", p.ID, p.Score, p.Text)
        }
        return nil
    }

    if err := os.MkdirAll("corpus", 0755); err != nil {
        return err
    }
    if err := corpus.WriteCandidates("corpus/candidates.json", allPassages); err != nil {
        return err
    }
    fmt.Fprintf(os.Stderr, "Wrote %d passages to corpus/candidates.json\n", len(allPassages))
    return nil
}

func filterAuthors(authors []config.Author, id string) []config.Author {
    for _, a := range authors {
        if a.ID == id {
            return []config.Author{a}
        }
    }
    return nil
}
```

- [ ] **Step 6: Verify the subcommand builds**

```bash
go build ./...
./literary-dna ingest chunk --help
```

Expected:
```
Chunk, score, and select passages from raw texts

Usage:
  literary-dna ingest chunk [flags]

Flags:
      --author string   Process only this author ID
      --dry-run         Print selected passages without writing candidates.json
```

- [ ] **Step 7: Run all pipeline tests**

```bash
go test ./internal/pipeline/... -v
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/pipeline/chunk.go internal/pipeline/chunk_test.go cmd/ingest/chunk.go
git commit -m "feat: add passage selection and chunk subcommand"
```

---

## Task 8: Gutendex Fetch Pipeline and `fetch` Subcommand

**Files:**
- Create: `internal/pipeline/fetch.go`
- Create: `cmd/ingest/fetch.go`

- [ ] **Step 1: Create `internal/pipeline/fetch.go`**

```go
package pipeline

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strings"
    "time"
)

const gutendexBase = "https://gutendex.com/books"

type gutendexBook struct {
    ID      int               `json:"id"`
    Title   string            `json:"title"`
    Formats map[string]string `json:"formats"`
}

// GutendexClient fetches book metadata and text from Gutendex, rate-limited
// to 1 request per second.
type GutendexClient struct {
    http    *http.Client
    limiter <-chan time.Time
}

func NewGutendexClient() *GutendexClient {
    return &GutendexClient{
        http:    &http.Client{Timeout: 30 * time.Second},
        limiter: time.Tick(time.Second), // 1 req/sec
    }
}

func (c *GutendexClient) fetchBook(ctx context.Context, bookID int) (*gutendexBook, error) {
    <-c.limiter
    url := fmt.Sprintf("%s/%d/", gutendexBase, bookID)
    req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
    if err != nil {
        return nil, err
    }
    resp, err := c.http.Do(req)
    if err != nil {
        return nil, fmt.Errorf("gutendex GET %d: %w", bookID, err)
    }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("gutendex %d: HTTP %d", bookID, resp.StatusCode)
    }
    var book gutendexBook
    if err := json.NewDecoder(resp.Body).Decode(&book); err != nil {
        return nil, fmt.Errorf("gutendex decode %d: %w", bookID, err)
    }
    return &book, nil
}

func (c *GutendexClient) fetchText(ctx context.Context, book *gutendexBook) (string, error) {
    // Find a plain text format URL
    var textURL string
    for mime, url := range book.Formats {
        if strings.HasPrefix(mime, "text/plain") {
            textURL = url
            break
        }
    }
    if textURL == "" {
        return "", fmt.Errorf("book %d has no text/plain format", book.ID)
    }

    <-c.limiter
    req, err := http.NewRequestWithContext(ctx, http.MethodGet, textURL, nil)
    if err != nil {
        return "", err
    }
    resp, err := c.http.Do(req)
    if err != nil {
        return "", fmt.Errorf("download %s: %w", textURL, err)
    }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusOK {
        return "", fmt.Errorf("download %s: HTTP %d", textURL, resp.StatusCode)
    }
    data, err := io.ReadAll(resp.Body)
    if err != nil {
        return "", fmt.Errorf("read body for book %d: %w", book.ID, err)
    }
    return string(data), nil
}

// FetchAndSave downloads book text to corpusDir/raw/<authorID>/<bookID>.txt.
// Skips the file if it already exists and force is false.
func FetchAndSave(ctx context.Context, client *GutendexClient, authorID string, bookID int, corpusDir string, force bool) error {
    outPath := fmt.Sprintf("%s/raw/%s/%d.txt", corpusDir, authorID, bookID)

    if !force {
        if _, err := os.Stat(outPath); err == nil {
            return nil // already exists
        }
    }

    book, err := client.fetchBook(ctx, bookID)
    if err != nil {
        return err
    }
    text, err := client.fetchText(ctx, book)
    if err != nil {
        return err
    }

    if err := os.MkdirAll(fmt.Sprintf("%s/raw/%s", corpusDir, authorID), 0755); err != nil {
        return err
    }
    return os.WriteFile(outPath, []byte(text), 0644)
}
```

- [ ] **Step 2: Create `cmd/ingest/fetch.go`**

```go
package ingest

import (
    "context"
    "fmt"
    "os"

    "github.com/spf13/cobra"

    "literary-dna/internal/config"
    "literary-dna/internal/pipeline"
)

var (
    fetchForce    bool
    fetchAuthorID string
)

var fetchCmd = &cobra.Command{
    Use:   "fetch",
    Short: "Download raw texts from Gutendex",
    RunE:  runFetch,
}

func init() {
    fetchCmd.Flags().BoolVar(&fetchForce, "force", false, "Re-fetch files that already exist")
    fetchCmd.Flags().StringVar(&fetchAuthorID, "author", "", "Fetch only this author ID")
    Cmd.AddCommand(fetchCmd)
}

func runFetch(cmd *cobra.Command, args []string) error {
    cfg, err := config.LoadConfig("authors.yaml")
    if err != nil {
        return err
    }

    authors := cfg.Authors
    if fetchAuthorID != "" {
        authors = filterAuthors(authors, fetchAuthorID)
        if len(authors) == 0 {
            return fmt.Errorf("author %q not found in authors.yaml", fetchAuthorID)
        }
    }

    ctx := context.Background()
    client := pipeline.NewGutendexClient()

    for _, author := range authors {
        for _, gid := range author.GutenbergIDs {
            fmt.Fprintf(os.Stderr, "Fetching %s / book %d...\n", author.ID, gid)
            if err := pipeline.FetchAndSave(ctx, client, author.ID, gid, "corpus", fetchForce); err != nil {
                fmt.Fprintf(os.Stderr, "  ERROR: %v\n", err)
                continue
            }
            fmt.Fprintf(os.Stderr, "  OK\n")
        }
    }
    return nil
}
```

- [ ] **Step 3: Verify the subcommand builds**

```bash
go build ./...
./literary-dna ingest fetch --help
```

Expected:
```
Download raw texts from Gutendex

Usage:
  literary-dna ingest fetch [flags]

Flags:
      --author string   Fetch only this author ID
      --force           Re-fetch files that already exist
```

- [ ] **Step 4: Commit**

```bash
git add internal/pipeline/fetch.go cmd/ingest/fetch.go
git commit -m "feat: add Gutendex fetch pipeline and fetch subcommand"
```

---

## Task 9: Bedrock Client and `embed` Subcommand

**Files:**
- Create: `internal/bedrock/client.go`
- Create: `internal/pipeline/embed.go`
- Create: `cmd/ingest/embed.go`

- [ ] **Step 1: Create `internal/bedrock/client.go`**

```go
package bedrock

import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
)

const TitanEmbedModelID = "amazon.titan-embed-text-v2:0"

// Embedder can embed a text string into a float32 vector.
type Embedder interface {
    Embed(ctx context.Context, text string) ([]float32, error)
}

type embedRequest struct {
    InputText  string `json:"inputText"`
    Dimensions int    `json:"dimensions"`
    Normalize  bool   `json:"normalize"`
}

type embedResponse struct {
    Embedding []float32 `json:"embedding"`
}

// Client wraps the Bedrock runtime for embedding calls.
type Client struct {
    runtime *bedrockruntime.Client
}

// New creates a Client using the default AWS credential chain.
// AWS_REGION must be set in the environment.
func New(ctx context.Context) (*Client, error) {
    cfg, err := config.LoadDefaultConfig(ctx)
    if err != nil {
        return nil, fmt.Errorf("load AWS config: %w", err)
    }
    return &Client{runtime: bedrockruntime.NewFromConfig(cfg)}, nil
}

// Embed calls Titan Embeddings V2 and returns a 1024-dimensional vector.
func (c *Client) Embed(ctx context.Context, text string) ([]float32, error) {
    body, err := json.Marshal(embedRequest{
        InputText:  text,
        Dimensions: 1024,
        Normalize:  true,
    })
    if err != nil {
        return nil, err
    }

    resp, err := c.runtime.InvokeModel(ctx, &bedrockruntime.InvokeModelInput{
        ModelId:     aws.String(TitanEmbedModelID),
        ContentType: aws.String("application/json"),
        Body:        body,
    })
    if err != nil {
        return nil, fmt.Errorf("bedrock InvokeModel: %w", err)
    }

    var result embedResponse
    if err := json.Unmarshal(resp.Body, &result); err != nil {
        return nil, fmt.Errorf("decode embedding response: %w", err)
    }
    return result.Embedding, nil
}
```

- [ ] **Step 2: Create `internal/pipeline/embed.go`**

```go
package pipeline

import (
    "context"
    "fmt"
    "os"
    "time"

    "literary-dna/internal/bedrock"
    "literary-dna/internal/corpus"
)

const (
    embedMaxRetries = 3
    embedRetryDelay = 500 * time.Millisecond
)

// EmbedPassages embeds each passage in passages that is not already in
// alreadyEmbedded, appends the results to existingEmbedded, and returns
// the full list. Failed passages are logged to stderr and skipped.
func EmbedPassages(
    ctx context.Context,
    embedder bedrock.Embedder,
    passages []corpus.Passage,
    alreadyEmbedded map[string]bool,
    existingEmbedded []corpus.EmbeddedPassage,
) ([]corpus.EmbeddedPassage, []string, error) {
    result := make([]corpus.EmbeddedPassage, len(existingEmbedded))
    copy(result, existingEmbedded)

    var failed []string

    for _, p := range passages {
        if alreadyEmbedded[p.ID] {
            continue
        }

        vec, err := embedWithRetry(ctx, embedder, p.Text)
        if err != nil {
            fmt.Fprintf(os.Stderr, "  SKIP %s: %v\n", p.ID, err)
            failed = append(failed, p.ID)
            continue
        }

        result = append(result, corpus.EmbeddedPassage{
            Passage: p,
            Vector:  vec,
        })
    }

    return result, failed, nil
}

func embedWithRetry(ctx context.Context, embedder bedrock.Embedder, text string) ([]float32, error) {
    delay := embedRetryDelay
    var lastErr error
    for attempt := 0; attempt < embedMaxRetries; attempt++ {
        if attempt > 0 {
            select {
            case <-ctx.Done():
                return nil, ctx.Err()
            case <-time.After(delay):
            }
            delay *= 2
        }
        vec, err := embedder.Embed(ctx, text)
        if err == nil {
            return vec, nil
        }
        lastErr = err
        fmt.Fprintf(os.Stderr, "  retry %d/%d: %v\n", attempt+1, embedMaxRetries, err)
    }
    return nil, lastErr
}
```

- [ ] **Step 3: Create `cmd/ingest/embed.go`**

```go
package ingest

import (
    "context"
    "fmt"
    "os"

    "github.com/spf13/cobra"

    "literary-dna/internal/bedrock"
    "literary-dna/internal/corpus"
    "literary-dna/internal/pipeline"
)

var embedAuthorID string

var embedCmd = &cobra.Command{
    Use:   "embed",
    Short: "Embed selected passages via Amazon Bedrock",
    RunE:  runEmbed,
}

func init() {
    embedCmd.Flags().StringVar(&embedAuthorID, "author", "", "Embed only passages for this author ID")
    Cmd.AddCommand(embedCmd)
}

func runEmbed(cmd *cobra.Command, args []string) error {
    ctx := context.Background()

    passages, err := corpus.ReadCandidates("corpus/candidates.json")
    if err != nil {
        return fmt.Errorf("read candidates: %w", err)
    }

    if embedAuthorID != "" {
        passages = filterPassages(passages, embedAuthorID)
    }

    alreadyEmbedded, err := corpus.ReadEmbeddedIDs("corpus/embedded.json")
    if err != nil {
        return fmt.Errorf("read embedded IDs: %w", err)
    }

    existing, err := corpus.ReadEmbedded("corpus/embedded.json")
    if os.IsNotExist(err) {
        existing = nil
    } else if err != nil {
        return fmt.Errorf("read embedded: %w", err)
    }

    client, err := bedrock.New(ctx)
    if err != nil {
        return fmt.Errorf("init bedrock: %w", err)
    }

    fmt.Fprintf(os.Stderr, "Embedding %d passages (%d already done)...\n",
        len(passages), len(alreadyEmbedded))

    result, failed, err := pipeline.EmbedPassages(ctx, client, passages, alreadyEmbedded, existing)
    if err != nil {
        return err
    }

    if err := corpus.WriteEmbedded("corpus/embedded.json", result); err != nil {
        return fmt.Errorf("write embedded: %w", err)
    }

    fmt.Fprintf(os.Stderr, "Wrote %d embedded passages to corpus/embedded.json\n", len(result))
    if len(failed) > 0 {
        fmt.Fprintf(os.Stderr, "Failed (%d): %v\n", len(failed), failed)
    }
    return nil
}

func filterPassages(passages []corpus.Passage, authorID string) []corpus.Passage {
    var out []corpus.Passage
    for _, p := range passages {
        if p.AuthorID == authorID {
            out = append(out, p)
        }
    }
    return out
}
```

- [ ] **Step 4: Verify the binary builds**

```bash
go build ./...
./literary-dna ingest embed --help
```

Expected:
```
Embed selected passages via Amazon Bedrock

Usage:
  literary-dna ingest embed [flags]

Flags:
      --author string   Embed only passages for this author ID
```

- [ ] **Step 5: Commit**

```bash
git add internal/bedrock/ internal/pipeline/embed.go cmd/ingest/embed.go
git commit -m "feat: add Bedrock embedding client and embed subcommand"
```

---

## Task 10: ChromaStore

**Files:**
- Create: `internal/store/chroma.go`
- Create: `internal/store/chroma_test.go`

- [ ] **Step 1: Write failing integration test (skipped if ChromaDB not available)**

```go
// internal/store/chroma_test.go
package store_test

import (
    "context"
    "os"
    "testing"

    "literary-dna/internal/corpus"
    "literary-dna/internal/store"
)

func TestChromaStore(t *testing.T) {
    chromaURL := os.Getenv("CHROMA_URL")
    if chromaURL == "" {
        t.Skip("CHROMA_URL not set — skipping ChromaStore integration test")
    }

    ctx := context.Background()
    s, err := store.NewChromaStore(ctx, chromaURL, "test-passages")
    if err != nil {
        t.Fatalf("NewChromaStore: %v", err)
    }
    defer s.Close()

    // Upsert a passage
    passages := []corpus.EmbeddedPassage{
        {
            Passage: corpus.Passage{
                ID:         "test-0001",
                AuthorID:   "hemingway",
                AuthorName: "Ernest Hemingway",
                Text:       "He was an old man.",
            },
            Vector: make([]float32, 1024), // zero vector for testing
        },
    }
    if err := s.Upsert(ctx, passages); err != nil {
        t.Fatalf("Upsert: %v", err)
    }

    // Search
    matches, err := s.Search(ctx, make([]float32, 1024), 5)
    if err != nil {
        t.Fatalf("Search: %v", err)
    }
    if len(matches) == 0 {
        t.Error("want at least 1 match, got 0")
    }

    // DeleteByAuthor
    if err := s.DeleteByAuthor(ctx, "hemingway"); err != nil {
        t.Fatalf("DeleteByAuthor: %v", err)
    }

    // Confirm deletion
    matches, err = s.Search(ctx, make([]float32, 1024), 5)
    if err != nil {
        t.Fatalf("Search after delete: %v", err)
    }
    if len(matches) != 0 {
        t.Errorf("want 0 matches after delete, got %d", len(matches))
    }
}
```

- [ ] **Step 2: Run to verify skip behaviour**

```bash
go test ./internal/store/... -v -run TestChromaStore
```

Expected: `--- SKIP: TestChromaStore (CHROMA_URL not set — skipping ChromaStore integration test)`

- [ ] **Step 3: Create `internal/store/chroma.go`**

```go
package store

import (
    "context"
    "fmt"

    chroma "github.com/amikos-tech/chroma-go"
    "github.com/amikos-tech/chroma-go/types"

    "literary-dna/internal/corpus"
)

// ChromaStore implements Store using a ChromaDB collection.
type ChromaStore struct {
    client     *chroma.Client
    collection *chroma.Collection
}

// NewChromaStore connects to ChromaDB at url and gets or creates a collection
// with the given name.
func NewChromaStore(ctx context.Context, url, collectionName string) (*ChromaStore, error) {
    client, err := chroma.NewClient(chroma.WithBasePath(url))
    if err != nil {
        return nil, fmt.Errorf("chroma client: %w", err)
    }

    col, err := client.GetOrCreateCollection(ctx, collectionName, map[string]interface{}{}, nil, types.L2)
    if err != nil {
        return nil, fmt.Errorf("get/create collection %q: %w", collectionName, err)
    }

    return &ChromaStore{client: client, collection: col}, nil
}

func (s *ChromaStore) Upsert(ctx context.Context, passages []corpus.EmbeddedPassage) error {
    if len(passages) == 0 {
        return nil
    }

    ids := make([]string, len(passages))
    embeddings := make([][]float32, len(passages))
    documents := make([]string, len(passages))
    metadatas := make([]map[string]interface{}, len(passages))

    for i, p := range passages {
        ids[i] = p.ID
        embeddings[i] = p.Vector
        documents[i] = p.Text
        metadatas[i] = map[string]interface{}{
            "author_id":   p.AuthorID,
            "author_name": p.AuthorName,
        }
    }

    _, err := s.collection.Upsert(ctx, embeddings, metadatas, documents, ids)
    return err
}

func (s *ChromaStore) DeleteByAuthor(ctx context.Context, authorID string) error {
    _, err := s.collection.Delete(ctx, nil, map[interface{}]interface{}{
        "author_id": map[string]interface{}{"$eq": authorID},
    }, nil)
    return err
}

func (s *ChromaStore) Search(ctx context.Context, vec []float32, topK int) ([]corpus.Match, error) {
    results, err := s.collection.QueryWithOptions(ctx,
        types.WithQueryVectors([][]float32{vec}),
        types.WithNResults(int32(topK)),
        types.WithInclude(types.IDocuments, types.IMetadatas, types.IDistances),
    )
    if err != nil {
        return nil, fmt.Errorf("chroma query: %w", err)
    }

    if len(results.Documents) == 0 || len(results.Documents[0]) == 0 {
        return nil, nil
    }

    docs := results.Documents[0]
    metas := results.Metadatas[0]
    dists := results.Distances[0]
    ids := results.Ids[0]

    matches := make([]corpus.Match, len(docs))
    for i, doc := range docs {
        matches[i] = corpus.Match{
            PassageID:  ids[i],
            AuthorID:   fmt.Sprint(metas[i]["author_id"]),
            AuthorName: fmt.Sprint(metas[i]["author_name"]),
            Text:       doc,
            Score:      1 - dists[i], // L2 distance → similarity
        }
    }
    return matches, nil
}

func (s *ChromaStore) Close() error { return nil }
```

- [ ] **Step 4: Build to verify no compile errors**

```bash
go build ./internal/store/...
```

Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add internal/store/chroma.go internal/store/chroma_test.go
git commit -m "feat: add ChromaStore implementation"
```

---

## Task 11: PgvectorStore and Store Factory

**Files:**
- Create: `internal/store/pgvector.go`
- Create: `internal/store/pgvector_test.go`
- Create: `internal/store/factory.go`

- [ ] **Step 1: Write failing integration test (skipped if DATABASE_URL not set)**

```go
// internal/store/pgvector_test.go
package store_test

import (
    "context"
    "os"
    "testing"

    "literary-dna/internal/corpus"
    "literary-dna/internal/store"
)

func TestPgvectorStore(t *testing.T) {
    dsn := os.Getenv("DATABASE_URL")
    if dsn == "" {
        t.Skip("DATABASE_URL not set — skipping PgvectorStore integration test")
    }

    ctx := context.Background()
    s, err := store.NewPgvectorStore(ctx, dsn)
    if err != nil {
        t.Fatalf("NewPgvectorStore: %v", err)
    }
    defer s.Close()

    // Upsert
    passages := []corpus.EmbeddedPassage{
        {
            Passage: corpus.Passage{
                ID:         "pgtest-0001",
                AuthorID:   "austen",
                AuthorName: "Jane Austen",
                Text:       "It is a truth universally acknowledged.",
            },
            Vector: make([]float32, 1024),
        },
    }
    if err := s.Upsert(ctx, passages); err != nil {
        t.Fatalf("Upsert: %v", err)
    }

    // Search
    matches, err := s.Search(ctx, make([]float32, 1024), 5)
    if err != nil {
        t.Fatalf("Search: %v", err)
    }
    if len(matches) == 0 {
        t.Error("want at least 1 match, got 0")
    }

    // DeleteByAuthor
    if err := s.DeleteByAuthor(ctx, "austen"); err != nil {
        t.Fatalf("DeleteByAuthor: %v", err)
    }
}
```

- [ ] **Step 2: Run to verify skip**

```bash
go test ./internal/store/... -v -run TestPgvectorStore
```

Expected: `--- SKIP: TestPgvectorStore (DATABASE_URL not set ...)`

- [ ] **Step 3: Create `internal/store/pgvector.go`**

```go
package store

import (
    "context"
    "fmt"

    "github.com/jackc/pgx/v5"
    "github.com/jackc/pgx/v5/pgxpool"
    pgvector "github.com/pgvector/pgvector-go"
    pgxvector "github.com/pgvector/pgvector-go/pgx"

    "literary-dna/internal/corpus"
)

// PgvectorStore implements Store using PostgreSQL + pgvector.
type PgvectorStore struct {
    pool *pgxpool.Pool
}

// NewPgvectorStore connects to Postgres and ensures the passages table and
// pgvector extension exist.
func NewPgvectorStore(ctx context.Context, dsn string) (*PgvectorStore, error) {
    cfg, err := pgxpool.ParseConfig(dsn)
    if err != nil {
        return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
    }
    cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
        return pgxvector.RegisterTypes(ctx, conn)
    }

    pool, err := pgxpool.NewWithConfig(ctx, cfg)
    if err != nil {
        return nil, fmt.Errorf("connect to postgres: %w", err)
    }

    if err := migrate(ctx, pool); err != nil {
        pool.Close()
        return nil, fmt.Errorf("migrate: %w", err)
    }

    return &PgvectorStore{pool: pool}, nil
}

func migrate(ctx context.Context, pool *pgxpool.Pool) error {
    _, err := pool.Exec(ctx, `
        CREATE EXTENSION IF NOT EXISTS vector;
        CREATE TABLE IF NOT EXISTS passages (
            id          TEXT PRIMARY KEY,
            author_id   TEXT NOT NULL,
            author_name TEXT NOT NULL,
            text        TEXT NOT NULL,
            embedding   vector(1024) NOT NULL
        );
        CREATE INDEX IF NOT EXISTS passages_embedding_idx
            ON passages USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 100);
    `)
    return err
}

func (s *PgvectorStore) Upsert(ctx context.Context, passages []corpus.EmbeddedPassage) error {
    for _, p := range passages {
        _, err := s.pool.Exec(ctx, `
            INSERT INTO passages (id, author_id, author_name, text, embedding)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (id) DO UPDATE
                SET author_id   = EXCLUDED.author_id,
                    author_name = EXCLUDED.author_name,
                    text        = EXCLUDED.text,
                    embedding   = EXCLUDED.embedding
        `, p.ID, p.AuthorID, p.AuthorName, p.Text, pgvector.NewVector(p.Vector))
        if err != nil {
            return fmt.Errorf("upsert %s: %w", p.ID, err)
        }
    }
    return nil
}

func (s *PgvectorStore) DeleteByAuthor(ctx context.Context, authorID string) error {
    _, err := s.pool.Exec(ctx, `DELETE FROM passages WHERE author_id = $1`, authorID)
    return err
}

func (s *PgvectorStore) Search(ctx context.Context, vec []float32, topK int) ([]corpus.Match, error) {
    rows, err := s.pool.Query(ctx, `
        SELECT id, author_id, author_name, text,
               1 - (embedding <=> $1) AS score
        FROM passages
        ORDER BY embedding <=> $1
        LIMIT $2
    `, pgvector.NewVector(vec), topK)
    if err != nil {
        return nil, fmt.Errorf("search: %w", err)
    }
    defer rows.Close()

    var matches []corpus.Match
    for rows.Next() {
        var m corpus.Match
        if err := rows.Scan(&m.PassageID, &m.AuthorID, &m.AuthorName, &m.Text, &m.Score); err != nil {
            return nil, err
        }
        matches = append(matches, m)
    }
    return matches, rows.Err()
}

func (s *PgvectorStore) Close() error {
    s.pool.Close()
    return nil
}
```

- [ ] **Step 4: Create `internal/store/factory.go`**

```go
package store

import (
    "context"
    "fmt"
    "os"
)

// NewStore creates a Store from the STORE_BACKEND environment variable
// (or the provided override). Valid values: "chroma", "pgvector".
func NewStore(ctx context.Context, backendOverride string) (Store, error) {
    backend := backendOverride
    if backend == "" {
        backend = os.Getenv("STORE_BACKEND")
    }
    if backend == "" {
        backend = "chroma" // default for local dev
    }

    switch backend {
    case "chroma":
        url := os.Getenv("CHROMA_URL")
        if url == "" {
            url = "http://localhost:8000"
        }
        return NewChromaStore(ctx, url, "passages")
    case "pgvector":
        dsn := os.Getenv("DATABASE_URL")
        if dsn == "" {
            return nil, fmt.Errorf("DATABASE_URL must be set when STORE_BACKEND=pgvector")
        }
        return NewPgvectorStore(ctx, dsn)
    default:
        return nil, fmt.Errorf("unknown STORE_BACKEND %q (want: chroma, pgvector)", backend)
    }
}
```

- [ ] **Step 5: Build to verify**

```bash
go build ./internal/store/...
```

Expected: no output.

- [ ] **Step 6: Commit**

```bash
git add internal/store/pgvector.go internal/store/pgvector_test.go internal/store/factory.go
git commit -m "feat: add PgvectorStore and store factory"
```

---

## Task 12: Load Pipeline and `load` Subcommand

**Files:**
- Create: `internal/pipeline/load.go`
- Create: `cmd/ingest/load.go`

- [ ] **Step 1: Create `internal/pipeline/load.go`**

```go
package pipeline

import (
    "context"
    "fmt"
    "os"

    "literary-dna/internal/corpus"
    "literary-dna/internal/store"
)

// LoadToStore deletes all existing passages for each author in passages,
// then upserts the new set. This prevents stale passages from accumulating
// after re-curation.
func LoadToStore(ctx context.Context, s store.Store, passages []corpus.EmbeddedPassage) error {
    // Group passages by author
    byAuthor := make(map[string][]corpus.EmbeddedPassage)
    for _, p := range passages {
        byAuthor[p.AuthorID] = append(byAuthor[p.AuthorID], p)
    }

    for authorID, authorPassages := range byAuthor {
        fmt.Fprintf(os.Stderr, "  Loading %s (%d passages)...\n", authorID, len(authorPassages))
        if err := s.DeleteByAuthor(ctx, authorID); err != nil {
            return fmt.Errorf("delete %s: %w", authorID, err)
        }
        if err := s.Upsert(ctx, authorPassages); err != nil {
            return fmt.Errorf("upsert %s: %w", authorID, err)
        }
    }
    return nil
}
```

- [ ] **Step 2: Create `cmd/ingest/load.go`**

```go
package ingest

import (
    "context"
    "fmt"
    "os"

    "github.com/spf13/cobra"

    "literary-dna/internal/corpus"
    "literary-dna/internal/pipeline"
    "literary-dna/internal/store"
)

var (
    loadAuthorID string
    loadBackend  string
)

var loadCmd = &cobra.Command{
    Use:   "load",
    Short: "Load embedded passages into the vector store",
    RunE:  runLoad,
}

func init() {
    loadCmd.Flags().StringVar(&loadAuthorID, "author", "", "Load only passages for this author ID")
    loadCmd.Flags().StringVar(&loadBackend, "store", "", "Store backend: chroma or pgvector (overrides STORE_BACKEND env)")
    Cmd.AddCommand(loadCmd)
}

func runLoad(cmd *cobra.Command, args []string) error {
    ctx := context.Background()

    passages, err := corpus.ReadEmbedded("corpus/embedded.json")
    if err != nil {
        return fmt.Errorf("read embedded: %w", err)
    }

    if loadAuthorID != "" {
        var filtered []corpus.EmbeddedPassage
        for _, p := range passages {
            if p.AuthorID == loadAuthorID {
                filtered = append(filtered, p)
            }
        }
        passages = filtered
    }

    s, err := store.NewStore(ctx, loadBackend)
    if err != nil {
        return fmt.Errorf("init store: %w", err)
    }
    defer s.Close()

    fmt.Fprintf(os.Stderr, "Loading %d passages into store...\n", len(passages))
    if err := pipeline.LoadToStore(ctx, s, passages); err != nil {
        return err
    }
    fmt.Fprintf(os.Stderr, "Done.\n")
    return nil
}
```

- [ ] **Step 3: Verify the binary builds with all four subcommands**

```bash
go build ./...
./literary-dna ingest --help
```

Expected:
```
Corpus ingestion pipeline commands

Usage:
  literary-dna ingest [command]

Available Commands:
  chunk       Chunk, score, and select passages from raw texts
  embed       Embed selected passages via Amazon Bedrock
  fetch       Download raw texts from Gutendex
  load        Load embedded passages into the vector store
```

- [ ] **Step 4: Commit**

```bash
git add internal/pipeline/load.go cmd/ingest/load.go
git commit -m "feat: add load pipeline and load subcommand"
```

---

## Task 13: End-to-End Integration Test

**Files:**
- Create: `internal/pipeline/pipeline_test.go`

This test runs the full pipeline (chunk only — fetch and embed require external services) against the test fixtures using an in-memory mock store.

- [ ] **Step 1: Create `internal/pipeline/pipeline_test.go`**

```go
package pipeline_test

import (
    "context"
    "os"
    "path/filepath"
    "testing"

    "literary-dna/internal/config"
    "literary-dna/internal/corpus"
    "literary-dna/internal/pipeline"
)

// mockStore implements store.Store for testing without a real database.
type mockStore struct {
    passages []corpus.EmbeddedPassage
}

func (m *mockStore) Upsert(_ context.Context, passages []corpus.EmbeddedPassage) error {
    m.passages = append(m.passages, passages...)
    return nil
}

func (m *mockStore) DeleteByAuthor(_ context.Context, authorID string) error {
    var remaining []corpus.EmbeddedPassage
    for _, p := range m.passages {
        if p.AuthorID != authorID {
            remaining = append(remaining, p)
        }
    }
    m.passages = remaining
    return nil
}

func (m *mockStore) Search(_ context.Context, vec []float32, topK int) ([]corpus.Match, error) {
    return nil, nil
}

func (m *mockStore) Close() error { return nil }

// mockEmbedder returns a fixed-length zero vector for any input.
type mockEmbedder struct{}

func (e *mockEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
    return make([]float32, 1024), nil
}

func TestFullPipeline_ChunkAndLoad(t *testing.T) {
    // Set up a temp corpus directory with the good-prose fixture
    dir := t.TempDir()
    rawDir := filepath.Join(dir, "raw", "testauthor")
    if err := os.MkdirAll(rawDir, 0755); err != nil {
        t.Fatal(err)
    }

    // Copy prose-good.txt as if fetch had downloaded it
    proseGood, err := os.ReadFile("../../testdata/fixtures/prose-good.txt")
    if err != nil {
        t.Fatalf("read prose-good.txt: %v", err)
    }
    if err := os.WriteFile(filepath.Join(rawDir, "67138.txt"), proseGood, 0644); err != nil {
        t.Fatal(err)
    }

    author := config.Author{
        ID:             "testauthor",
        Name:           "Test Author",
        GutenbergIDs:   []int{67138},
        TargetPassages: 3,
    }

    // Chunk
    passages, err := pipeline.ProcessAuthor(author, dir)
    if err != nil {
        t.Fatalf("ProcessAuthor: %v", err)
    }
    if len(passages) == 0 {
        t.Fatal("want at least 1 passage, got 0")
    }
    for _, p := range passages {
        if p.AuthorID != "testauthor" {
            t.Errorf("passage %q has wrong author_id: %q", p.ID, p.AuthorID)
        }
        if p.Score < 0.5 {
            t.Errorf("passage %q has low score %f", p.ID, p.Score)
        }
    }

    // Embed (mock — no real Bedrock call)
    ctx := context.Background()
    embedder := &mockEmbedder{}
    embedded, failed, err := pipeline.EmbedPassages(ctx, embedder, passages, map[string]bool{}, nil)
    if err != nil {
        t.Fatalf("EmbedPassages: %v", err)
    }
    if len(failed) != 0 {
        t.Errorf("want 0 failures, got %v", failed)
    }
    if len(embedded) != len(passages) {
        t.Errorf("want %d embedded, got %d", len(passages), len(embedded))
    }
    for _, ep := range embedded {
        if len(ep.Vector) != 1024 {
            t.Errorf("passage %q has wrong vector length %d", ep.ID, len(ep.Vector))
        }
    }

    // Load
    ms := &mockStore{}
    if err := pipeline.LoadToStore(ctx, ms, embedded); err != nil {
        t.Fatalf("LoadToStore: %v", err)
    }
    if len(ms.passages) != len(embedded) {
        t.Errorf("want %d passages in store, got %d", len(embedded), len(ms.passages))
    }

    // Re-load same author — DeleteByAuthor + Upsert, store should still have same count
    if err := pipeline.LoadToStore(ctx, ms, embedded); err != nil {
        t.Fatalf("LoadToStore (re-run): %v", err)
    }
    if len(ms.passages) != len(embedded) {
        t.Errorf("after re-load: want %d passages, got %d", len(embedded), len(ms.passages))
    }
}

func TestFullPipeline_BoilerplateDiscarded(t *testing.T) {
    dir := t.TempDir()
    rawDir := filepath.Join(dir, "raw", "bpauthor")
    if err := os.MkdirAll(rawDir, 0755); err != nil {
        t.Fatal(err)
    }

    boilerplate, err := os.ReadFile("../../testdata/fixtures/prose-boilerplate.txt")
    if err != nil {
        t.Fatalf("read prose-boilerplate.txt: %v", err)
    }
    if err := os.WriteFile(filepath.Join(rawDir, "999.txt"), boilerplate, 0644); err != nil {
        t.Fatal(err)
    }

    author := config.Author{
        ID:             "bpauthor",
        Name:           "BP Author",
        GutenbergIDs:   []int{999},
        TargetPassages: 5,
    }

    passages, err := pipeline.ProcessAuthor(author, dir)
    if err != nil {
        t.Fatalf("ProcessAuthor: %v", err)
    }
    // Boilerplate fixture is short and contains "Project Gutenberg" — expect 0 passages
    if len(passages) != 0 {
        t.Errorf("want 0 passages from boilerplate fixture, got %d", len(passages))
    }
}
```

- [ ] **Step 2: Run the E2E tests**

```bash
go test ./internal/pipeline/... -v -run "TestFullPipeline"
```

Expected: PASS — both tests green. (These tests use no external services.)

- [ ] **Step 3: Run the full test suite**

```bash
go test ./... -v
```

Expected: all tests PASS; store integration tests SKIP (no CHROMA_URL/DATABASE_URL set).

- [ ] **Step 4: Commit**

```bash
git add internal/pipeline/pipeline_test.go
git commit -m "test: add end-to-end pipeline integration test with mock store"
```

---

## Self-Review Checklist

- [x] **Spec coverage:**
  - `fetch` subcommand with `--force`, `--author`, rate limiting ✓ (Task 8)
  - `chunk` subcommand with `--dry-run`, `--author`, scoring, selection ✓ (Tasks 5–7)
  - `embed` subcommand with retry, skip-already-embedded, `--author` ✓ (Task 9)
  - `load` subcommand with `DeleteByAuthor`+`Upsert`, `--store`, `--author` ✓ (Tasks 11–12)
  - `Store` interface with `Upsert`, `DeleteByAuthor`, `Search`, `Close` ✓ (Task 4)
  - `ChromaStore` implementation ✓ (Task 10)
  - `PgvectorStore` implementation ✓ (Task 11)
  - Store factory (`STORE_BACKEND` env / `--store` flag) ✓ (Task 11)
  - `corpus/candidates.json` read/write ✓ (Task 3)
  - `corpus/embedded.json` read/write + ID tracking ✓ (Task 3)
  - `authors.yaml` parsing with default `target_passages` ✓ (Task 2)
  - Unit tests for chunking/scoring ✓ (Tasks 5–6)
  - Integration tests for stores (skipped without env vars) ✓ (Tasks 10–11)
  - E2E test with mock store and mock embedder ✓ (Task 13)
  - Boilerplate discard in E2E test ✓ (Task 13)
