# Frontend — Design Spec

**Project:** Literary DNA  
**Date:** 2026-04-14  
**Status:** Approved  

---

## Overview

A single-page frontend for Literary DNA. The user pastes a writing sample, clicks Analyze, and sees their top 3 author matches with evidence passages and a streaming Claude explanation. No build tooling, no bundler — three static files served directly by the Go server.

---

## Technology

- **Preact** + **htm** loaded from CDN via `<script type="module">` — React-compatible component model, ~3KB, no build step
- **Plain CSS** — minimal reset + CSS custom properties, no framework
- **Native browser APIs** — `fetch` + `ReadableStream` for SSE, `navigator.clipboard` for copy

---

## File Structure

```
frontend/
  index.html   # HTML shell, CDN script tags, mounts Preact app to #app
  app.js       # all components, state, and SSE stream logic
  style.css    # reset, custom properties, component styles
```

The Go server serves `frontend/` as static files. `GET /` serves `index.html`.

---

## UI States

The app moves through four states:

```
idle ──[Analyze]──► loading ──[event: matches]──► results
                          └──[error / network]──► error

results ──[Clear]──► idle
error   ──[Clear]──► idle
```

### idle

Input card with textarea and Analyze button. Word count displayed below textarea. Analyze is disabled if input is under 20 or over 5,000 words.

### loading

Input card remains visible, textarea and buttons disabled. Three skeleton author cards appear below with pulsing placeholder content. Status text: "Analyzing your writing..."

### results

Input card remains visible with Clear and Analyze buttons enabled. Author match cards replace the skeleton cards. Explanation section streams in below the cards. Copy summary button at the bottom.

### error

Input card remains visible. If the error occurs before any matches arrive: results area shows "Something went wrong — please try again." If the error occurs after matches are already displayed (Claude failure): match cards remain; only the explanation area shows the error message.

---

## Components

### `App`

Root component. Owns all state: `{ status, inputText, matches, explanation }`. Renders `InputCard` and conditionally `SkeletonCards`, `ResultCards`, or error message.

### `InputCard`

Props: `text`, `onChange`, `onAnalyze`, `onClear`, `disabled`, `status`

Renders the textarea, word count, and action buttons. In `results` and `error` states, shows both Clear and Analyze. In `loading`, disables both. In `idle`, shows only Analyze (no Clear).

### `SkeletonCard` (× 3)

Animated placeholder card shown during `loading`. Staggered opacity (1.0 / 0.7 / 0.5) to give a sense of ranked depth. No props.

### `ResultCard`

Props: `rank`, `authorName`, `score`, `passages`

Renders rank badge, author name, match percentage, and up to 2 evidence passages as blockquotes. Rank badge color: #7c9ef8 (1st), #a78bfa (2nd), #34d399 (3rd).

### `ExplanationPanel`

Props: `text`, `streaming`

Renders the "Why you match" section. While `streaming` is true, shows a blinking cursor after the text. Once `streaming` is false (after `event: done`), cursor disappears.

### `CopyButton`

Props: `matches`, `explanation` (nullable — omitted from summary if Claude failed)

Copies the structured summary to clipboard. Label toggles to "Copied!" for 2 seconds after click.

---

## SSE Stream Handling

`EventSource` only supports GET requests. Since `/analyze` is a POST with a JSON body, the stream is consumed via `fetch` + `response.body.getReader()` and parsed manually.

```js
async function analyze(text, onMatches, onDelta, onDone, onError) {
  const res = await fetch('/analyze', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text }),
  });

  if (!res.ok) {
    onError(await res.json());
    return;
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });

    // parse SSE frames from buffer
    const frames = buffer.split('\n\n');
    buffer = frames.pop(); // incomplete frame stays in buffer

    for (const frame of frames) {
      const eventMatch = frame.match(/^event: (\w+)/m);
      const dataMatch = frame.match(/^data: (.+)/m);
      if (!eventMatch || !dataMatch) continue;

      const event = eventMatch[1];
      const data = JSON.parse(dataMatch[1]);

      if (event === 'matches') onMatches(data.matches);
      else if (event === 'explanation') onDelta(data.delta);
      else if (event === 'done') onDone();
      else if (event === 'error') onError(data);
    }
  }
}
```

---

## Copy Summary Format

```
Your Literary DNA: 84% Hemingway · 71% Verne · 63% Conrad

Ernest Hemingway (84%)
"He was an old man who fished alone in a skiff in the Gulf Stream..."

Jules Verne (71%)
"The Nautilus was piercing the water with its sharp spur..."

Joseph Conrad (63%)
"The sea-reach of the Thames stretched before us..."

[explanation text]

— analyzed by Literary DNA
```

Copied via `navigator.clipboard.writeText()`. One evidence passage per author (the first of the two stored). Explanation included if fully streamed; omitted if Claude failed.

---

## Styling

Minimal CSS reset + custom properties. No utility framework.

```css
:root {
  --bg: #0f1117;
  --surface: #1a1d27;
  --border: #2a2d3a;
  --text: #e2e4ed;
  --muted: #6b7280;
}
```

Dark theme only for v1. System font stack. Max content width 640px, centered. No mobile breakpoints in v1 (out of scope).

---

## Static File Serving

The Go server registers a handler for `GET /` and all unmatched routes that serves files from the `frontend/` directory using `http.FileServer`. The `/analyze` and `/health` routes are registered before the static file handler so they take precedence.

---

## Testing

No automated tests for the frontend in v1. The SSE parsing function (`analyze`) is a pure function that can be unit tested in isolation if needed, but this is left to implementation discretion. Manual testing covers:

- Idle → submit → skeleton → results → copy
- Clear from results resets to idle
- Short input (<20 words) disables Analyze
- Network error before matches shows error state
- Claude error after matches shows match cards + explanation error

---

## Out of Scope

- Mobile-optimized layout
- Light theme
- Keyboard navigation / accessibility audit
- Browser history / back button support
- More than one language
