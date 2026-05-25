# Search (quick.search)

Full-text, vector, and hybrid search across indexed Quick sites. Backed by PostgreSQL (`tsvector` + `pgvector`). Site is automatically scoped from the subdomain.

## Include

```html
<script src="/client/quick.js"></script>
```

## Search

```javascript
// Basic search (scoped to current site)
const results = await quick.search.search("deploy");
// => [{ id, site_id, url_path, title, score, snippet, additional_snippets }]

// Choose method
await quick.search.search("deploy", { method: "fts" });      // keyword (tsvector)
await quick.search.search("deploy", { method: "vector" });   // semantic (pgvector)
await quick.search.search("deploy", { method: "hybrid" });   // default (RRF fusion)

// Search across all sites
await quick.search.search("deploy", { scope: "global" });

// Search specific sites
await quick.search.search("deploy", { sites: ["quick", "docs", "blog"] });

// Filter by source type and limit
await quick.search.search("deploy", { source: "site", limit: 5 });
```

### Tuning

```javascript
await quick.search.search("deploy", {
  vec_similarity: 0.6,  // min cosine similarity for vector (0-1, higher = stricter)
  fts_rank: 0.01,       // min ts_rank score for FTS results
  fts_weight: 0.7,      // RRF weight for FTS side (hybrid only)
  vec_weight: 0.3,      // RRF weight for vector side (hybrid only)
});
```

## List Searchable Sites

```javascript
const sites = await quick.search.sites();
// => [{ site_id, document_count, last_updated }]
```

## Index Stats

```javascript
// Current site (derived from subdomain)
const stats = await quick.search.stats();
// => { site_id, documents, chunks, total_content_bytes, avg_chunks_per_doc, last_updated }

// Specific site
await quick.search.stats("quick");

// Global stats (when no subdomain)
await quick.search.stats();
// => { sites, documents, chunks, embedded_chunks, total_content_bytes, avg_chunks_per_doc }
```

## Full Document Content

```javascript
const doc = await quick.search.content("sites/quick/ci.html");
// => { id, site_id, url_path, title, full_text }
// Returns null if not found.
```

## Search Methods

| Method | How it works | Best for |
|---|---|---|
| `fts` | PostgreSQL full-text search (`tsvector`/`tsquery`), `ts_rank` scoring | Exact keyword queries |
| `vector` | Cosine similarity on chunk embeddings (`pgvector`) | Semantic / conceptual queries |
| `hybrid` | Reciprocal Rank Fusion combining FTS and vector | General use (default) |

## Quick Reference

| Method | Description |
|---|---|
| `search(query, options?)` | Search indexed content |
| `sites()` | List searchable sites |
| `stats(siteId?)` | Index statistics (site-scoped or global) |
| `content(id, siteId?)` | Fetch full document text |

### Search Options

| Option | Default | Description |
|---|---|---|
| `method` | `hybrid` | `fts`, `vector`, or `hybrid` |
| `scope` | — | `global` to search all sites |
| `sites` | — | Array of site IDs to search |
| `source` | — | Filter by source type (`site`, `files`) |
| `limit` | `10` | Max results |
| `vec_similarity` | `0.55` | Min vector similarity (0–1) |
| `fts_rank` | `0` | Min FTS `ts_rank` score |
| `fts_weight` | `0.7` | RRF weight for FTS (hybrid) |
| `vec_weight` | `0.3` | RRF weight for vector (hybrid) |
