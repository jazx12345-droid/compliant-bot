# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project does

Automated regulatory monitoring tool for an in-house legal counsel at a tech company with cross-border operations in Malaysia, Hong Kong, and the US. It scrapes 20 regulatory websites daily, deduplicates items into SQLite, scores them for relevance via Claude API, and emails a grouped HTML digest.

## Setup

```bash
pip install -r requirements.txt
python -m playwright install chromium   # required for JS-rendered sources
cp .env.example .env                    # fill in SMTP + ANTHROPIC_API_KEY
```

## Running the pipeline

```bash
# Full pipeline, once (fetch → enrich → email digest)
python3 main.py --run-once

# Dry run: fetch + enrich, print digest to stdout, no email, no DB mark-sent
python3 main.py --run-once --dry-run

# Start the daily 08:00 scheduler (blocking)
python3 main.py
python3 main.py --schedule-time 07:30

# Run individual stages
python3 fetcher.py                          # fetch all sources
python3 fetcher.py --jurisdiction HK        # one jurisdiction
python3 fetcher.py --source "SFC" --verbose # one source, verbose
python3 enricher.py --limit 50 --dry-run    # enrich without writing to DB
python3 digest.py --dry-run                 # preview digest in terminal
python3 digest.py --save-html preview.html  # render HTML for browser preview
```

## Architecture

The pipeline is strictly linear; each module can also run standalone:

```
sources.py → fetcher.py → db.py ← enricher.py ← Claude API
                                ↓
                           digest.py → SMTP
                                ↑
                           main.py (orchestrates all + scheduler + alert emails)
```

**`sources.py`** — static list of 20 regulatory sources. Each entry has `name`, `url`, `jurisdiction` (MY / MY-LABUAN / HK / US), `type`, and `tags`. `type` controls which fetch strategy is used.

**`fetcher.py`** — three fetch strategies dispatched by `source["type"]`:
- `rss` — requests + feedparser (SFC, SEC press releases)
- `html` — requests + BeautifulSoup with site-specific parsers (Labuan FSA, SSM, OFAC, FinCEN, HKEX)
- `playwright` — headless Chromium for JS-rendered / WAF-protected sites (SC Malaysia, BNM, Bursa, HKMA, SEC rules pages)

Each site has its own named parser function (e.g. `_parse_labuan_fsa`, `_parse_hkex_circulars`). All parsers return `list[dict]` with keys `title`, `url`, `date`, `source`, `jurisdiction`, `tags`. `fetch_all()` orchestrates all sources and calls `db.deduplicate()` before returning.

**`db.py`** — SQLite wrapper around a single `items` table. Schema version tracked via `PRAGMA user_version`; migration from a legacy `articles` table runs automatically on first open. Key API: `insert_item()`, `deduplicate()`, `get_unsent_items()`, `update_summary()`, `update_relevance_score()`, `mark_sent()`. `relevance_score` is stored as `0.0–1.0` (divide enricher's 1–10 score by 10 before storing).

**`enricher.py`** — calls `gemini-1.5-flash` (free tier) via `google-generativeai`. Sends a single prompt requesting a JSON object; parses the response with `_extract_json()` (strips markdown fences if the model wraps output despite instructions), then validates with Pydantic `EnrichmentResult` (summary, tags, relevance_score 1–10, reasoning). Items scoring below `MIN_SCORE = 5` return `None` and are not persisted. The batch helper `enrich_items()` builds one `GenerativeModel` client and reuses it across all items, writing results back via `db.update_summary()` / `db.update_relevance_score()`.

**`digest.py`** — queries `items` where `sent_at IS NULL AND relevance_score >= min_score`, groups by jurisdiction in order MY → MY-LABUAN → HK → US, renders multipart HTML+plaintext email, sends via SMTP (STARTTLS port 587 by default, `DIGEST_SMTP_TLS=false` for SSL port 465), then calls `db.mark_sent()`.

**`main.py`** — wraps each stage in a `_stage()` context manager that logs timing, catches exceptions, and calls `send_alert()`. Stage failures are non-fatal by design: a fetch failure doesn't block enrichment of previously stored items; an enrich failure doesn't block sending already-scored items. The built-in `sched` module drives daily scheduling (no external dependency).

## Key data flows to be aware of

- **`content` field is not stored in SQLite.** `fetcher.py` returns items with a `content` key from scraped text, but `db.py` does not persist it. `enricher.py` expects `content` in the item dict passed to it. In the pipeline, `enrich_items()` is called with items fetched fresh from the DB (which have no `content`), so enrichment currently runs title-only. To add full-text enrichment, scraped content must be stored in the DB or passed through in-memory before the DB write.

- **Score dual representation.** `enricher.py` works in 1–10 integer scores internally. When writing to the DB it divides by 10 (stored as 0.0–1.0). `digest.py` multiplies back by 10 for display. `DIGEST_MIN_SCORE` env var is in 0.0–1.0 scale.

- **Deduplication is URL-based.** The `url` column has a `UNIQUE` constraint. `deduplicate()` and `insert_item()` both return `None` / skip on `IntegrityError` for duplicate URLs.

- **`sent_at` gates the digest.** Only items with `sent_at IS NULL` appear in any digest. Once `mark_sent()` runs, an item never appears again regardless of its score.

## Adding a new source

1. Add an entry to `SOURCES` in `sources.py` with the correct `type`.
2. If `type = "html"` or `type = "playwright"`, add a `_parse_<name>` function in `fetcher.py` and register it in `_HTML_DISPATCH` or `_PLAYWRIGHT_DISPATCH`.
3. Test with: `python3 fetcher.py --source "<name>" --no-dedupe --verbose`

## Environment variables reference

| Variable | Required | Default | Notes |
|---|---|---|---|
| `GEMINI_API_KEY` | yes | — | For enricher (gemini-1.5-flash) |
| `DIGEST_SMTP_HOST` | yes | — | e.g. `smtp.gmail.com` |
| `DIGEST_SMTP_PORT` | yes | `587` | |
| `DIGEST_SMTP_USER` | yes | — | Login address |
| `DIGEST_SMTP_PASSWORD` | yes | — | App password for Gmail |
| `DIGEST_FROM` | yes | — | `"Name <addr>"` |
| `DIGEST_TO` | yes | — | Comma-separated recipients |
| `DIGEST_SMTP_TLS` | no | `true` | Set `false` for SSL port 465 |
| `DIGEST_MIN_SCORE` | no | `0.5` | 0.0–1.0 (= 5/10) |
| `DIGEST_HOURS` | no | `24` | Digest look-back window |
| `PIPELINE_ALERT_TO` | no | `DIGEST_TO` | Override alert recipient |
| `PIPELINE_ENRICH_LIMIT` | no | `200` | Max items enriched per run |
| `PIPELINE_SCHEDULE_TIME` | no | `08:00` | Daily run time (local) |
