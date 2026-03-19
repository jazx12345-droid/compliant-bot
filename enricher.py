"""
AI enrichment module for regulatory items.

Calls Google Gemini (gemini-1.5-flash, free tier) to produce:
  - A one-paragraph summary
  - Relevance tags (corporate_governance, securities, tax, employment, …)
  - A relevance score 1–10 for an in-house counsel at a tech company
    with cross-border investments (MY / HK / US)

Items scoring below 5 are filtered out (returns None).

Requires:
  GEMINI_API_KEY  environment variable (or .env file)

Usage
─────
    from enricher import enrich_item

    item = {
        "id": 42,
        "title": "SC Malaysia issues new guidelines on ESG disclosure",
        "content": "Full scraped text or PDF content …",
        "jurisdiction": "MY",
        "source": "SC Malaysia Media Releases",
    }
    result = enrich_item(item)   # returns EnrichmentResult or None
    if result:
        print(result.summary)
        print(result.tags)
        print(result.relevance_score)
"""

import json
import os
import re
import time
from typing import Optional

from google import genai
from google.genai import types as genai_types
from pydantic import BaseModel, Field, ValidationError

# ── Load .env (optional) ──────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Constants ─────────────────────────────────────────────────────────────────

MODEL = "gemini-2.5-flash"

# Free tier: 5 req/min for gemini-2.5-flash. 13s between calls stays safely
# under that limit and keeps daily quota (25 req/day on free tier) in mind.
REQUEST_DELAY = 13  # seconds

VALID_TAGS = [
    "corporate_governance",
    "securities",
    "capital_markets",
    "banking",
    "financial_regulation",
    "monetary_policy",
    "listing_rules",
    "aml",
    "sanctions",
    "enforcement",
    "litigation",
    "rulemaking",
    "tax",
    "employment",
    "data_privacy",
    "technology_regulation",
    "cross_border",
    "offshore",
    "investment",
    "fund_management",
    "insurance",
    "company_law",
    "esg",
    "other",
]

MIN_SCORE = 5  # items scoring below this are dropped


# ── Pydantic schema ───────────────────────────────────────────────────────────

class EnrichmentResult(BaseModel):
    summary: str = Field(
        description=(
            "A single paragraph (3–6 sentences) summarising the regulatory item "
            "for an in-house counsel. Highlight the key obligation, deadline, or "
            "enforcement action and any immediate action required."
        )
    )
    tags: list[str] = Field(
        description=(
            "List of 1–5 topic tags chosen from the allowed list. "
            f"Allowed values: {', '.join(VALID_TAGS)}"
        )
    )
    relevance_score: int = Field(
        ge=1,
        le=10,
        description=(
            "Integer 1–10 reflecting relevance for an in-house legal counsel at a "
            "technology company with cross-border investments in Malaysia, Hong Kong, "
            "and the United States. "
            "10 = directly actionable obligation or enforcement risk; "
            "7–9 = significant policy change requiring monitoring; "
            "4–6 = general industry relevance; "
            "1–3 = peripheral or unlikely to affect the company."
        ),
    )
    reasoning: str = Field(
        description=(
            "One sentence explaining why this score was assigned. "
            "Not shown to end users — used for audit / calibration."
        )
    )


# ── Prompt ────────────────────────────────────────────────────────────────────

_PROMPT_TEMPLATE = """\
You are a regulatory intelligence assistant helping an in-house legal counsel \
at a technology company that operates in Malaysia, Hong Kong, and the United States \
and holds cross-border investments across all three jurisdictions.

Analyse the regulatory item below and respond with ONLY a valid JSON object — \
no markdown fences, no prose, no extra keys.

Required JSON schema:
{{
  "summary": "<single paragraph, 3-6 sentences, key obligation/deadline/action>",
  "tags": ["<tag1>", "<tag2>"],
  "relevance_score": <integer 1-10>,
  "reasoning": "<one sentence explaining the score>"
}}

Allowed tag values (use only these):
{tags}

Scoring guide:
10  Immediate compliance obligation or enforcement action directly affecting the company.
7-9 Material policy/rule change requiring legal review and likely action.
4-6 Industry development worth monitoring; no immediate action required.
1-3 Peripheral, unrelated sectors, or purely macro context.

Be concise. Do not invent information not present in the source text.

---
Source: {source}
Jurisdiction: {jurisdiction}
Title: {title}

Content:
{content}
"""


# ── Gemini client factory ─────────────────────────────────────────────────────

def _make_client() -> genai.Client:
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise EnvironmentError(
            "GEMINI_API_KEY is not set. "
            "Add it to your environment or .env file."
        )
    return genai.Client(api_key=api_key)


# ── JSON extraction ───────────────────────────────────────────────────────────

def _extract_json(text: str) -> dict:
    """
    Pull the first JSON object out of the model response.
    Handles cases where the model wraps output in markdown fences despite
    being told not to.
    """
    # Strip ```json ... ``` fences if present
    text = re.sub(r"```(?:json)?\s*", "", text).strip().rstrip("`").strip()

    # Find the outermost {...}
    start = text.find("{")
    end   = text.rfind("}")
    if start == -1 or end == -1:
        raise ValueError(f"No JSON object found in model response: {text[:200]!r}")

    return json.loads(text[start : end + 1])


# ── Core function ─────────────────────────────────────────────────────────────

def enrich_item(
    item: dict,
    client: Optional[genai.Client] = None,
) -> Optional[EnrichmentResult]:
    """
    Enrich a single regulatory item with AI-generated summary, tags, and score.

    Args:
        item:   dict with at minimum 'title' and 'content' keys.
                Optional keys: 'jurisdiction', 'source', 'url'.
        client: optional pre-built genai.Client (useful for batching to
                avoid re-constructing the client on every call).

    Returns:
        EnrichmentResult if relevance_score >= MIN_SCORE, else None.

    Raises:
        EnvironmentError  if GEMINI_API_KEY is not set.
        ValueError        if the model returns unparseable JSON.
    """
    if client is None:
        client = _make_client()

    prompt = _PROMPT_TEMPLATE.format(
        tags=", ".join(VALID_TAGS),
        source=item.get("source", "unknown"),
        jurisdiction=item.get("jurisdiction", "unknown"),
        title=item.get("title", "(no title)"),
        content=item.get("content", "").strip()
               or "(no content — score based on title only)",
    )

    response = client.models.generate_content(
        model=MODEL,
        contents=prompt,
        config=genai_types.GenerateContentConfig(
            temperature=0.1,
            max_output_tokens=4096,
        ),
    )
    raw = response.text

    data = _extract_json(raw)

    # Validate and coerce with Pydantic
    result = EnrichmentResult(**data)

    # Clamp tags to the allowed list (model may hallucinate extras)
    result.tags = [t for t in result.tags if t in VALID_TAGS]

    if result.relevance_score < MIN_SCORE:
        return None

    return result


# ── Batch helper ──────────────────────────────────────────────────────────────

def enrich_items(
    items: list[dict],
    update_db: bool = True,
) -> tuple[list[dict], int]:
    """
    Enrich a list of items and optionally persist results to the DB.

    Each item dict must have 'id' in addition to 'title' / 'content'.

    Returns:
        (enriched_items, skipped_count)
    """
    import db  # local import to avoid circular dependency

    # Build the client once; reused across all items
    try:
        client: genai.Client = _make_client()
    except EnvironmentError as exc:
        raise RuntimeError(str(exc)) from exc

    enriched: list[dict] = []
    skipped = 0

    for i, item in enumerate(items):
        if i > 0:
            time.sleep(REQUEST_DELAY)  # stay within free-tier rate limit
        item_id = item.get("id")
        try:
            result = enrich_item(item, client=client)
        except Exception as exc:
            print(f"[enricher] ERROR item {item_id} ({item.get('title', '')[:60]}): {exc}")
            skipped += 1
            continue

        if result is None:
            skipped += 1
            continue

        if update_db and item_id:
            db.update_summary(item_id, result.summary)
            db.update_relevance_score(item_id, result.relevance_score / 10.0)

        enriched_item = dict(item)
        enriched_item["summary"] = result.summary
        enriched_item["tags"] = result.tags
        enriched_item["relevance_score"] = result.relevance_score
        enriched.append(enriched_item)

    return enriched, skipped


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import json as _json

    parser = argparse.ArgumentParser(description="Enrich unsent regulatory items with Gemini.")
    parser.add_argument("--jurisdiction", help="Filter by jurisdiction code (MY/HK/US)")
    parser.add_argument("--limit", type=int, default=50, help="Max items to process (default 50)")
    parser.add_argument("--dry-run", action="store_true", help="Do not write results to DB")
    parser.add_argument("--verbose", action="store_true", help="Print each result as JSON")
    args = parser.parse_args()

    import db

    items = db.get_unsent_items(
        jurisdiction=args.jurisdiction,
        min_score=None,
        limit=args.limit,
    )
    unscored = [i for i in items if i.get("relevance_score") is None]

    print(f"[enricher] {len(unscored)} unscored items to process (limit {args.limit})")

    enriched, skipped = enrich_items(unscored, update_db=not args.dry_run)

    print(f"[enricher] Done — {len(enriched)} enriched, {skipped} skipped/errored")

    if args.verbose:
        for it in enriched:
            print(_json.dumps(
                {k: it[k] for k in ("id", "title", "relevance_score", "tags", "summary")},
                indent=2,
                ensure_ascii=False,
            ))
