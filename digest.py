"""
Regulatory digest emailer.

Queries SQLite for unsent items from the past 24 hours (that have been
enriched and scored ≥ 0.5 / 5-out-of-10), groups them by jurisdiction,
renders a clean HTML email, sends it via SMTP, then marks the items sent.

Configuration (environment variables or .env file)
───────────────────────────────────────────────────
Required:
  DIGEST_SMTP_HOST      e.g. smtp.gmail.com  or  smtp.office365.com
  DIGEST_SMTP_PORT      e.g. 587
  DIGEST_SMTP_USER      your login address
  DIGEST_SMTP_PASSWORD  app-password / OAuth token
  DIGEST_FROM           display name + address, e.g. "Regulatory Monitor <you@example.com>"
  DIGEST_TO             comma-separated recipient list

Optional:
  DIGEST_SMTP_TLS       "true" (default) — STARTTLS; set "false" for SSL-wrapped port 465
  DIGEST_MIN_SCORE      minimum relevance_score (0.0–1.0, default 0.5 = 5/10)
  DIGEST_HOURS          look-back window in hours (default 24)
  DIGEST_SUBJECT_PREFIX e.g. "[RegWatch]" (default "[Regulatory Digest]")

Gmail quick-start
─────────────────
  1. Enable 2-step verification on your Google account.
  2. Create an App Password at myaccount.google.com/apppasswords.
  3. Set DIGEST_SMTP_HOST=smtp.gmail.com, DIGEST_SMTP_PORT=587,
     DIGEST_SMTP_TLS=true, DIGEST_SMTP_PASSWORD=<16-char app password>.

Outlook / Microsoft 365 quick-start
────────────────────────────────────
  Set DIGEST_SMTP_HOST=smtp.office365.com, DIGEST_SMTP_PORT=587,
  DIGEST_SMTP_TLS=true, DIGEST_SMTP_PASSWORD=<your password or app password>.
"""

import os
import smtplib
import sqlite3
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, parseaddr
from pathlib import Path
from typing import Optional

import db

# ── Config ────────────────────────────────────────────────────────────────────

# Load .env if python-dotenv is installed (optional convenience)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

JURISDICTION_LABELS = {
    "MY":       "Malaysia",
    "MY-LABUAN": "Malaysia — Labuan",
    "HK":       "Hong Kong",
    "US":       "United States",
}

JURISDICTION_ORDER = ["MY", "MY-LABUAN", "HK", "US"]


# ── DB query ──────────────────────────────────────────────────────────────────

def _get_recent_unsent(hours: int, min_score: float) -> list[dict]:
    """
    Return unsent items created in the last ``hours`` hours with
    relevance_score >= min_score (NULL-scored items are excluded —
    they haven't been enriched yet).
    """
    conn = db.get_conn()
    try:
        cur = conn.execute(
            """
            SELECT *
              FROM items
             WHERE sent_at IS NULL
               AND created_at >= datetime('now', ?)
               AND relevance_score IS NOT NULL
               AND relevance_score >= ?
             ORDER BY jurisdiction, date DESC, created_at DESC
            """,
            (f"-{hours} hours", min_score),
        )
        return [db._row_to_dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


# ── Grouping ──────────────────────────────────────────────────────────────────

def _group_by_jurisdiction(items: list[dict]) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for item in items:
        j = item.get("jurisdiction") or "Other"
        groups.setdefault(j, []).append(item)
    # Sort jurisdictions in preferred order; unknown ones go last alphabetically
    ordered: dict[str, list[dict]] = {}
    for j in JURISDICTION_ORDER:
        if j in groups:
            ordered[j] = groups.pop(j)
    for j in sorted(groups):
        ordered[j] = groups[j]
    return ordered


# ── HTML rendering ────────────────────────────────────────────────────────────

_HTML_HEAD = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{subject}</title>
<style>
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial,
                 sans-serif;
    font-size: 14px;
    line-height: 1.6;
    color: #1a1a1a;
    background: #f5f5f5;
    margin: 0;
    padding: 0;
  }}
  .wrapper {{
    max-width: 680px;
    margin: 24px auto;
    background: #ffffff;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 1px 4px rgba(0,0,0,.12);
  }}
  .header {{
    background: #1a3a5c;
    color: #ffffff;
    padding: 24px 32px 20px;
  }}
  .header h1 {{
    margin: 0 0 4px;
    font-size: 20px;
    font-weight: 600;
    letter-spacing: -.2px;
  }}
  .header p {{
    margin: 0;
    font-size: 12px;
    opacity: .75;
  }}
  .body {{
    padding: 0 32px 32px;
  }}
  .jurisdiction-heading {{
    font-size: 11px;
    font-weight: 700;
    letter-spacing: .08em;
    text-transform: uppercase;
    color: #1a3a5c;
    border-bottom: 2px solid #1a3a5c;
    padding-bottom: 6px;
    margin: 32px 0 16px;
  }}
  .item {{
    margin-bottom: 20px;
    padding: 16px 18px;
    background: #fafafa;
    border: 1px solid #e8e8e8;
    border-radius: 6px;
  }}
  .item-title {{
    margin: 0 0 4px;
    font-size: 14px;
    font-weight: 600;
  }}
  .item-title a {{
    color: #1a3a5c;
    text-decoration: none;
  }}
  .item-title a:hover {{ text-decoration: underline; }}
  .item-meta {{
    font-size: 11px;
    color: #777;
    margin: 0 0 8px;
  }}
  .item-summary {{
    font-size: 13px;
    color: #333;
    margin: 0 0 10px;
    line-height: 1.55;
  }}
  .tags {{
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
  }}
  .tag {{
    font-size: 10px;
    font-weight: 600;
    letter-spacing: .04em;
    text-transform: uppercase;
    background: #e8f0fe;
    color: #1a56db;
    padding: 2px 7px;
    border-radius: 10px;
  }}
  .score-badge {{
    display: inline-block;
    font-size: 10px;
    font-weight: 700;
    background: #1a3a5c;
    color: #fff;
    padding: 2px 7px;
    border-radius: 10px;
    margin-left: 6px;
    vertical-align: middle;
  }}
  .footer {{
    background: #f0f0f0;
    padding: 16px 32px;
    font-size: 11px;
    color: #888;
    text-align: center;
  }}
</style>
</head>
<body>
<div class="wrapper">
"""

_HTML_HEADER = """\
  <div class="header">
    <h1>{subject}</h1>
    <p>Generated {now_utc} UTC &nbsp;·&nbsp; {total} items across {jcount} jurisdictions</p>
  </div>
  <div class="body">
"""

_HTML_JURISDICTION = """\
    <div class="jurisdiction-heading">{label} &nbsp;({count})</div>
"""

_HTML_ITEM = """\
    <div class="item">
      <p class="item-title">
        <a href="{url}">{title}</a>
        <span class="score-badge">{score}/10</span>
      </p>
      <p class="item-meta">{source} &nbsp;·&nbsp; {date}</p>
      {summary_block}
      {tags_block}
    </div>
"""

_HTML_FOOT = """\
  </div><!-- /body -->
  <div class="footer">
    This digest was automatically generated. Items are filtered to relevance ≥ {min_score_pct}%.
    Do not reply to this email.
  </div>
</div><!-- /wrapper -->
</body>
</html>
"""


def _esc(text: str) -> str:
    """Minimal HTML escaping."""
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
    )


def _score_display(raw: Optional[float]) -> str:
    """Convert stored 0.0–1.0 score → 1–10 integer string."""
    if raw is None:
        return "?"
    return str(round(raw * 10))


def render_html(
    groups: dict[str, list[dict]],
    subject: str,
    min_score: float,
) -> str:
    total = sum(len(v) for v in groups.values())
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")

    parts = [
        _HTML_HEAD.format(subject=_esc(subject)),
        _HTML_HEADER.format(
            subject=_esc(subject),
            now_utc=now_utc,
            total=total,
            jcount=len(groups),
        ),
    ]

    for jurisdiction, items in groups.items():
        label = JURISDICTION_LABELS.get(jurisdiction, jurisdiction)
        parts.append(_HTML_JURISDICTION.format(label=_esc(label), count=len(items)))

        for item in items:
            title = _esc(item.get("title") or "(untitled)")
            url = item.get("url") or "#"
            source = _esc(item.get("source") or "")
            date = _esc(item.get("date") or "")
            score = _score_display(item.get("relevance_score"))

            raw_summary = (item.get("summary") or "").strip()
            summary_block = (
                f'<p class="item-summary">{_esc(raw_summary)}</p>'
                if raw_summary
                else ""
            )

            raw_tags = item.get("tags") or []
            if isinstance(raw_tags, str):
                raw_tags = [t for t in raw_tags.split(",") if t]
            tags_block = ""
            if raw_tags:
                tag_spans = "".join(
                    f'<span class="tag">{_esc(t.replace("_", " "))}</span>'
                    for t in raw_tags
                )
                tags_block = f'<div class="tags">{tag_spans}</div>'

            parts.append(
                _HTML_ITEM.format(
                    url=url,
                    title=title,
                    score=score,
                    source=source,
                    date=date,
                    summary_block=summary_block,
                    tags_block=tags_block,
                )
            )

    parts.append(_HTML_FOOT.format(min_score_pct=round(min_score * 100)))
    return "".join(parts)


def render_plaintext(groups: dict[str, list[dict]], subject: str) -> str:
    lines = [subject, "=" * len(subject), ""]
    for jurisdiction, items in groups.items():
        label = JURISDICTION_LABELS.get(jurisdiction, jurisdiction)
        lines += [f"\n── {label} ({len(items)}) ──", ""]
        for item in items:
            score = _score_display(item.get("relevance_score"))
            lines += [
                f"[{score}/10] {item.get('title', '(untitled)')}",
                f"  Source : {item.get('source', '')}",
                f"  Date   : {item.get('date', '')}",
                f"  URL    : {item.get('url', '')}",
            ]
            summary = (item.get("summary") or "").strip()
            if summary:
                lines.append(f"  Summary: {summary}")
            tags = item.get("tags") or []
            if tags:
                if isinstance(tags, str):
                    tags = [t for t in tags.split(",") if t]
                lines.append(f"  Tags   : {', '.join(tags)}")
            lines.append("")
    return "\n".join(lines)


# ── Email assembly ────────────────────────────────────────────────────────────

def _build_message(
    subject: str,
    html_body: str,
    plain_body: str,
    from_addr: str,
    to_addrs: list[str],
) -> MIMEMultipart:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)
    msg.attach(MIMEText(plain_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    return msg


# ── SMTP send ─────────────────────────────────────────────────────────────────

def _send_smtp(msg: MIMEMultipart, cfg: dict) -> None:
    host = cfg["host"]
    port = int(cfg["port"])
    user = cfg["user"]
    password = cfg["password"]
    use_tls = cfg.get("tls", "true").lower() != "false"
    to_addrs = [a.strip() for a in cfg["to"].split(",")]

    if use_tls:
        # STARTTLS (port 587 for Gmail / Outlook)
        with smtplib.SMTP(host, port, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(user, password)
            server.sendmail(user, to_addrs, msg.as_bytes())
    else:
        # SSL-wrapped (port 465)
        with smtplib.SMTP_SSL(host, port, timeout=30) as server:
            server.login(user, password)
            server.sendmail(user, to_addrs, msg.as_bytes())


# ── Main entry point ──────────────────────────────────────────────────────────

def send_digest(
    hours: int = 24,
    min_score: float = 0.5,
    dry_run: bool = False,
) -> dict:
    """
    Build and send the regulatory digest email.

    Args:
        hours:     look-back window (default 24 h).
        min_score: minimum stored relevance_score (0.0–1.0).  0.5 = 5/10.
        dry_run:   if True, render and print the email but do not send or mark sent.

    Returns:
        dict with keys: items_found, items_sent, jurisdictions, subject
    """
    cfg = {
        "host":     os.environ.get("DIGEST_SMTP_HOST", ""),
        "port":     os.environ.get("DIGEST_SMTP_PORT", "587"),
        "user":     os.environ.get("DIGEST_SMTP_USER", ""),
        "password": os.environ.get("DIGEST_SMTP_PASSWORD", ""),
        "tls":      os.environ.get("DIGEST_SMTP_TLS", "true"),
        "from":     os.environ.get("DIGEST_FROM", ""),
        "to":       os.environ.get("DIGEST_TO", ""),
    }

    if not dry_run:
        missing = [k for k in ("host", "user", "password", "from", "to") if not cfg[k]]
        if missing:
            raise EnvironmentError(
                f"Missing required environment variables: "
                + ", ".join(f"DIGEST_{k.upper()}" for k in missing)
            )

    prefix = os.environ.get("DIGEST_SUBJECT_PREFIX", "[Regulatory Digest]")
    date_str = datetime.now(timezone.utc).strftime("%d %b %Y")
    subject = f"{prefix} {date_str}"

    # ── Fetch items ──
    items = _get_recent_unsent(hours=hours, min_score=min_score)
    if not items:
        print(f"[digest] No unsent items found in the last {hours} h (min score {min_score:.0%}).")
        return {"items_found": 0, "items_sent": 0, "jurisdictions": [], "subject": subject}

    groups = _group_by_jurisdiction(items)
    item_ids = [i["id"] for i in items]

    # ── Render ──
    html_body  = render_html(groups, subject, min_score)
    plain_body = render_plaintext(groups, subject)

    if dry_run:
        print(f"[digest] DRY RUN — {len(items)} items, {len(groups)} jurisdictions")
        print(plain_body)
        return {
            "items_found": len(items),
            "items_sent": 0,
            "jurisdictions": list(groups.keys()),
            "subject": subject,
        }

    # ── Send ──
    from_addr = cfg["from"]
    to_addrs  = [a.strip() for a in cfg["to"].split(",")]
    msg = _build_message(subject, html_body, plain_body, from_addr, to_addrs)
    _send_smtp(msg, cfg)
    print(f"[digest] Sent '{subject}' → {cfg['to']}")

    # ── Mark sent ──
    marked = db.mark_sent(item_ids)
    print(f"[digest] Marked {marked} items as sent.")

    return {
        "items_found": len(items),
        "items_sent": marked,
        "jurisdictions": list(groups.keys()),
        "subject": subject,
    }


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Send the regulatory digest email.")
    parser.add_argument(
        "--hours", type=int, default=24,
        help="Look-back window in hours (default 24)",
    )
    parser.add_argument(
        "--min-score", type=float, default=0.5,
        help="Minimum relevance_score 0.0–1.0 (default 0.5 = 5/10)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Render and print the digest without sending or marking sent",
    )
    parser.add_argument(
        "--save-html", metavar="FILE",
        help="Also save the rendered HTML to a file (useful for previewing)",
    )
    args = parser.parse_args()

    # Preview / save HTML without sending
    if args.save_html:
        items = _get_recent_unsent(hours=args.hours, min_score=args.min_score)
        if items:
            groups = _group_by_jurisdiction(items)
            prefix = os.environ.get("DIGEST_SUBJECT_PREFIX", "[Regulatory Digest]")
            subject = f"{prefix} {datetime.now(timezone.utc).strftime('%d %b %Y')}"
            html = render_html(groups, subject, args.min_score)
            Path(args.save_html).write_text(html, encoding="utf-8")
            print(f"[digest] HTML saved to {args.save_html}")
        else:
            print("[digest] No items to render.")

    result = send_digest(
        hours=args.hours,
        min_score=args.min_score,
        dry_run=args.dry_run,
    )
    print(f"[digest] Result: {result}")
