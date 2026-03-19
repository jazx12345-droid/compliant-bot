"""
Regulatory monitor — pipeline orchestrator.

Pipeline (runs once, or on a schedule):
  1. fetch    — scrape all sources, deduplicate against SQLite
  2. enrich   — call Claude API for summary + relevance score per item
  3. digest   — render HTML email and send via SMTP
  4. (alert)  — if any stage raises, send a separate alert email

Scheduling
──────────
Runs every morning at 08:00 local time (configurable via --schedule-time).
The scheduler is a lightweight blocking loop using the built-in `sched`
module — no external dependencies required.  For production you can instead
use a system cron job and call:
    python3 main.py --run-once

Logging
───────
All output goes to both stdout and  logs/pipeline.log  (rotating, 5 × 5 MB).
Error-level messages are additionally mirrored to  logs/errors.log.

Environment variables (same .env as digest.py)
──────────────────────────────────────────────
Required for email sending:  DIGEST_SMTP_*  /  DIGEST_FROM  /  DIGEST_TO
Required for enrichment:     ANTHROPIC_API_KEY
Optional:
  PIPELINE_ALERT_TO       override recipient for alert emails (default: DIGEST_TO)
  PIPELINE_ENRICH_LIMIT   max items to enrich per run (default 200)
  PIPELINE_ENRICH_HOURS   look-back window for enrichment in hours (default 25)
  DIGEST_HOURS            look-back window for digest (default 24)
  DIGEST_MIN_SCORE        minimum relevance score for digest (default 0.5)
"""

from __future__ import annotations

import argparse
import logging
import logging.handlers
import os
import sched
import smtplib
import sys
import time
import traceback
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

# ── Load .env ────────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Logging setup ─────────────────────────────────────────────────────────────

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

_fmt = logging.Formatter(
    fmt="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def _build_logger() -> logging.Logger:
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # stdout — INFO and above
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(_fmt)
    root.addHandler(sh)

    # logs/pipeline.log — INFO and above, rotating 5 × 5 MB
    fh = logging.handlers.RotatingFileHandler(
        LOG_DIR / "pipeline.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    fh.setLevel(logging.INFO)
    fh.setFormatter(_fmt)
    root.addHandler(fh)

    # logs/errors.log — ERROR and above only
    eh = logging.handlers.RotatingFileHandler(
        LOG_DIR / "errors.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    eh.setLevel(logging.ERROR)
    eh.setFormatter(_fmt)
    root.addHandler(eh)

    return logging.getLogger("main")


log = _build_logger()

# ── Alert email ───────────────────────────────────────────────────────────────


def _smtp_cfg() -> dict:
    return {
        "host":     os.environ.get("DIGEST_SMTP_HOST", ""),
        "port":     int(os.environ.get("DIGEST_SMTP_PORT", "587")),
        "user":     os.environ.get("DIGEST_SMTP_USER", ""),
        "password": os.environ.get("DIGEST_SMTP_PASSWORD", ""),
        "tls":      os.environ.get("DIGEST_SMTP_TLS", "true").lower() != "false",
        "from":     os.environ.get("DIGEST_FROM", ""),
        "to":       os.environ.get("PIPELINE_ALERT_TO")
                    or os.environ.get("DIGEST_TO", ""),
    }


def send_alert(stage: str, exc: BaseException, tb: str) -> None:
    """Send a plain-text alert email when a pipeline stage fails."""
    cfg = _smtp_cfg()
    if not all([cfg["host"], cfg["user"], cfg["password"], cfg["from"], cfg["to"]]):
        log.warning("Alert email skipped — SMTP not fully configured.")
        return

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    subject = f"[RegWatch ALERT] Pipeline failure in '{stage}' — {now}"
    body = (
        f"The regulatory monitoring pipeline encountered an error.\n\n"
        f"Stage   : {stage}\n"
        f"Time    : {now}\n"
        f"Error   : {type(exc).__name__}: {exc}\n\n"
        f"Traceback:\n{tb}\n\n"
        f"Check {LOG_DIR / 'errors.log'} for the full log.\n"
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = cfg["from"]
    to_addrs = [a.strip() for a in cfg["to"].split(",")]
    msg["To"] = ", ".join(to_addrs)
    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        if cfg["tls"]:
            with smtplib.SMTP(cfg["host"], cfg["port"], timeout=30) as server:
                server.ehlo(); server.starttls(); server.ehlo()
                server.login(cfg["user"], cfg["password"])
                server.sendmail(cfg["user"], to_addrs, msg.as_bytes())
        else:
            with smtplib.SMTP_SSL(cfg["host"], cfg["port"], timeout=30) as server:
                server.login(cfg["user"], cfg["password"])
                server.sendmail(cfg["user"], to_addrs, msg.as_bytes())
        log.info("Alert email sent to %s", cfg["to"])
    except Exception as mail_exc:
        log.error("Failed to send alert email: %s", mail_exc)


# ── Pipeline stages ───────────────────────────────────────────────────────────


def _stage(name: str):
    """
    Decorator-like context manager.  Logs start/end timing and catches
    any exception, logs it, sends an alert, then re-raises so the caller
    can decide whether to abort the run.
    """
    import contextlib

    @contextlib.contextmanager
    def _ctx():
        log.info("── Stage: %s — starting ──", name)
        t0 = time.monotonic()
        try:
            yield
            elapsed = time.monotonic() - t0
            log.info("── Stage: %s — done (%.1f s) ──", name, elapsed)
        except Exception as exc:
            elapsed = time.monotonic() - t0
            tb = traceback.format_exc()
            log.error(
                "── Stage: %s — FAILED after %.1f s ──\n%s",
                name, elapsed, tb,
            )
            send_alert(name, exc, tb)
            raise

    return _ctx()


def stage_fetch() -> list[dict]:
    """Scrape all sources and deduplicate against the DB."""
    with _stage("fetch"):
        from fetcher import fetch_all
        items = fetch_all(dedupe=True)
        log.info("fetch: %d new items stored", len(items))
        return items


def stage_enrich(new_items: list[dict]) -> tuple[int, int]:
    """
    Enrich unscored items from the last PIPELINE_ENRICH_HOURS hours.
    We query the DB directly rather than re-enriching the fetch result
    so that any items persisted by earlier runs also get scored.
    """
    with _stage("enrich"):
        import db
        from enricher import enrich_items

        hours = int(os.environ.get("PIPELINE_ENRICH_HOURS", "25"))
        limit = int(os.environ.get("PIPELINE_ENRICH_LIMIT", "200"))

        # get_unsent_items with min_score=None returns all unsent items;
        # enrich_items will skip those already scored.
        candidates = db.get_unsent_items(min_score=None, limit=limit)
        unscored = [i for i in candidates if i.get("relevance_score") is None]

        if not unscored:
            log.info("enrich: no unscored items to process")
            return 0, 0

        log.info("enrich: processing %d unscored items (limit %d)", len(unscored), limit)
        enriched, skipped = enrich_items(unscored, update_db=True)
        log.info("enrich: %d enriched, %d skipped/below-threshold", len(enriched), skipped)
        return len(enriched), skipped


def stage_digest() -> dict:
    """Render and send the HTML email digest."""
    with _stage("digest"):
        from digest import send_digest

        hours     = int(os.environ.get("DIGEST_HOURS", "24"))
        min_score = float(os.environ.get("DIGEST_MIN_SCORE", "0.5"))

        result = send_digest(hours=hours, min_score=min_score)
        log.info(
            "digest: sent '%s' — %d items across %s",
            result["subject"],
            result["items_sent"],
            ", ".join(result["jurisdictions"]) or "none",
        )
        return result


# ── Run once ──────────────────────────────────────────────────────────────────


def run_pipeline(dry_run: bool = False) -> bool:
    """
    Execute the full pipeline.  Returns True if all stages succeeded.

    With dry_run=True the digest is rendered and printed but not sent,
    and items are not marked as sent in the DB.
    """
    start = datetime.now(timezone.utc)
    log.info("═══ Pipeline run starting — %s ═══", start.strftime("%Y-%m-%d %H:%M UTC"))

    failed_stages: list[str] = []

    # ── 1. Fetch ──
    new_items: list[dict] = []
    try:
        new_items = stage_fetch()
    except Exception:
        failed_stages.append("fetch")
        # Fetcher failure is non-fatal: DB may still have unsent items
        # from a previous run that we can enrich and digest.

    # ── 2. Enrich ──
    try:
        stage_enrich(new_items)
    except Exception:
        failed_stages.append("enrich")
        # Enrich failure is non-fatal: we can still send summaries for
        # items enriched in prior runs.

    # ── 3. Digest ──
    if not dry_run:
        try:
            stage_digest()
        except Exception:
            failed_stages.append("digest")
    else:
        log.info("dry-run mode — skipping digest send")
        try:
            from digest import _get_recent_unsent, _group_by_jurisdiction, render_plaintext
            hours     = int(os.environ.get("DIGEST_HOURS", "24"))
            min_score = float(os.environ.get("DIGEST_MIN_SCORE", "0.5"))
            items  = _get_recent_unsent(hours=hours, min_score=min_score)
            groups = _group_by_jurisdiction(items)
            print(render_plaintext(groups, "[DRY RUN] Regulatory Digest"))
            log.info("dry-run: would send %d items", len(items))
        except Exception as exc:
            log.error("dry-run render error: %s", exc)

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    if failed_stages:
        log.error(
            "═══ Pipeline finished with failures: %s (%.0f s) ═══",
            ", ".join(failed_stages), elapsed,
        )
        return False

    log.info("═══ Pipeline completed successfully (%.0f s) ═══", elapsed)
    return True


# ── Scheduler ─────────────────────────────────────────────────────────────────


def _seconds_until(target_hour: int, target_minute: int) -> float:
    """Seconds from now until the next occurrence of HH:MM local time."""
    now = datetime.now()
    today_target = now.replace(
        hour=target_hour, minute=target_minute, second=0, microsecond=0
    )
    if today_target <= now:
        # Target already passed today — schedule for tomorrow
        from datetime import timedelta
        today_target += timedelta(days=1)
    return (today_target - now).total_seconds()


def run_scheduled(schedule_time: str = "08:00", dry_run: bool = False) -> None:
    """
    Block forever, running the pipeline once per day at schedule_time (HH:MM local).
    Catches unhandled exceptions so the scheduler loop never exits on its own.
    """
    try:
        hour, minute = [int(x) for x in schedule_time.split(":")]
    except ValueError:
        log.error("Invalid --schedule-time '%s', expected HH:MM", schedule_time)
        sys.exit(1)

    log.info("Scheduler active — pipeline will run daily at %02d:%02d local time", hour, minute)
    scheduler = sched.scheduler(time.time, time.sleep)

    def _enqueue():
        wait = _seconds_until(hour, minute)
        log.info(
            "Next run scheduled in %.0f min (at %s local time)",
            wait / 60,
            datetime.now().replace(
                hour=hour, minute=minute, second=0, microsecond=0
            ).strftime("%Y-%m-%d %H:%M"),
        )
        scheduler.enter(wait, 1, _fire)

    def _fire():
        try:
            run_pipeline(dry_run=dry_run)
        except Exception:
            log.error("Unhandled exception in scheduled run:\n%s", traceback.format_exc())
        finally:
            _enqueue()  # always reschedule regardless of outcome

    _enqueue()
    scheduler.run()  # blocks until the scheduler is empty (never, for us)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Regulatory monitoring pipeline orchestrator.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
────────
  # One-off run (fetch → enrich → send digest)
  python3 main.py --run-once

  # One-off dry run (fetch + enrich, print digest, no email sent)
  python3 main.py --run-once --dry-run

  # Start the daily 08:00 scheduler (blocks)
  python3 main.py

  # Schedule at a custom time
  python3 main.py --schedule-time 07:30

  # Schedule and dry-run every day (useful for testing the scheduler)
  python3 main.py --schedule-time 08:00 --dry-run
        """,
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Execute the pipeline once and exit (no scheduler).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip sending emails; print digest to stdout instead. Does not mark items sent.",
    )
    parser.add_argument(
        "--schedule-time",
        default=os.environ.get("PIPELINE_SCHEDULE_TIME", "08:00"),
        metavar="HH:MM",
        help="Daily run time in local time (default 08:00). Ignored with --run-once.",
    )
    args = parser.parse_args()

    if args.run_once:
        ok = run_pipeline(dry_run=args.dry_run)
        sys.exit(0 if ok else 1)
    else:
        run_scheduled(schedule_time=args.schedule_time, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
