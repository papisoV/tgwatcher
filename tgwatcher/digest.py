"""Market digest generation — turns signal_factors rows into a Chinese summary.

Computes a time window (cold start = 36h, otherwise last_digest_at → now, capped
at 36h), aggregates signals, calls LLM with json_mode=False (free-form prose),
persists result to the `digests` table.

Public API:
    generate_digest(storage, llm, llm_config) -> DigestResult
    get_latest_digest(storage) -> DigestResult | None
    list_digests(storage, limit) -> list[DigestResult]
"""
import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from sqlalchemy import select

from tgwatcher.models import Digest
from tgwatcher.signal_llm import LLMConfig, SignalLLMClient
from tgwatcher.storage import Storage
from tgwatcher.tz_utils import utc_now, utc_to_local, local_to_utc

logger = logging.getLogger(__name__)

COLD_START_HOURS = 36
MAX_WINDOW_HOURS = 36          # if last_digest_at is older, cap to 36h
MIN_SIGNALS_FOR_SUMMARY = 5    # below this, skip LLM and return "no change"

DIGEST_PROMPT_TEMPLATE = """你是一个加密货币市场分析师。基于以下结构化信号数据，写一份简洁的中文市场摘要。

数据时间窗口：{from_at} 到 {to_at}（本地时间）
信号总数：{total}

聚合统计：
- 净方向分：{net_direction:+.2f}（-1=全空，0=中性，+1=全多）
- 平均置信度：{avg_confidence:.2f}
- 平均幅度：{avg_magnitude:.2f}

按事件类型分组：
{event_type_breakdown}

按标的分组（top 5）：
{symbols_breakdown}

高置信事件（confidence >= 0.8，按时间倒序）：
{high_confidence_events}

请输出以下格式（纯文本，不要 markdown 代码块）：

【市场方向】
一句话总结净方向 + 驱动因素

【重点事件】
2-4 条最值得关注的事件，每条包含：时间、标的、方向、一句话原因

【风险提示】
1-2 条仍未消化的风险（halflife_min 较长的利空事件）

【一句话展望】
对接下来 12 小时的简短判断
"""


@dataclass
class DigestResult:
    id: int | None
    from_at: datetime          # UTC
    to_at: datetime            # UTC
    signal_count: int
    summary: str | None        # None when skipped (min-signals threshold)
    created_at: datetime | None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "from_at": utc_to_local(self.from_at).isoformat() if self.from_at else None,
            "to_at": utc_to_local(self.to_at).isoformat() if self.to_at else None,
            "signal_count": self.signal_count,
            "summary": self.summary,
            "created_at": utc_to_local(self.created_at).isoformat() if self.created_at else None,
        }


def _compute_window(storage: Storage) -> tuple[datetime, datetime]:
    """Determine [from_at, to_at] for next digest. Both UTC, naive."""
    now = utc_now().replace(tzinfo=None)
    with storage.get_session() as sess:
        latest = sess.execute(
            select(Digest).order_by(Digest.to_at.desc()).limit(1)
        ).scalar_one_or_none()
    if latest is None:
        return now - timedelta(hours=COLD_START_HOURS), now
    from_at = latest.to_at
    # Cap: if user hasn't run digest in days, don't pull huge window.
    if (now - from_at) > timedelta(hours=MAX_WINDOW_HOURS):
        from_at = now - timedelta(hours=MAX_WINDOW_HOURS)
    # Floor: if last digest was moments ago, still allow tiny window — caller
    # will skip if signal_count < MIN_SIGNALS_FOR_SUMMARY.
    return from_at, now


def _fetch_signals(storage: Storage, from_at: datetime, to_at: datetime) -> list[dict]:
    """Fetch is_signal=1 rows in [from_at, to_at] (UTC, naive)."""
    import sqlite3
    db_path = _resolve_db_path(storage)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(
        """
        SELECT message_id, direction, magnitude, urgency, confidence,
               halflife_min, symbols, event_type, reasoning, created_at
        FROM signal_factors
        WHERE is_signal = 1 AND llm_status = 'completed'
          AND created_at > ? AND created_at <= ?
        ORDER BY created_at DESC
        """,
        (from_at.isoformat(), to_at.isoformat()),
    )
    out = [dict(r) for r in cur.fetchall()]
    con.close()
    return out


def _resolve_db_path(storage: Storage) -> str:
    """Extract db path from storage engine URL."""
    url = str(storage.engine.url)
    # sqlite:///<path>
    return url.replace("sqlite:///", "", 1)


def _aggregate(signals: list[dict], from_at: datetime, to_at: datetime) -> dict:
    """Compute aggregate stats from signal list."""
    if not signals:
        return {"empty": True}

    total = len(signals)
    directions = [s["direction"] for s in signals]
    confidences = [s["confidence"] for s in signals]
    magnitudes = [s["magnitude"] for s in signals]

    net_direction = sum(directions) / total
    avg_conf = sum(confidences) / total
    avg_mag = sum(magnitudes) / total

    by_type: dict[str, list[dict]] = defaultdict(list)
    for s in signals:
        by_type[s["event_type"]].append(s)

    event_type_lines = []
    for et, items in sorted(by_type.items(), key=lambda x: -len(x[1])):
        avg_dir = sum(i["direction"] for i in items) / len(items)
        event_type_lines.append(
            f"  - {et}: {len(items)} 条, 平均方向 {avg_dir:+.2f}"
        )

    by_symbol: dict[str, list[dict]] = defaultdict(list)
    for s in signals:
        try:
            syms = json.loads(s["symbols"]) if s["symbols"] else ["*"]
        except (json.JSONDecodeError, TypeError):
            syms = ["*"]
        for sym in syms:
            by_symbol[sym].append(s)

    symbol_lines = []
    for sym, items in sorted(by_symbol.items(), key=lambda x: -len(x[1]))[:5]:
        avg_dir = sum(i["direction"] for i in items) / len(items)
        symbol_lines.append(
            f"  - {sym}: {len(items)} 条, 平均方向 {avg_dir:+.2f}"
        )

    high_conf = sorted(
        [s for s in signals if s["confidence"] >= 0.8],
        key=lambda s: s["created_at"],
        reverse=True,
    )[:8]
    high_conf_lines = []
    for s in high_conf:
        time_str = str(s["created_at"])[5:16].replace("T", " ")
        try:
            syms = json.loads(s["symbols"]) if s["symbols"] else ["*"]
            sym_str = "/".join(syms)
        except (json.JSONDecodeError, TypeError):
            sym_str = "*"
        reasoning = (s["reasoning"] or "")[:100]
        high_conf_lines.append(
            f"  - {time_str} [{sym_str}] dir={s['direction']:+.1f} "
            f"conf={s['confidence']:.2f} ({s['event_type']}): {reasoning}"
        )

    from_local = utc_to_local(from_at).strftime("%Y-%m-%d %H:%M")
    to_local = utc_to_local(to_at).strftime("%Y-%m-%d %H:%M")

    return {
        "empty": False,
        "from_at": from_local,
        "to_at": to_local,
        "total": total,
        "net_direction": net_direction,
        "avg_confidence": avg_conf,
        "avg_magnitude": avg_mag,
        "event_type_breakdown": "\n".join(event_type_lines) or "  (无)",
        "symbols_breakdown": "\n".join(symbol_lines) or "  (无)",
        "high_confidence_events": "\n".join(high_conf_lines) or "  (无)",
    }


def _build_prompt(agg: dict) -> str | None:
    if agg.get("empty"):
        return None
    return DIGEST_PROMPT_TEMPLATE.format(**agg)


def generate_digest(storage: Storage, llm: SignalLLMClient) -> DigestResult:
    """Compute window, fetch signals, call LLM, persist. Returns DigestResult.

    If signal_count < MIN_SIGNALS_FOR_SUMMARY, skips LLM and returns a
    "no significant change" stub (still persisted to history for audit).
    """
    from_at, to_at = _compute_window(storage)
    signals = _fetch_signals(storage, from_at, to_at)
    count = len(signals)

    if count < MIN_SIGNALS_FOR_SUMMARY:
        logger.info("Digest skipped: only %d signals in window (min=%d)",
                    count, MIN_SIGNALS_FOR_SUMMARY)
        summary = f"最近无显著变化（{count} 条信号，少于 {MIN_SIGNALS_FOR_SUMMARY} 条阈值）"
        digest = Digest(from_at=from_at, to_at=to_at, signal_count=count,
                        summary=summary)
        with storage.get_session() as sess:
            sess.add(digest)
            sess.commit()
            sess.refresh(digest)
        return DigestResult(id=digest.id, from_at=digest.from_at, to_at=digest.to_at,
                           signal_count=digest.signal_count,
                           summary=digest.summary, created_at=digest.created_at)

    agg = _aggregate(signals, from_at, to_at)
    prompt = _build_prompt(agg)

    # json_mode=False: free-form Chinese prose, not structured JSON.
    raw = llm._call_llm(prompt, max_tokens_override=1024, json_mode=False)

    digest = Digest(from_at=from_at, to_at=to_at, signal_count=count,
                    summary=raw.strip())
    with storage.get_session() as sess:
        sess.add(digest)
        sess.commit()
        sess.refresh(digest)
    return DigestResult(id=digest.id, from_at=digest.from_at, to_at=digest.to_at,
                       signal_count=digest.signal_count,
                       summary=digest.summary, created_at=digest.created_at)


def get_latest_digest(storage: Storage) -> DigestResult | None:
    with storage.get_session() as sess:
        d = sess.execute(
            select(Digest).order_by(Digest.created_at.desc()).limit(1)
        ).scalar_one_or_none()
        if d is None:
            return None
        return DigestResult(id=d.id, from_at=d.from_at, to_at=d.to_at,
                           signal_count=d.signal_count, summary=d.summary,
                           created_at=d.created_at)


def list_digests(storage: Storage, limit: int = 20) -> list[DigestResult]:
    with storage.get_session() as sess:
        rows = sess.execute(
            select(Digest).order_by(Digest.created_at.desc()).limit(limit)
        ).scalars().all()
        return [DigestResult(id=d.id, from_at=d.from_at, to_at=d.to_at,
                            signal_count=d.signal_count, summary=d.summary,
                            created_at=d.created_at) for d in rows]
