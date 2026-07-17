"""Helper script for tg-analyze skill — fetch messages and write Claude analysis results."""

import json
import os
import re
import sys

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

USAGE = """
Usage:
  python tg_analyze_helper.py preanalyze <db_path> [--count N] [--chat-id ID] [--from DATE] [--to DATE] [--overwrite] [--all]
  python tg_analyze_helper.py fetch <db_path> [--count N] [--chat-id ID] [--from DATE] [--to DATE] [--overwrite] [--all] [--noise-stats]
  python tg_analyze_helper.py write <db_path> <results_json_file> [--mode factor|timeline|theme|deep]
  python tg_analyze_helper.py stats <db_path>
  python tg_analyze_helper.py init <db_path>
"""

# ── Noise pre-filter rules ──────────────────────────────────────────
# High-confidence noise patterns — only exclude messages that are VERY likely noise.
# Borderline cases are left for Claude to judge.
NOISE_PATTERNS: dict[str, list[str]] = {
    "price_quote": [
        "突破$", "跌破$", "站上$", "逼近$", "触及$",
        "创历史新高", "创年内新低", "创历史新低",
        "涨幅%", "跌幅%", "涨%", "跌%",
        "24h涨", "24h跌", "24H涨", "24H跌",
        # Chinese price patterns
        "突破.*美元", "跌破.*美元", "站上.*美元",
        "价格突破", "价格跌破",
        "比特币价格", "BTC价格",
        "价值.*美元的比特币", "价值.*美元的以太",
    ],
    "etf_flow": [
        "ETF流入", "ETF流出", "资金净流入", "资金净流出",
        "ETF净流入", "ETF净流出", "BTC ETF", "ETH ETF",
        "现货ETF", "ETF资金",
    ],
    "exchange_listing": [
        "币安上线", "上线永续", "Launchpool", "新币挖矿",
        "上线合约", "开放交易", "上架交易对",
    ],
    "airdrop": [
        "空投领取", "领取空投", "TGE", "代币生成",
    ],
    "routine_data": [
        "恐惧贪婪指数", "多空比", "资金费率",
        "持仓量变化", "清算数据", "爆仓",
        "未平仓合约", "OI变化",
        # Chinese routine data patterns
        "空头头寸被清算", "多头头寸被清算",
        "标普.*指数.*上涨", "标普.*指数.*下跌",
        "纳斯达克指数.*上涨", "纳斯达克指数.*下跌",
        "指数收盘", "指数开盘",
        "原油库存", "制造业指数",
    ],
    "stock_news": [
        # Non-crypto stock news that shouldn't be crypto signals
        "股价.*跌破", "股价.*突破",
        "IPO发行价", "可转换优先",
        "CONV SR", "票据到期",
    ],
}

# Macro-signal keywords — if a message matches ANY of these, it's NEVER filtered as noise
# even if it also matches a noise pattern (e.g., "BTC创历史新高，美联储降息推动" should be kept)
MACRO_SIGNAL_KEYWORDS: list[str] = [
    "美联储", "Fed", "降息", "加息", "利率", "CPI", "GDP", "非农", "NFP",
    "通胀", "通缩", "量化宽松", "QE", "QT", "缩表",
    "SEC", "监管", "合规", "禁令", "罚款", "起诉", "诉讼", "审查",
    "制裁", "战争", "冲突", "地缘", "贸易战", "关税",
    "特朗普", "Trump", "拜登", "Biden", "白宫", "国会",
    "黑客", "被盗", "漏洞", "攻击", "跑路", "Rug", "闪电贷",
    "法案", "立法", "政策", "合规要求",
    "银行", "破产", "救助", "系统性风险",
    "欧央行", "BOJ", "日本央行", "英国央行",
]


# ── Text deduplication ──────────────────────────────────────────────
def _dedup_tradfin_translation(text: str) -> str:
    """
    Remove duplicate Chinese translation block from Tradfin messages.

    Pattern: **Tradfin：...content...**\\nTradfin：...same content in Chinese...
    The second block is a machine translation of the first and adds no value.
    Also strips separator lines (————————————).
    """
    if not text or "Tradfin" not in text:
        return text

    # Strip separator lines
    text = re.sub(r'—{5,}', '', text)

    # Pattern: bold section ends with **, newline, then Tradfin： starts translation
    parts = re.split(r'\*\*\s*\n(?=Tradfin[：:])', text, maxsplit=1)
    if len(parts) == 2:
        result = parts[0].strip()
        result = re.sub(r'^\*\*', '', result).strip()
        result = re.sub(r'\*\*$', '', result).strip()
        return result

    # Fallback: remove everything from second Tradfin： line onward
    lines = text.split('\n')
    seen_tradfin = False
    kept_lines = []
    for line in lines:
        if re.match(r'^Tradfin[：:]', line.strip()):
            if seen_tradfin:
                break
            seen_tradfin = True
        kept_lines.append(line)
    return '\n'.join(kept_lines) if len(kept_lines) < len(lines) else text


# ── Token extraction ────────────────────────────────────────────────
TOKEN_PATTERN = re.compile(
    r'\$([A-Z]{2,10})'
    r'|(?<!\w)(BTC|ETH|SOL|BNB|XRP|ADA|DOGE|AVAX|DOT|MATIC|LINK|UNI|AAVE|ARB|OP|APT|SUI|SEI|NEAR|FTM|ATOM|FIL|INJ|TIA|JUP|WIF|PEPE|SHIB|TRUMP|DJT)(?!\w)'
)

# ── Category-to-factor mapping ──────────────────────────────────────
CATEGORY_FACTOR_MAP: dict[str, dict] = {
    "event_regulatory": {
        "event_type": "regulatory", "scope": "macro",
        "sentiment": 2, "intensity": 4, "urgency": 4, "action_hint": "hedge",
    },
    "event_macro": {
        "event_type": "macro", "scope": "macro",
        "sentiment": 3, "intensity": 4, "urgency": 3, "action_hint": "watch",
    },
    "event_exploit": {
        "event_type": "exploit", "scope": "micro",
        "sentiment": 1, "intensity": 5, "urgency": 5, "action_hint": "short",
    },
    "event_listing": {
        "event_type": "listing", "scope": "micro",
        "sentiment": 4, "intensity": 3, "urgency": 2, "action_hint": "watch",
    },
    "event_partnership": {
        "event_type": "partnership", "scope": "micro",
        "sentiment": 4, "intensity": 3, "urgency": 2, "action_hint": "watch",
    },
    "bullish": {
        "event_type": "market", "scope": "micro",
        "sentiment": 4, "intensity": 3, "urgency": 2, "action_hint": "long",
    },
    "bearish": {
        "event_type": "market", "scope": "micro",
        "sentiment": 2, "intensity": 3, "urgency": 2, "action_hint": "short",
    },
    "scope_macro": {"scope": "macro"},
    "scope_micro": {"scope": "micro"},
    "urgency_high": {"urgency": 5, "intensity": 4},
}


def _extract_tokens(text: str) -> list[str]:
    """Extract crypto token symbols from text."""
    tokens: set[str] = set()
    for match in TOKEN_PATTERN.finditer(text):
        token = match.group(1) or match.group(2)
        if token:
            tokens.add(token.upper())
    return sorted(tokens)


def _compute_sentiment_label(sentiment: int) -> str:
    """Compute sentiment label from numeric value."""
    if sentiment <= 2:
        return "bearish"
    if sentiment == 3:
        return "neutral"
    return "bullish"


def _preanalyze_message(text: str, msg_id: int, chat_id: int) -> dict:
    """
    Pre-analyze a single message using local rules.

    Returns a factor dict with is_signal, pre-filled factor values,
    and a confidence field ("high" = no Claude review needed, "low" = review).
    """
    from tgwatcher.signal_filter import KeywordFilter, DEFAULT_KEYWORD_RULES

    clean_text = _dedup_tradfin_translation(text)

    # Step 1: Check if high-confidence noise (price quotes, routine data, etc.)
    is_noise, noise_reason = _is_likely_noise(clean_text)

    # Step 2: Run keyword filter
    kf = KeywordFilter({"keywords": DEFAULT_KEYWORD_RULES})
    filter_result = kf.filter(clean_text)

    # Determine if keyword filter found a "strong" signal category
    categories = filter_result.preliminary_factors.get("matched_categories", [])
    has_strong_signal = any(c in ("event_regulatory", "event_macro", "event_exploit") for c in categories)

    # Noise classification logic:
    # - No keyword match at all → noise
    # - Noise pattern matched AND no strong signal category → noise
    #   (e.g., "标普500上涨0.4%" matches bullish/上涨 but is just market data)
    # - Noise pattern matched BUT strong signal present → borderline, keep as signal candidate
    # - No noise pattern, keyword match → signal candidate
    is_definite_noise = (not filter_result.passed) or (is_noise and not has_strong_signal)

    if is_definite_noise:
        return {
            "message_id": msg_id,
            "chat_id": chat_id,
            "is_signal": False,
            "sentiment": 3,
            "sentiment_label": "neutral",
            "event_type": "market",
            "scope": "micro",
            "intensity": 1,
            "urgency": 1,
            "action_hint": "none",
            "affected_tokens": [],
            "reasoning": f"noise: {noise_reason}, no strong signal category" if is_noise else "noise: no signal keywords matched",
            "confidence": "high",
            "matched_categories": [],
            "matched_keywords": filter_result.matched_keywords,
        }

    # Even if keyword filter passed, check if it's noise with macro override
    # e.g., "特朗普硬币视频" matches "特朗普" but is just a meme, not a signal
    # If the ONLY match is from MACRO_SIGNAL_KEYWORDS and noise pattern also matched,
    # classify as low-confidence signal (Claude will review)
    categories = filter_result.preliminary_factors.get("matched_categories", [])

    # Signal candidate — build pre-filled factors from categories
    factors: dict = {
        "message_id": msg_id,
        "chat_id": chat_id,
        "is_signal": True,
        "sentiment": 3,
        "sentiment_label": "neutral",
        "event_type": "other",
        "scope": "micro",
        "intensity": 3,
        "urgency": 3,
        "action_hint": "watch",
        "affected_tokens": _extract_tokens(clean_text),
        "reasoning": "",
        "confidence": "low",
        "matched_categories": categories,
        "matched_keywords": filter_result.matched_keywords,
    }

    # If noise pattern also matched, mark as low-confidence (Claude should double-check)
    if is_noise:
        factors["confidence"] = "low"
        factors["reasoning"] = f"borderline: noise pattern ({noise_reason}) but signal keywords present"

    # Merge category-specific overrides
    for cat in categories:
        if cat in CATEGORY_FACTOR_MAP:
            for key, val in CATEGORY_FACTOR_MAP[cat].items():
                factors[key] = val

    # Primary event_type from most specific event category
    event_cats = [c for c in categories if c.startswith("event_")]
    if event_cats:
        primary = event_cats[0]
        if primary in CATEGORY_FACTOR_MAP:
            factors["event_type"] = CATEGORY_FACTOR_MAP[primary]["event_type"]

    # Override scope if scope_* categories present
    scope_cats = [c for c in categories if c.startswith("scope_")]
    if scope_cats:
        last_scope = scope_cats[-1]
        if last_scope in CATEGORY_FACTOR_MAP and "scope" in CATEGORY_FACTOR_MAP[last_scope]:
            factors["scope"] = CATEGORY_FACTOR_MAP[last_scope]["scope"]

    # Override urgency if urgency_high present
    if "urgency_high" in categories:
        factors["urgency"] = 5
        factors["intensity"] = max(factors.get("intensity", 3), 4)

    # Compute sentiment_label from final sentiment
    factors["sentiment_label"] = _compute_sentiment_label(factors["sentiment"])

    # Macro events with no specific tokens → empty affected_tokens
    if factors["scope"] == "macro" and factors["event_type"] in ("macro", "regulatory"):
        factors["affected_tokens"] = []

    # Auto-reasoning from matched categories
    if not factors["reasoning"]:
        cat_labels = {
            "event_regulatory": "监管事件",
            "event_macro": "宏观经济",
            "event_exploit": "安全事件",
            "event_listing": "上线事件",
            "event_partnership": "合作事件",
            "bullish": "利好信号",
            "bearish": "利空信号",
        }
        labels = [cat_labels.get(c, c) for c in categories if c in cat_labels]
        if labels:
            factors["reasoning"] = f"预分类: {'+'.join(labels)}; 关键词: {','.join(filter_result.matched_keywords[:5])}"

    return factors


def _is_likely_noise(text: str) -> tuple[bool, str]:
    """
    Check if a message is high-confidence noise.

    Returns (is_noise, reason). Only returns True when very confident.
    Messages matching macro-signal keywords are NEVER classified as noise.
    """
    if not text:
        return False, ""

    # If any macro-signal keyword is present, keep it — never filter
    text_lower = text.lower()
    for kw in MACRO_SIGNAL_KEYWORDS:
        if kw.lower() in text_lower:
            return False, ""

    # Check noise patterns
    for category, patterns in NOISE_PATTERNS.items():
        for pattern in patterns:
            if "$" in pattern:
                # Dollar-sign patterns: $ means "digit" (e.g., "突破$60000")
                regex_pattern = pattern.replace("$", r"\d")
                if re.search(regex_pattern, text):
                    return True, f"noise:{category}"
            elif ".*" in pattern or "%" in pattern:
                # Regex patterns (e.g., "突破.*美元", "涨幅%")
                try:
                    if re.search(pattern, text):
                        return True, f"noise:{category}"
                except re.error:
                    if pattern.lower() in text_lower:
                        return True, f"noise:{category}"
            else:
                if pattern.lower() in text_lower:
                    return True, f"noise:{category}"

    return False, ""


def _get_storage(db_path: str):
    """Import and return Storage instance."""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from tgwatcher.storage import Storage
    return Storage(db_path)


def cmd_preanalyze(db_path: str, args: list[str]):
    """Pre-analyze messages: local rule engine fills factors, output separates noise from signal candidates."""
    from sqlalchemy import text
    storage = _get_storage(db_path)

    # Parse args
    count = 100
    chat_id = None
    date_from = None
    date_to = None
    overwrite = False
    include_all = False

    i = 0
    while i < len(args):
        if args[i] == "--count" and i + 1 < len(args):
            count = int(args[i + 1]); i += 2
        elif args[i] == "--chat-id" and i + 1 < len(args):
            chat_id = int(args[i + 1]); i += 2
        elif args[i] == "--from" and i + 1 < len(args):
            date_from = args[i + 1]; i += 2
        elif args[i] == "--to" and i + 1 < len(args):
            date_to = args[i + 1]; i += 2
        elif args[i] == "--overwrite":
            overwrite = True; i += 1
        elif args[i] == "--all":
            include_all = True; i += 1
        else:
            i += 1

    # Fetch messages
    from datetime import datetime
    from tgwatcher.tz_utils import local_to_utc, utc_to_local
    df = local_to_utc(datetime.fromisoformat(date_from)) if date_from else None
    dt = None
    if date_to:
        dt_local = datetime.fromisoformat(date_to)
        if dt_local.hour == 0 and dt_local.minute == 0 and dt_local.second == 0:
            dt_local = dt_local.replace(hour=23, minute=59, second=59, microsecond=999999)
        dt = local_to_utc(dt_local)

    fetch_count = count * 2
    result = storage.query_messages(
        chat_id=chat_id, date_from=df, date_to=dt,
        page=1, page_size=fetch_count,
    )
    messages = result["messages"]

    # Filter out already-analyzed (unless overwrite)
    if not overwrite and messages:
        with storage.engine.connect() as conn:
            pairs = [(m["message_id"], m["chat_id"]) for m in messages]
            chunk_size = 900
            existing_ids: set[tuple[int, int]] = set()
            for c_start in range(0, len(pairs), chunk_size):
                chunk = pairs[c_start:c_start + chunk_size]
                placeholders = ", ".join(f"(:mid{j}, :cid{j})" for j in range(len(chunk)))
                params = {}
                for j, (mid, cid) in enumerate(chunk):
                    params[f"mid{j}"] = mid
                    params[f"cid{j}"] = cid
                rows = conn.execute(text(
                    f"SELECT message_id, chat_id FROM claude_factors "
                    f"WHERE (message_id, chat_id) IN ({placeholders}) "
                    f"AND llm_status='completed'"
                ), params).fetchall()
                for row in rows:
                    existing_ids.add((row[0], row[1]))
        messages = [m for m in messages if (m["message_id"], m["chat_id"]) not in existing_ids]

    messages = messages[:count]

    # Run pre-analysis on each message
    noise_results: list[dict] = []
    signal_candidates: list[dict] = []

    for m in messages:
        text = m.get("text") or ""
        pre = _preanalyze_message(text, m["message_id"], m["chat_id"])

        # Add metadata for display (convert UTC to local time)
        date_val = m.get("date")
        if date_val:
            try:
                pre["date"] = utc_to_local(datetime.fromisoformat(date_val)).isoformat()[:16]
            except (ValueError, TypeError):
                pre["date"] = date_val[:16]
        else:
            pre["date"] = ""
        pre["chat_title"] = m.get("chat_title") or ""
        pre["text_compressed"] = _dedup_tradfin_translation(text[:300])
        pre["text_original_length"] = len(text)

        if not pre["is_signal"] and not include_all:
            noise_results.append(pre)
        else:
            signal_candidates.append(pre)

    # Build compact display for Claude — only signal candidates
    signal_lines = []
    for idx, s in enumerate(signal_candidates):
        line = (
            f"[{idx}] msg={s['message_id']} chat={s['chat_id']} | "
            f"{s['date']} | {s['chat_title']}\n"
            f"  Text: {s['text_compressed']}\n"
            f"  Pre: event={s['event_type']} sent={s['sentiment']}({s['sentiment_label']}) "
            f"scope={s['scope']} int={s['intensity']} urg={s['urgency']} "
            f"hint={s['action_hint']} tokens={s['affected_tokens']}\n"
            f"  Reason: {s['reasoning']}"
        )
        signal_lines.append(line)

    output = {
        "signal_candidates": signal_candidates,
        "noise_count": len(noise_results),
        "noise_results": noise_results,
        "signal_display": "\n".join(signal_lines),
        "total_fetched": len(messages),
        "total_available": result["total"],
    }

    print(json.dumps(output, ensure_ascii=False, default=str))


def cmd_init(db_path: str):
    """Create claude_factors table if not exists, and migrate missing columns."""
    from sqlalchemy import text
    storage = _get_storage(db_path)
    with storage.engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS claude_factors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                sentiment INTEGER,
                sentiment_label VARCHAR(20),
                event_type VARCHAR(32),
                scope VARCHAR(16),
                intensity INTEGER,
                urgency INTEGER,
                affected_tokens TEXT,
                action_hint VARCHAR(16),
                reasoning TEXT,
                cross_refs TEXT,
                is_signal BOOLEAN DEFAULT 1,
                analysis_mode VARCHAR(16),
                llm_status VARCHAR(20) DEFAULT 'completed',
                llm_model VARCHAR(64),
                llm_raw TEXT,
                factor_version INTEGER DEFAULT 1,
                created_at DATETIME DEFAULT (datetime('now')),
                updated_at DATETIME DEFAULT (datetime('now')),
                UNIQUE(message_id, chat_id)
            )
        """))
        # Migrate: add is_signal column if missing (older tables)
        cols = [row[1] for row in conn.execute(text("PRAGMA table_info(claude_factors)"))]
        if "is_signal" not in cols:
            conn.execute(text("ALTER TABLE claude_factors ADD COLUMN is_signal BOOLEAN DEFAULT 1"))
        if "analysis_mode" not in cols:
            conn.execute(text("ALTER TABLE claude_factors ADD COLUMN analysis_mode VARCHAR(16)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_claude_factors_chat_id ON claude_factors(chat_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_claude_factors_action_hint ON claude_factors(action_hint)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_claude_factors_is_signal ON claude_factors(is_signal)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_claude_factors_llm_status ON claude_factors(llm_status)"))
        conn.commit()
    print("OK: claude_factors table ready")


def cmd_fetch(db_path: str, args: list[str]):
    """Fetch messages from DB, output as JSON for analysis."""
    from sqlalchemy import text
    storage = _get_storage(db_path)

    # Parse args
    count = 50
    chat_id = None
    date_from = None
    date_to = None
    overwrite = False
    include_all = False
    noise_stats = False

    i = 0
    while i < len(args):
        if args[i] == "--count" and i + 1 < len(args):
            count = int(args[i + 1]); i += 2
        elif args[i] == "--chat-id" and i + 1 < len(args):
            chat_id = int(args[i + 1]); i += 2
        elif args[i] == "--from" and i + 1 < len(args):
            date_from = args[i + 1]; i += 2
        elif args[i] == "--to" and i + 1 < len(args):
            date_to = args[i + 1]; i += 2
        elif args[i] == "--overwrite":
            overwrite = True; i += 1
        elif args[i] == "--all":
            include_all = True; i += 1
        elif args[i] == "--noise-stats":
            noise_stats = True; i += 1
        else:
            i += 1

    # Fetch messages — over-fetch to account for noise filtering
    # Request more than needed so we still get ~count after filtering
    fetch_count = count * 3 if not include_all else count
    from datetime import datetime
    from tgwatcher.tz_utils import local_to_utc, utc_to_local
    df = local_to_utc(datetime.fromisoformat(date_from)) if date_from else None
    dt = None
    if date_to:
        dt_local = datetime.fromisoformat(date_to)
        if dt_local.hour == 0 and dt_local.minute == 0 and dt_local.second == 0:
            dt_local = dt_local.replace(hour=23, minute=59, second=59, microsecond=999999)
        dt = local_to_utc(dt_local)

    result = storage.query_messages(
        chat_id=chat_id, date_from=df, date_to=dt,
        page=1, page_size=fetch_count,
    )
    messages = result["messages"]

    # If not overwrite, batch-filter out messages already in claude_factors
    if not overwrite and messages:
        with storage.engine.connect() as conn:
            # Batch query instead of per-row
            pairs = [(m["message_id"], m["chat_id"]) for m in messages]
            # SQLite has variable limit (999), chunk if needed
            chunk_size = 900
            existing_ids: set[tuple[int, int]] = set()
            for c_start in range(0, len(pairs), chunk_size):
                chunk = pairs[c_start:c_start + chunk_size]
                placeholders = ", ".join(f"(:mid{j}, :cid{j})" for j in range(len(chunk)))
                params = {}
                for j, (mid, cid) in enumerate(chunk):
                    params[f"mid{j}"] = mid
                    params[f"cid{j}"] = cid
                rows = conn.execute(text(
                    f"SELECT message_id, chat_id FROM claude_factors "
                    f"WHERE (message_id, chat_id) IN ({placeholders}) "
                    f"AND llm_status='completed'"
                ), params).fetchall()
                for row in rows:
                    existing_ids.add((row[0], row[1]))
        messages = [m for m in messages if (m["message_id"], m["chat_id"]) not in existing_ids]

    # Noise pre-filter (unless --all)
    noise_counts: dict[str, int] = {}
    total_before_filter = len(messages)
    if not include_all:
        filtered = []
        for m in messages:
            text = m.get("text") or ""
            is_noise, reason = _is_likely_noise(text)
            if is_noise:
                noise_counts[reason] = noise_counts.get(reason, 0) + 1
            else:
                filtered.append(m)
        messages = filtered

    # Trim to requested count (newest first since query_messages returns DESC)
    messages = messages[:count]

    # Also fetch existing DeepSeek factors for context
    factors_map = {}
    for msg in messages:
        sf = storage.get_signal_factor(msg["message_id"], msg["chat_id"])
        if sf:
            factors_map[f"{msg['message_id']}_{msg['chat_id']}"] = {
                "sentiment": sf.get("sentiment"),
                "sentiment_label": sf.get("sentiment_label"),
                "event_type": sf.get("event_type"),
                "urgency": sf.get("urgency"),
                "reasoning": sf.get("reasoning"),
            }

    # Format messages for prompt
    formatted_lines = []
    for i, m in enumerate(messages):
        text = m.get("text") or ""
        if len(text) > 500:
            text = text[:500] + "..."
        date_val = m.get("date")
        if date_val:
            try:
                date_str = utc_to_local(datetime.fromisoformat(date_val)).isoformat()[:16]
            except (ValueError, TypeError):
                date_str = date_val[:16]
        else:
            date_str = ""
        chat_title = m.get("chat_title") or ""
        formatted_lines.append(
            f"[{i}] msg_id={m['message_id']} chat_id={m['chat_id']} | "
            f"[{chat_title}] | {date_str} | {text}"
        )

    output = {
        "messages": messages,
        "formatted": "\n".join(formatted_lines),
        "existing_factors": factors_map,
        "total_available": result["total"],
        "fetched": len(messages),
        "noise_filter": {
            "enabled": not include_all,
            "total_before_filter": total_before_filter,
            "filtered_out": total_before_filter - len(messages),
            "noise_breakdown": noise_counts,
        },
    }

    if noise_stats:
        print(f"Noise Filter Statistics:")
        print(f"  Messages before filter: {total_before_filter}")
        print(f"  Filtered as noise:      {total_before_filter - len(messages)}")
        print(f"  Kept for analysis:      {len(messages)}")
        print(f"  Filter ratio:           {(total_before_filter - len(messages)) * 100 // max(total_before_filter, 1)}%")
        if noise_counts:
            print(f"  Breakdown:")
            for reason, cnt in sorted(noise_counts.items(), key=lambda x: -x[1]):
                print(f"    {reason}: {cnt}")
        print()

    print(json.dumps(output, ensure_ascii=False, default=str))


def cmd_write(db_path: str, results_file: str, args: list[str]):
    """Write analysis results from JSON file to claude_factors table."""
    from sqlalchemy import text
    storage = _get_storage(db_path)

    mode = "factor"
    i = 0
    while i < len(args):
        if args[i] == "--mode" and i + 1 < len(args):
            mode = args[i + 1]; i += 2
        else:
            i += 1

    with open(results_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Support both array and object with per_message key
    if isinstance(data, dict):
        items = data.get("per_message", [data])
    else:
        items = data

    written = 0
    with storage.engine.connect() as conn:
        for item in items:
            row = {
                "message_id": item["message_id"],
                "chat_id": item["chat_id"],
                "sentiment": item.get("sentiment"),
                "sentiment_label": item.get("sentiment_label"),
                "event_type": item.get("event_type"),
                "scope": item.get("scope"),
                "intensity": item.get("intensity"),
                "urgency": item.get("urgency"),
                "affected_tokens": json.dumps(item.get("affected_tokens", []), ensure_ascii=False),
                "action_hint": item.get("action_hint"),
                "reasoning": item.get("reasoning"),
                "cross_refs": json.dumps(item.get("cross_refs", []), ensure_ascii=False),
                "is_signal": 1 if item.get("is_signal", True) else 0,
                "analysis_mode": mode,
                "llm_status": "completed",
                "llm_model": "claude",
                "llm_raw": json.dumps(item, ensure_ascii=False),
            }
            existing = conn.execute(text(
                "SELECT id FROM claude_factors WHERE message_id=:mid AND chat_id=:cid"
            ), {"mid": row["message_id"], "cid": row["chat_id"]}).fetchone()
            if existing:
                set_parts = [f"{k}=:{k}" for k in row.keys() if k not in ("message_id", "chat_id")]
                set_clause = ", ".join(set_parts)
                conn.execute(text(
                    f"UPDATE claude_factors SET {set_clause}, updated_at=datetime('now') "
                    f"WHERE message_id=:message_id AND chat_id=:chat_id"
                ), row)
            else:
                cols = ", ".join(row.keys())
                vals = ", ".join(f":{k}" for k in row.keys())
                conn.execute(text(f"INSERT INTO claude_factors ({cols}) VALUES ({vals})"), row)
            written += 1
        conn.commit()

    print(f"OK: wrote {written} results to claude_factors (mode={mode})")


def cmd_stats(db_path: str):
    """Print analysis coverage statistics."""
    from sqlalchemy import text
    storage = _get_storage(db_path)
    with storage.engine.connect() as conn:
        total = conn.execute(text(
            "SELECT COUNT(*) FROM messages WHERE is_deleted=0 AND text IS NOT NULL"
        )).scalar()
        ds_done = conn.execute(text(
            "SELECT COUNT(*) FROM signal_factors WHERE llm_status='completed'"
        )).scalar()
        claude_done = conn.execute(text(
            "SELECT COUNT(*) FROM claude_factors WHERE llm_status='completed'"
        )).scalar()
        claude_signals = conn.execute(text(
            "SELECT COUNT(*) FROM claude_factors WHERE is_signal=1 AND llm_status='completed'"
        )).scalar()
        claude_noise = conn.execute(text(
            "SELECT COUNT(*) FROM claude_factors WHERE is_signal=0 AND llm_status='completed'"
        )).scalar()
        claude_by_mode = {}
        for row in conn.execute(text(
            "SELECT analysis_mode, COUNT(*) as cnt FROM claude_factors "
            "WHERE llm_status='completed' GROUP BY analysis_mode"
        )):
            claude_by_mode[row[0] or "unknown"] = row[1]
        action_dist = {}
        for row in conn.execute(text(
            "SELECT action_hint, COUNT(*) as cnt FROM claude_factors "
            "WHERE action_hint IS NOT NULL GROUP BY action_hint"
        )):
            action_dist[row[0]] = row[1]
        # Top affected tokens (signals only)
        token_counts: dict[str, int] = {}
        for row in conn.execute(text(
            "SELECT affected_tokens FROM claude_factors "
            "WHERE affected_tokens IS NOT NULL AND is_signal=1"
        )):
            try:
                tokens = json.loads(row[0])
                for t in tokens:
                    token_counts[t] = token_counts.get(t, 0) + 1
            except (json.JSONDecodeError, TypeError):
                pass
        top_tokens = sorted(token_counts.items(), key=lambda x: -x[1])[:10]
        # Event type distribution (signals only)
        event_dist = {}
        for row in conn.execute(text(
            "SELECT event_type, COUNT(*) as cnt FROM claude_factors "
            "WHERE is_signal=1 AND event_type IS NOT NULL GROUP BY event_type"
        )):
            event_dist[row[0]] = row[1]

    pct_ds = ds_done * 100 // max(total, 1)
    pct_claude = claude_done * 100 // max(total, 1)
    signal_pct = claude_signals * 100 // max(claude_done, 1)
    print(f"TGWatcher Analysis Coverage:")
    print(f"  Total messages:       {total}")
    print(f"  DeepSeek analyzed:    {ds_done} ({pct_ds}%)")
    print(f"  Claude analyzed:      {claude_done} ({pct_claude}%)")
    print(f"    Signals (is_signal=1): {claude_signals} ({signal_pct}%)")
    print(f"    Noise  (is_signal=0):  {claude_noise} ({100 - signal_pct}%)")
    print(f"  Claude by mode:       {claude_by_mode}")
    print(f"  Signal event types:   {event_dist}")
    print(f"  Action hints:         {action_dist}")
    print(f"  Top affected tokens:  {top_tokens}")


def main():
    from tgwatcher.tz_utils import set_tz_offset
    set_tz_offset(8)  # Default UTC+8; matches config.yaml default

    if len(sys.argv) < 3:
        print(USAGE)
        sys.exit(1)

    command = sys.argv[1]
    db_path = sys.argv[2]
    rest_args = sys.argv[3:]

    if command == "init":
        cmd_init(db_path)
    elif command == "preanalyze":
        cmd_preanalyze(db_path, rest_args)
    elif command == "fetch":
        cmd_fetch(db_path, rest_args)
    elif command == "write":
        if not rest_args:
            print("Error: write command requires a results JSON file path")
            sys.exit(1)
        results_file = rest_args[0]
        cmd_write(db_path, results_file, rest_args[1:])
    elif command == "stats":
        cmd_stats(db_path)
    else:
        print(f"Unknown command: {command}")
        print(USAGE)
        sys.exit(1)


if __name__ == "__main__":
    main()
