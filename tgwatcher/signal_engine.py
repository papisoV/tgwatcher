"""Signal engine - orchestrates keyword filtering and LLM refinement for news factor extraction."""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field

from tgwatcher.signal_filter import KeywordFilter
from tgwatcher.signal_llm import SignalLLMClient, compute_sentiment_label, LLMRefineError
from tgwatcher.storage import Storage

logger = logging.getLogger(__name__)


@dataclass
class BatchResult:
    total: int = 0
    completed: int = 0
    failed: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)


class SignalEngine:
    def __init__(self, storage: Storage, keyword_filter: KeywordFilter,
                 llm: SignalLLMClient, config: dict):
        self._storage = storage
        self._filter = keyword_filter
        self._llm = llm
        self._config = config
        self._batch_size = config.get("batch_size", 50)
        self._llm_delay = config.get("llm_delay", 1.0)
        self._factor_version = config.get("factor_version", 1)

    def process_message(self, msg: dict) -> dict | None:
        """Process a single message through keyword filter and LLM. Returns factor dict or None."""
        text = msg.get("text")
        min_len = self._config.get("filter", {}).get("min_text_length", 10)

        if not text or len(text) < min_len:
            return None

        message_id = msg["message_id"]
        chat_id = msg["chat_id"]

        # Step 1: Keyword filter
        filter_result = self._filter.filter(text)

        if not filter_result.passed:
            factor_data = {
                "message_id": message_id,
                "chat_id": chat_id,
                "llm_status": "skipped",
                "filter_result": "rejected",
                "matched_keywords": json.dumps(filter_result.matched_keywords, ensure_ascii=False),
                "keyword_preliminary": json.dumps(filter_result.preliminary_factors, ensure_ascii=False),
                "factor_version": self._factor_version,
            }
            self._storage.save_signal_factor(factor_data)
            return factor_data

        # Step 2: Save processing state, then run LLM
        factor_data = {
            "message_id": message_id,
            "chat_id": chat_id,
            "llm_status": "processing",
            "filter_result": "passed",
            "matched_keywords": json.dumps(filter_result.matched_keywords, ensure_ascii=False),
            "keyword_preliminary": json.dumps(filter_result.preliminary_factors, ensure_ascii=False),
            "factor_version": self._factor_version,
        }
        self._storage.save_signal_factor(factor_data)

        try:
            llm_result = self._llm.refine(text, filter_result.preliminary_factors)
            # Merge LLM result into factor data
            factor_data.update({
                "sentiment": llm_result["sentiment"],
                "sentiment_label": compute_sentiment_label(llm_result["sentiment"]),
                "event_type": llm_result["event_type"],
                "scope": llm_result["scope"],
                "intensity": llm_result["intensity"],
                "urgency": llm_result["urgency"],
                "reasoning": llm_result.get("reasoning", ""),
                "llm_status": "completed",
                "llm_model": self._config.get("llm", {}).get("model", ""),
                "llm_raw": json.dumps(llm_result, ensure_ascii=False),
            })
        except (LLMRefineError, Exception) as e:
            logger.warning("LLM refinement failed for msg %d: %s", message_id, e)
            factor_data.update({
                "llm_status": "failed",
                "llm_error": str(e)[:256],
            })

        self._storage.save_signal_factor(factor_data)
        return factor_data

    def process_batch(self, chat_id: int | None = None,
                      date_from: str | None = None, date_to: str | None = None,
                      overwrite: bool = False,
                      progress_callback=None, stop_check=None) -> BatchResult:
        """Process all unprocessed messages. Commits every batch_size rows."""
        result = BatchResult()

        # Reset stuck processing rows first
        reset_count = self._storage.reset_stuck_processing()
        if reset_count > 0:
            logger.info("Reset %d stuck processing rows", reset_count)

        messages = self._storage.get_unprocessed_messages(
            chat_id=chat_id, date_from=date_from, date_to=date_to, overwrite=overwrite
        )
        result.total = len(messages)
        logger.info("Batch processing %d messages (overwrite=%s)", result.total, overwrite)

        commit_counter = 0
        for i, msg in enumerate(messages):
            factor = self.process_message(msg)

            if factor is None:
                continue

            status = factor.get("llm_status", "unknown")
            if status == "completed":
                result.completed += 1
            elif status == "failed":
                result.failed += 1
                result.errors.append(f"msg {msg['message_id']}: {factor.get('llm_error', 'unknown')}")
            elif status == "skipped":
                result.skipped += 1

            commit_counter += 1
            if commit_counter >= self._batch_size:
                commit_counter = 0

            if progress_callback:
                try:
                    progress_callback(i + 1, result.total, result.failed)
                except Exception:
                    pass

            if self._llm_delay > 0 and status != "skipped":
                time.sleep(self._llm_delay)

            if stop_check and stop_check():
                logger.info("Batch processing stopped by request")
                break

        logger.info("Batch complete: %d completed, %d failed, %d skipped out of %d",
                     result.completed, result.failed, result.skipped, result.total)
        return result

    def process_new_message(self, msg: dict) -> dict | None:
        """Process a new message from the listener. Wraps LLM call with timeout.
        On failure, sets llm_status='pending' so batch processor can retry later.
        Pushes SSE event on completion.
        """
        try:
            factor = self.process_message(msg)
            # Push SSE event (import here to avoid circular imports)
            if factor and factor.get("llm_status") in ("completed", "skipped"):
                from tgwatcher.web.api import push_sse_event
                push_sse_event("signal_factor", {
                    "message_id": msg["message_id"],
                    "chat_id": msg["chat_id"],
                    "sentiment_label": factor.get("sentiment_label"),
                    "event_type": factor.get("event_type"),
                    "scope": factor.get("scope"),
                    "urgency": factor.get("urgency"),
                })
            return factor
        except Exception as e:
            logger.error("process_new_message failed for msg %d: %s", msg.get("message_id"), e)
            # Set to pending so batch processor can retry
            self._storage.save_signal_factor({
                "message_id": msg["message_id"],
                "chat_id": msg["chat_id"],
                "llm_status": "pending",
            })
            return None