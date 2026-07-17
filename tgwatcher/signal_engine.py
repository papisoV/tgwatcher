"""Signal engine - orchestrates keyword filtering and LLM refinement for news factor extraction."""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field

from tgwatcher.signal_filter import KeywordFilter
from tgwatcher.signal_llm import SignalLLMClient, LLMRefineError
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
        self._llm_batch_size = config.get("llm_batch_size", 15)
        self._factor_version = config.get("factor_version", 2)

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
                "direction": llm_result["direction"],
                "magnitude": llm_result["magnitude"],
                "urgency": llm_result["urgency"],
                "confidence": llm_result["confidence"],
                "halflife_min": llm_result["halflife_min"],
                "symbols": json.dumps(llm_result["symbols"], ensure_ascii=False),
                "event_type": llm_result["event_type"],
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

    def _process_batch_llm(
        self,
        pending: list[tuple[dict, dict]],
        progress_callback=None,
        base_index: int = 0,
        total: int = 0,
    ) -> tuple[int, int, list[str]]:
        """
        Process filter-passed messages using batch LLM calls.

        Args:
            pending: List of (msg, filter_result) tuples.
            progress_callback: Optional callback(processed, total, errors).
            base_index: Offset for progress reporting.
            total: Total message count for progress reporting.

        Returns:
            (completed_count, failed_count, errors_list)
        """
        completed = 0
        failed = 0
        errors: list[str] = []

        # Chunk the pending messages into groups of llm_batch_size
        chunk_size = self._llm_batch_size
        for chunk_start in range(0, len(pending), chunk_size):
            chunk = pending[chunk_start:chunk_start + chunk_size]

            # Mark all as processing
            for msg, filter_result in chunk:
                self._storage.save_signal_factor({
                    "message_id": msg["message_id"],
                    "chat_id": msg["chat_id"],
                    "llm_status": "processing",
                    "filter_result": "passed",
                    "matched_keywords": json.dumps(filter_result.matched_keywords, ensure_ascii=False),
                    "keyword_preliminary": json.dumps(filter_result.preliminary_factors, ensure_ascii=False),
                    "factor_version": self._factor_version,
                })

            # Build batch input
            batch_input = [(msg["text"], filt.preliminary_factors) for msg, filt in chunk]

            # Call batch LLM
            batch_results = self._llm.refine_batch(batch_input)

            # Save each result
            for i, (msg, filter_result) in enumerate(chunk):
                llm_result = batch_results[i] if i < len(batch_results) else None
                factor_data = {
                    "message_id": msg["message_id"],
                    "chat_id": msg["chat_id"],
                    "filter_result": "passed",
                    "matched_keywords": json.dumps(filter_result.matched_keywords, ensure_ascii=False),
                    "keyword_preliminary": json.dumps(filter_result.preliminary_factors, ensure_ascii=False),
                    "factor_version": self._factor_version,
                }

                if llm_result is not None:
                    factor_data.update({
                        "direction": llm_result["direction"],
                        "magnitude": llm_result["magnitude"],
                        "urgency": llm_result["urgency"],
                        "confidence": llm_result["confidence"],
                        "halflife_min": llm_result["halflife_min"],
                        "symbols": json.dumps(llm_result["symbols"], ensure_ascii=False),
                        "event_type": llm_result["event_type"],
                        "reasoning": llm_result.get("reasoning", ""),
                        "llm_status": "completed",
                        "llm_model": self._config.get("llm", {}).get("model", ""),
                        "llm_raw": json.dumps(llm_result, ensure_ascii=False),
                    })
                    completed += 1
                else:
                    factor_data.update({
                        "llm_status": "failed",
                        "llm_error": "Batch item validation failed",
                    })
                    failed += 1
                    errors.append(f"msg {msg['message_id']}: validation failed in batch")

                self._storage.save_signal_factor(factor_data)

            # Progress callback after each chunk
            if progress_callback:
                try:
                    processed = base_index + chunk_start + len(chunk)
                    progress_callback(processed, total, failed)
                except Exception:
                    pass

            # Delay between batch API calls
            if self._llm_delay > 0 and chunk_start + chunk_size < len(pending):
                time.sleep(self._llm_delay)

        return completed, failed, errors

    def process_batch(self, chat_id: int | None = None,
                      date_from: str | None = None, date_to: str | None = None,
                      overwrite: bool = False,
                      progress_callback=None, stop_check=None) -> BatchResult:
        """
        Process all unprocessed messages in two phases:
        1. Filter all messages, save rejected/skipped immediately.
        2. Batch LLM calls for filter-passed messages.
        """
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

        min_len = self._config.get("filter", {}).get("min_text_length", 10)

        # Phase 1: Filter all messages
        pending: list[tuple[dict, dict]] = []
        for i, msg in enumerate(messages):
            text = msg.get("text")
            if not text or len(text) < min_len:
                continue

            message_id = msg["message_id"]
            chat_id_msg = msg["chat_id"]
            filter_result = self._filter.filter(text)

            if not filter_result.passed:
                # Save rejected immediately
                self._storage.save_signal_factor({
                    "message_id": message_id,
                    "chat_id": chat_id_msg,
                    "llm_status": "skipped",
                    "filter_result": "rejected",
                    "matched_keywords": json.dumps(filter_result.matched_keywords, ensure_ascii=False),
                    "keyword_preliminary": json.dumps(filter_result.preliminary_factors, ensure_ascii=False),
                    "factor_version": self._factor_version,
                })
                result.skipped += 1
            else:
                pending.append((msg, filter_result))

            if progress_callback:
                try:
                    progress_callback(i + 1, result.total, 0)
                except Exception:
                    pass

            if stop_check and stop_check():
                logger.info("Batch processing stopped during filter phase")
                return result

        # Phase 2: Batch LLM calls
        logger.info(
            "Filter phase done: %d passed, %d skipped out of %d",
            len(pending), result.skipped, result.total,
        )

        if pending:
            completed, failed, errors = self._process_batch_llm(
                pending,
                progress_callback=progress_callback,
                base_index=result.total,  # offset after filter phase
                total=result.total,
            )
            result.completed = completed
            result.failed = failed
            result.errors = errors

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
                    "direction": factor.get("direction"),
                    "event_type": factor.get("event_type"),
                    "magnitude": factor.get("magnitude"),
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
