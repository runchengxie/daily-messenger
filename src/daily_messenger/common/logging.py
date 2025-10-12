from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

_RUN_ID = os.getenv("DM_RUN_ID") or uuid.uuid4().hex


class JsonFormatter(logging.Formatter):
    """Render log records as structured JSON."""

    def __init__(self, component: str, context: Dict[str, Any] | None = None) -> None:
        super().__init__()
        self.component = component
        self._context: Dict[str, Any] = dict(context or {})

    def update_context(self, **context: Any) -> None:
        for key, value in context.items():
            if value is not None:
                self._context[key] = value

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        payload: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "message": record.getMessage(),
            "component": self.component,
            "run_id": _RUN_ID,
        }
        payload.update(self._context)
        extra = getattr(record, "dm_extra", None)
        if isinstance(extra, dict):
            for key, value in extra.items():
                if value is not None:
                    payload[key] = value

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)


def setup_logger(component: str, **context: Any) -> logging.Logger:
    """Configure and return a component-specific JSON logger."""

    logger = logging.getLogger(f"daily_messenger.{component}")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = JsonFormatter(component, context)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False
    else:
        handler = logger.handlers[0]
        formatter = getattr(handler, "formatter", None)
        if isinstance(formatter, JsonFormatter):
            formatter.update_context(**context)
    return logger


def log(logger: logging.Logger, level: int, message: str, **fields: Any) -> None:
    """Emit a JSON structured log with optional extra fields."""

    logger.log(level, message, extra={"dm_extra": fields})


def get_run_id() -> str:
    return _RUN_ID
