from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from .logging import get_run_id


def _meta_path(out_dir: Path) -> Path:
    return out_dir / "run_meta.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_meta(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {
            "run_id": get_run_id(),
            "started_at": _utc_now(),
            "steps": {},
        }
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {
            "run_id": get_run_id(),
            "started_at": _utc_now(),
            "steps": {},
            "warning": "previous meta corrupt",
        }


def record_step(out_dir: Path, step: str, status: str, **fields: Any) -> None:
    path = _meta_path(out_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    meta = _load_meta(path)
    steps = meta.setdefault("steps", {})
    step_meta = steps.get(step, {})
    step_meta.update({k: v for k, v in fields.items() if v is not None})
    step_meta["status"] = status
    step_meta["updated_at"] = _utc_now()
    steps[step] = step_meta
    if status.lower() in {"completed", "failed"}:
        meta["completed_at"] = _utc_now()
    path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
