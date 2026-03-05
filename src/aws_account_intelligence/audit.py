from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class AuditLogger:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.audit_dir = output_dir / "audit"
        self.audit_dir.mkdir(parents=True, exist_ok=True)

    def emit(self, event_type: str, payload: dict[str, Any]) -> None:
        record = {
            "timestamp": datetime.now(UTC).isoformat(),
            "event_type": event_type,
            **payload,
        }
        path = self.audit_dir / f"{datetime.now(UTC).date().isoformat()}.jsonl"
        with path.open("a") as handle:
            handle.write(json.dumps(record, default=str) + "\n")
