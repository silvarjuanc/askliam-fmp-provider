"""AskLiam V6.5.1 Stage 0 — authenticated, non-destructive Google Sheets UPSERT.

Install: pip install gspread google-auth pandas
Inject existing provider callables returning {tab_name: pandas.DataFrame}; never modify
their internal logic. Only three approved worksheets are ever written.
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

VERSION = "V6.5.1"
TABS = {
    "Raw_OHLCV": ("canonical_symbol", "trading_date", "source"),
    "Market_Data_Snapshot": ("ranking_run_id", "canonical_symbol"),
    "FMP_Runtime_State": ("Setting",),
}
_SECRET_PATTERN = re.compile(r"(?i)(?:apikey|api_key|token|secret|authorization)\s*[=:]")
_RUN_GUARD = threading.Lock()


class Stage0Error(RuntimeError):
    def __init__(self, status: str, detail: str):
        super().__init__(f"{status}: {detail}")
        self.status = status
        self.detail = detail


@dataclass(frozen=True)
class RowChange:
    key: tuple[str, ...]
    position: int | None
    cells: dict[str, str]


class Stage0_Daily_Ingestor:
    """Invoked only on explicit instruction; no cron/scheduler is installed.

    Providers: named zero-argument callables returning a dict[str, DataFrame].
    Existing adapters perform their own cache/routing/quota policies.

    Required env:
        GOOGLE_SERVICE_ACCOUNT_JSON (JSON object or base64 JSON; no file path)
        ASKLIAM_SPREADSHEET_ID

    Run result is COMMITTED only after fresh authenticated read-back of all 3
    worksheets. A partial Google Sheets write cannot be atomically rolled back:
    in such cases return PARTIAL_COMMIT_UNVERIFIED and HALT.
    """

    def __init__(
        self,
        providers: Mapping[str, Callable[[], Mapping[str, pd.DataFrame]]],
        spreadsheet_id: str | None = None,
        *,
        credentials_env: str = "GOOGLE_SERVICE_ACCOUNT_JSON",
        max_batch_cells: int = 150,
        max_append_rows: int = 100,
    ):
        self.providers = dict(providers)
        self.spreadsheet_id = spreadsheet_id or os.getenv("ASKLIAM_SPREADSHEET_ID")
        self.credentials_env = credentials_env
        self.max_batch_cells = max_batch_cells
        self.max_append_rows = max_append_rows
        if not self.spreadsheet_id:
            raise Stage0Error("CONFIGURATION_FAILED", "ASKLIAM_SPREADSHEET_ID is missing")
        if not self.providers or not all(callable(p) for p in self.providers.values()):
            raise Stage0Error("CONFIGURATION_FAILED", "Providers must be nonempty callable mapping")
        self.run_id = datetime.now(timezone.utc).strftime("STAGE0-%Y%m%dT%H%M%SZ-") + VERSION

    def _credentials(self) -> Credentials:
        raw = os.getenv(self.credentials_env)
        if not raw:
            raise Stage0Error("AUTHENTICATION_FAILED", "MISSING_CREDENTIAL_ENV")
        try:
            info = json.loads(raw)
        except json.JSONDecodeError:
            try:
                info = json.loads(base64.b64decode(raw.strip(), validate=True).decode("utf-8"))
            except Exception:
                raise Stage0Error("AUTHENTICATION_FAILED", "INVALID_JSON_OR_BASE64") from None

        # Render environment editors sometimes serialize the JSON object as
        # a JSON string. Unwrap once, never evaluate arbitrary input.
        if isinstance(info, str):
            try:
                info = json.loads(info)
            except json.JSONDecodeError:
                raise Stage0Error("AUTHENTICATION_FAILED", "WRAPPED_JSON_INVALID") from None

        if not isinstance(info, dict) or info.get("type") != "service_account":
            raise Stage0Error("AUTHENTICATION_FAILED", "NOT_SERVICE_ACCOUNT_JSON")
        if not all(info.get(k) for k in ("private_key", "client_email", "token_uri")):
            raise Stage0Error("AUTHENTICATION_FAILED", "MISSING_SERVICE_ACCOUNT_FIELDS")

        # Accept escaped newlines if double-escaped by Render value entry.
        key = info["private_key"]
        if isinstance(key, str) and "\\n" in key and "\n" not in key:
            info = dict(info)
            info["private_key"] = key.replace("\\n", "\n")

        try:
            return Credentials.from_service_account_info(
                info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
            )
        except (ValueError, TypeError):
            raise Stage0Error("AUTHENTICATION_FAILED", "PRIVATE_KEY_OR_SERVICE_ACCOUNT_FORMAT_INVALID") from None
        except Exception:
            raise Stage0Error("AUTHENTICATION_FAILED", "SERVICE_ACCOUNT_INITIALIZATION_FAILED") from None

    def _open(self):
        # Called separately for initial capture, before-commit guard and read-back.
        try:
            return gspread.authorize(self._credentials()).open_by_key(self.spreadsheet_id)
        except Stage0Error:
            raise
        except Exception as exc:
            raise Stage0Error("AUTHENTICATION_FAILED", type(exc).__name__) from None

    @staticmethod
    def _scalar(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (dict, list, tuple)):
            return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if isinstance(value, (datetime, pd.Timestamp)):
            return value.isoformat()
        try:
            if pd.isna(value):
                return ""
        except (TypeError, ValueError):
            pass
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            if not math.isfinite(float(value)):
                return ""
            return format(value, ".15g")
        return str(value).strip()

    @staticmethod
    def _key(tab: str, row: Mapping[str, str]) -> tuple[str, ...]:
        pieces = []
        for name in TABS[tab]:
            v = str(row.get(name, "")).strip()
            if name in ("canonical_symbol", "source"):
                v = v.upper()
            if not v or (name == "trading_date" and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", v)):
                raise Stage0Error("VALIDATION_FAILED", f"{tab}: invalid key column {name}")
            pieces.append(v)
        return tuple(pieces)

    def _capture(self, book) -> dict:
        result = {}
        for tab, keys in TABS.items():
            try:
                ws = book.worksheet(tab)
                matrix = ws.get_all_values()
            except Exception as exc:
                raise Stage0Error("SHEET_READ_FAILED", f"{tab}: {type(exc).__name__}") from None
            if not matrix or not matrix[0]:
                raise Stage0Error("SCHEMA_FAILED", f"{tab}: missing headers")
            headers = [str(h).strip() for h in matrix[0]]
            if len(set(headers)) != len(headers) or any(not h for h in headers):
                raise Stage0Error("SCHEMA_FAILED", f"{tab}: duplicated/empty header")
            if any(k not in headers for k in keys):
                raise Stage0Error("SCHEMA_FAILED", f"{tab}: required key columns missing")
            rows, locations = {}, {}
            for index, raw in enumerate(matrix[1:], start=2):
                values = (raw + [""] * len(headers))[:len(headers)]
                if not any(str(x).strip() for x in values):
                    continue
                row = dict(zip(headers, values))
                key = self._key(tab, row)
                if key in rows:
                    raise Stage0Error("DUPLICATE_KEY", f"{tab}: pre-existing duplicate {key}")
                rows[key], locations[key] = row, index
            result[tab] = {
                "headers": headers,
                "rows": rows,
                "locations": locations,
                "worksheet": ws,
            }
        return result

    @staticmethod
    def _fingerprint(state: dict) -> str:
        stable = {
            tab: {
                "headers": state[tab]["headers"],
                "rows": {
                    "|".join(key): row
                    for key, row in sorted(state[tab]["rows"].items())
                },
            } for tab in TABS
        }
        return hashlib.sha256(json.dumps(stable, sort_keys=True).encode()).hexdigest()

    def _collect(self) -> dict[str, pd.DataFrame]:
        grouped: dict[str, list[pd.DataFrame]] = {t: [] for t in TABS}
        for provider_name, provider in self.providers.items():
            try:
                data = provider()
            except Exception as exc:
                raise Stage0Error("PROVIDER_DEFERRED", f"{provider_name}: {type(exc).__name__}") from None
            if not isinstance(data, Mapping):
                raise Stage0Error("VALIDATION_FAILED", f"{provider_name}: not a mapping")
            if set(data) - set(TABS):
                raise Stage0Error("UNAUTHORIZED_TAB", f"{provider_name}: output not in approved tabs")
            for tab, frame in data.items():
                if not isinstance(frame, pd.DataFrame):
                    raise Stage0Error("VALIDATION_FAILED", f"{provider_name}/{tab}: not DataFrame")
                if not frame.empty:
                    grouped[tab].append(frame.copy())
        return {
            t: pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            for t, frames in grouped.items()
        }

    def _plan(self, before: dict, frames: dict) -> tuple[dict, dict]:
        plans, expected = {}, {}
        for tab in TABS:
            headers = before[tab]["headers"]
            known = before[tab]["rows"]
            target = {key: row.copy() for key, row in known.items()}
            updates, inserts, seen = [], [], set()
            frame = frames[tab]
            if not frame.empty:
                cols = list(frame.columns)
                if len(cols) != len(set(cols)) or (set(cols) - set(headers)):
                    raise Stage0Error("SCHEMA_FAILED", f"{tab}: unexpected/duplicate columns")
                if not set(TABS[tab]).issubset(cols):
                    raise Stage0Error("SCHEMA_FAILED", f"{tab}: key column absent")
                for raw in frame.to_dict(orient="records"):
                    payload = {k: self._scalar(v) for k, v in raw.items()}
                    if any(_SECRET_PATTERN.search(str(v)) for k, v in payload.items()
                           if ("url" in k.lower() or "source" in k.lower()) and v):
                        raise Stage0Error("SECRET_IN_PAYLOAD", f"{tab}: URL/source contains a secret-shaped parameter")
                    key = self._key(tab, payload)
                    if key in seen:
                        raise Stage0Error("DUPLICATE_KEY", f"{tab}: incoming duplicate {key}")
                    seen.add(key)
                    if key not in target:
                        row = {h: payload.get(h, "") for h in headers}
                        target[key] = row
                        inserts.append(RowChange(key, None, row))
                        continue
                    previous = target[key]
                    delta = {}
                    for field, value in payload.items():
                        # Missing optional = unknown, never clear known values.
                        if value == "":
                            continue
                        if previous.get(field, "") != value:
                            delta[field] = value
                    if delta:
                        target[key] = {**previous, **delta}
                        updates.append(RowChange(key, before[tab]["locations"][key], delta))
            if not set(known).issubset(target):
                raise Stage0Error("VALIDATION_FAILED", f"{tab}: historical key loss")
            expected[tab] = target
            plans[tab] = {"updates": updates, "inserts": inserts}
        return plans, expected

    @staticmethod
    def _chunks(items: list, n: int):
        for i in range(0, len(items), n):
            yield items[i:i+n]

    def _commit(self, before: dict, plans: dict) -> None:
        # No .clear(), no replace, no table deletion.
        # Read-back is mandatory before declaring any success.
        for tab in TABS:
            ws = before[tab]["worksheet"]
            headers = before[tab]["headers"]
            updates = []
            for item in plans[tab]["updates"]:
                for col, val in item.cells.items():
                    column = headers.index(col) + 1
                    from gspread.utils import rowcol_to_a1
                    updates.append({
                        "range": rowcol_to_a1(item.position, column),
                        "values": [[val]],
                    })
            for chunk in self._chunks(updates, self.max_batch_cells):
                ws.batch_update(chunk, value_input_option="RAW")
            additions = [
                [item.cells.get(column, "") for column in headers]
                for item in plans[tab]["inserts"]
            ]
            for chunk in self._chunks(additions, self.max_append_rows):
                ws.append_rows(chunk, value_input_option="RAW",
                               insert_data_option="INSERT_ROWS")

    def _verify(self, expected: dict) -> dict:
        # New authenticated read, not local re-use or in-memory mock.
        actual = self._capture(self._open())
        details = {}
        for tab in TABS:
            a, e = actual[tab], expected[tab]
            if len(a["rows"]) != len(e):
                raise Stage0Error("VERIFICATION_FAILED", f"{tab}: row count mismatch")
            if set(a["rows"]) != set(e):
                raise Stage0Error("VERIFICATION_FAILED", f"{tab}: key set mismatch")
            for key, row in e.items():
                for column in a["headers"]:
                    if a["rows"][key].get(column, "") != row.get(column, ""):
                        raise Stage0Error(
                            "VERIFICATION_FAILED",
                            f"{tab}: read-back mismatch {key}/{column}",
                        )
            details[tab] = {"verified_rows": len(e), "status": "PASS"}
        return details

    def run(self) -> dict:
        # In-process exclusion. Deploy as ONE worker instance; cross-instance
        # locking must be implemented externally before horizontal scaling.
        if not _RUN_GUARD.acquire(blocking=False):
            raise Stage0Error("CONCURRENT_RUN_BLOCKED", "Stage 0 already executing")
        committed = False
        try:
            before = self._capture(self._open())
            before_hash = self._fingerprint(before)
            frames = self._collect()
            plans, expected = self._plan(before, frames)
            # Reject silent no-op success when no provider supplied any rows.
            if not any(not f.empty for f in frames.values()):
                raise Stage0Error("PROVIDER_DEFERRED", "All provider payloads are empty")
            fresh = self._capture(self._open())
            if self._fingerprint(fresh) != before_hash:
                raise Stage0Error("CONCURRENT_MODIFICATION", "Workbook changed after STATE_BEFORE")
            committed = True  # a write could succeed and the response fail
            try:
                self._commit(fresh, plans)
            except Exception as exc:
                raise Stage0Error(
                    "PARTIAL_COMMIT_UNVERIFIED",
                    f"Write interrupted: {type(exc).__name__}; halt and reconcile",
                ) from None
            verified = self._verify(expected)
            return {
                "version": VERSION,
                "run_id": self.run_id,
                "commit_status": "COMMITTED",
                "verify_status": "PASS",
                "final_status": "COMMITTED",
                "tabs": {
                    tab: {
                        **verified[tab],
                        "inserted": len(plans[tab]["inserts"]),
                        "updated": len(plans[tab]["updates"]),
                    }
                    for tab in TABS
                },
                "cursor_advance_permitted": True,
                "ranking_permitted": True,
            }
        except Stage0Error:
            raise
        except Exception as exc:
            raise Stage0Error(
                "VERIFICATION_FAILED" if committed else "STAGE0_ABORTED",
                f"{type(exc).__name__}; pipeline halted",
            ) from None
        finally:
            _RUN_GUARD.release()
