"""Temporary light-override bookkeeping.

The GGS light controller, while in a non-Manual mode (Schedule=1 / PPFD=12),
follows its stored schedule and ignores a bare mOnOff=0 — so a plain "turn off"
from HA does nothing useful in those modes. To give the user a *temporary* off
(turn the lamp off now, but resume the schedule on its own at the next
transition) we:

  1. switch the lamp to Manual + off  (so it actually goes dark now), and
  2. remember the original mode + the epoch at which to restore it,
  3. restore the original mode at the next schedule boundary.

This module owns step 2/3's state. It is deliberately free of any MQTT / asyncio
dependency so the boundary math can be unit-tested in isolation; the proxy wires
it to ``session.inject`` and a background restore loop.

Safety contract (this protects live plants — see the 2026-06-26 incident where a
lamp was stranded OFF in Manual overnight):
  * If a schedule boundary cannot be computed, NO override is armed — the caller
    must fall back to the mode-preserving no-op rather than strand the lamp off.
  * Every restore deadline is clamped to ``[MIN_OVERRIDE_SEC, MAX_OVERRIDE_SEC]``
    so a bad schedule can never park the lamp dark indefinitely.
  * State is persisted, so a proxy restart mid-override still restores the lamp.
"""

import json
import logging
import os
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Hard bounds on how long a temporary override may keep the lamp off. The real
# restore moment is the schedule's next transition; these only backstop a
# miscomputed boundary. 24 h covers the longest realistic veg photoperiod.
MAX_OVERRIDE_SEC = 86400
# Never schedule a restore less than a minute out — avoids a restore firing in
# the same breath as the off when the user toggles right before a transition.
MIN_OVERRIDE_SEC = 60

_DAY = 86400


def next_boundary_seconds(now_local: int, period) -> Optional[int]:
    """Seconds from ``now_local`` (seconds since local midnight) until the next
    schedule transition described by ``period`` (a controller timePeriod /
    ppfdPeriod list), or ``None`` if no meaningful boundary can be derived.

    A schedule toggles the lamp at every period's ``startTime`` / ``endTime``
    each day. The next boundary is the soonest transition strictly after
    ``now_local`` across ALL periods (split photoperiods are supported),
    wrapping to tomorrow if every boundary already passed today.
    """
    if not isinstance(period, list) or not period:
        return None
    boundaries = set()
    for p in period:
        if not isinstance(p, dict):
            continue
        start = p.get("startTime")
        end = p.get("endTime")
        if not isinstance(start, int) or not isinstance(end, int):
            continue
        start %= _DAY
        end %= _DAY
        # Degenerate window (start == end after the modulo, e.g. unconfigured
        # 0/0 or a 24 h wrap) contributes no transition — skip it.
        if start == end:
            continue
        boundaries.add(start)
        boundaries.add(end)
    if not boundaries:
        return None
    ordered = sorted(boundaries)
    for b in ordered:
        if b > now_local:
            return b - now_local
    # Every boundary already passed today → first one tomorrow.
    return (ordered[0] + _DAY) - now_local


def compute_restore_at(
    now_epoch: float,
    now_local: int,
    period,
    max_sec: int = MAX_OVERRIDE_SEC,
    min_sec: int = MIN_OVERRIDE_SEC,
) -> Optional[float]:
    """Epoch at which to restore the original mode, or ``None`` if the schedule
    yields no boundary (caller must then NOT arm an override). The delay is
    clamped to ``[min_sec, max_sec]`` as a stranding backstop."""
    secs = next_boundary_seconds(now_local, period)
    if secs is None:
        return None
    secs = max(min_sec, min(secs, max_sec))
    return now_epoch + secs


class TempOverrideManager:
    """Tracks pending light overrides keyed by ``(device_id, field)``.

    Each entry: ``{"original_mode": int, "restore_at": float, "armed_at": float}``.
    Persisted to ``persist_path`` (JSON) when given; in-memory only otherwise
    (standalone / tests). All access happens on the single asyncio loop thread,
    so no locking is needed.
    """

    def __init__(self, persist_path: Optional[str] = None):
        self._persist_path = persist_path
        self._overrides: Dict[str, dict] = {}
        self._load()

    @staticmethod
    def _key(device_id: str, field: str) -> str:
        return f"{device_id}|{field}"

    def arm(
        self, device_id: str, field: str, original_mode: int, restore_at: float,
        armed_at: Optional[float] = None,
    ) -> bool:
        """Record a pending override and persist it. Returns ``True`` only when
        the entry is durably stored (always ``True`` in in-memory mode). On a
        persist failure the in-memory entry is rolled back and ``False`` is
        returned, so the caller can decline to darken the lamp — never strand
        it off without a recoverable record."""
        key = self._key(device_id, field)
        self._overrides[key] = {
            "original_mode": int(original_mode),
            "restore_at": float(restore_at),
            "armed_at": float(armed_at) if armed_at is not None else float(restore_at),
        }
        if not self._save():
            self._overrides.pop(key, None)
            return False
        return True

    def get(self, device_id: str, field: str) -> Optional[dict]:
        entry = self._overrides.get(self._key(device_id, field))
        return dict(entry) if entry is not None else None

    def clear(self, device_id: str, field: str) -> Optional[dict]:
        entry = self._overrides.pop(self._key(device_id, field), None)
        if entry is not None:
            self._save()
        return dict(entry) if entry is not None else None

    def due(self, now_epoch: float) -> List[Tuple[str, str, int]]:
        """All overrides whose restore deadline has passed, as
        ``(device_id, field, original_mode)`` tuples."""
        out: List[Tuple[str, str, int]] = []
        for key, entry in self._overrides.items():
            if entry["restore_at"] <= now_epoch:
                device_id, _, field = key.partition("|")
                out.append((device_id, field, int(entry["original_mode"])))
        return out

    # ── persistence ──────────────────────────────────────────────────────────
    def _load(self) -> None:
        if not self._persist_path or not os.path.exists(self._persist_path):
            return
        try:
            with open(self._persist_path) as f:
                data = json.load(f)
            if isinstance(data, dict):
                # Keep only well-formed entries — a corrupt record must not
                # crash startup or, worse, leave a lamp un-restorable.
                for key, entry in data.items():
                    if (
                        isinstance(entry, dict)
                        and isinstance(entry.get("original_mode"), int)
                        and isinstance(entry.get("restore_at"), (int, float))
                    ):
                        self._overrides[key] = {
                            "original_mode": int(entry["original_mode"]),
                            "restore_at": float(entry["restore_at"]),
                            "armed_at": float(entry.get("armed_at", entry["restore_at"])),
                        }
            logger.info("Loaded %d pending light override(s)", len(self._overrides))
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Could not read overrides from %s: %s", self._persist_path, e)

    def _save(self) -> bool:
        """Atomically persist overrides. Returns ``True`` on success (or when
        running in-memory). A failure is logged at error level — the caller
        (``arm``) treats it as "not durably stored"."""
        if not self._persist_path:
            return True  # in-memory mode: no restart-survival expected
        try:
            tmp = f"{self._persist_path}.tmp"
            with open(tmp, "w") as f:
                json.dump(self._overrides, f)
            os.replace(tmp, self._persist_path)
            return True
        except OSError as e:
            logger.error(
                "Could not persist overrides to %s: %s — a lamp may not be "
                "restorable across a restart", self._persist_path, e,
            )
            return False
