import json

from proxy.override_manager import (
    next_boundary_seconds,
    compute_restore_at,
    TempOverrideManager,
    MAX_OVERRIDE_SEC,
    MIN_OVERRIDE_SEC,
)

H = 3600


def _period(start_h, end_h):
    return [{"startTime": start_h * H, "endTime": end_h * H, "enabled": 1, "weekmask": 127}]


# ── next_boundary_seconds ────────────────────────────────────────────────────

def test_boundary_before_start_returns_start():
    # photoperiod 06:00–22:00, now 05:00 → next transition is 06:00 (1 h)
    assert next_boundary_seconds(5 * H, _period(6, 22)) == 1 * H


def test_boundary_during_on_phase_returns_end():
    # now 14:00 inside 06:00–22:00 → next transition is 22:00 (8 h)
    assert next_boundary_seconds(14 * H, _period(6, 22)) == 8 * H


def test_boundary_after_end_wraps_to_next_day_start():
    # now 23:00, window 06:00–22:00 → next is tomorrow 06:00 (7 h)
    assert next_boundary_seconds(23 * H, _period(6, 22)) == 7 * H


def test_boundary_overnight_window_before_end():
    # veg-style overnight 18:00–12:00, now 03:00 → next transition is 12:00 (9 h)
    assert next_boundary_seconds(3 * H, _period(18, 12)) == 9 * H


def test_boundary_overnight_window_during_off_gap():
    # overnight 18:00–12:00, now 14:00 (lamp off) → next is 18:00 (4 h)
    assert next_boundary_seconds(14 * H, _period(18, 12)) == 4 * H


def test_boundary_exactly_on_start_picks_next():
    # at exactly 06:00 the 06:00 boundary is not "strictly after" now → 22:00
    assert next_boundary_seconds(6 * H, _period(6, 22)) == 16 * H


def test_boundary_degenerate_window_returns_none():
    assert next_boundary_seconds(10 * H, _period(8, 8)) is None


def test_boundary_empty_period_returns_none():
    assert next_boundary_seconds(10 * H, []) is None
    assert next_boundary_seconds(10 * H, None) is None


def test_boundary_non_int_times_returns_none():
    assert next_boundary_seconds(10 * H, [{"startTime": "06:00", "endTime": "22:00"}]) is None


def test_boundary_missing_keys_returns_none():
    assert next_boundary_seconds(10 * H, [{"foo": 1}]) is None


def test_boundary_multi_period_picks_nearest_across_all():
    # split photoperiod: on 06–12 and 18–22; now 13:00 → next is 18:00 (5 h)
    split = _period(6, 12) + _period(18, 22)
    assert next_boundary_seconds(13 * H, split) == 5 * H
    # now 23:00 → wraps to earliest boundary tomorrow (06:00 = 7 h)
    assert next_boundary_seconds(23 * H, split) == 7 * H


def test_boundary_endtime_over_a_day_is_degenerate():
    # endTime 25:00 → 01:00 after modulo; startTime 01:00 → equal → no boundary
    assert next_boundary_seconds(10 * H, [{"startTime": 1 * H, "endTime": 25 * H}]) is None


def test_boundary_skips_degenerate_period_uses_valid_one():
    mixed = [{"startTime": 8 * H, "endTime": 8 * H}] + _period(6, 22)
    assert next_boundary_seconds(14 * H, mixed) == 8 * H  # 22:00 boundary


# ── compute_restore_at ───────────────────────────────────────────────────────

def test_restore_at_adds_boundary_to_epoch():
    now = 1_000_000.0
    # 14:00 in a 06–22 window → 8 h out
    assert compute_restore_at(now, 14 * H, _period(6, 22)) == now + 8 * H


def test_restore_at_clamps_to_max():
    now = 1_000_000.0
    # overnight 18–12, now 18:00:01 → ~18 h until next 12:00 transition.
    # With a small explicit max_sec the delay must clamp down to it. (Two
    # daily boundaries mean the real next transition is always < 24 h, so the
    # default MAX is only reachable via a tightened cap or malformed data.)
    res = compute_restore_at(now, 18 * H + 1, _period(18, 12), max_sec=3600)
    assert res == now + 3600


def test_restore_at_clamps_to_min():
    now = 1_000_000.0
    # 30 s before the 22:00 boundary → would be 30 s, clamp up to MIN
    res = compute_restore_at(now, 22 * H - 30, _period(6, 22))
    assert res == now + MIN_OVERRIDE_SEC


def test_restore_at_none_when_no_boundary():
    assert compute_restore_at(1_000_000.0, 10 * H, _period(8, 8)) is None


# ── TempOverrideManager (in-memory) ──────────────────────────────────────────

def test_arm_get_roundtrip():
    m = TempOverrideManager()
    assert m.arm("ggs_1", "light", 1, 1_000_500.0, armed_at=1_000_000.0) is True
    e = m.get("ggs_1", "light")
    assert e["original_mode"] == 1
    assert e["restore_at"] == 1_000_500.0
    assert e["armed_at"] == 1_000_000.0


def test_arm_returns_false_and_rolls_back_when_persist_fails():
    # persist path under a non-existent directory → write raises OSError
    m = TempOverrideManager("/nonexistent_dir_xyz_42/overrides.json")
    assert m.arm("ggs_1", "light", 1, 1_000_500.0) is False
    # rolled back: no phantom in-memory entry that disk doesn't have
    assert m.get("ggs_1", "light") is None


def test_get_returns_copy_not_reference():
    m = TempOverrideManager()
    m.arm("ggs_1", "light", 1, 1_000_500.0)
    e = m.get("ggs_1", "light")
    e["original_mode"] = 99
    assert m.get("ggs_1", "light")["original_mode"] == 1


def test_get_missing_returns_none():
    assert TempOverrideManager().get("ggs_1", "light") is None


def test_clear_removes_and_returns():
    m = TempOverrideManager()
    m.arm("ggs_1", "light", 12, 1_000_500.0)
    removed = m.clear("ggs_1", "light")
    assert removed["original_mode"] == 12
    assert m.get("ggs_1", "light") is None
    assert m.clear("ggs_1", "light") is None


def test_due_returns_only_past_deadline():
    m = TempOverrideManager()
    m.arm("ggs_1", "light", 1, 1_000_000.0)   # due at 1e6
    m.arm("ggs_1", "light2", 12, 2_000_000.0)  # due at 2e6
    due = m.due(1_500_000.0)
    assert ("ggs_1", "light", 1) in due
    assert ("ggs_1", "light2", 12) not in due


def test_due_field_with_separator_char_parses():
    # field never contains '|', but guard the partition logic anyway
    m = TempOverrideManager()
    m.arm("ggs_1", "light", 1, 10.0)
    assert m.due(20.0) == [("ggs_1", "light", 1)]


# ── persistence ──────────────────────────────────────────────────────────────

def test_persist_survives_new_instance(tmp_path=None):
    import tempfile
    import os
    d = tempfile.mkdtemp()
    path = os.path.join(d, "overrides.json")
    m1 = TempOverrideManager(path)
    m1.arm("ggs_1", "light", 1, 1_000_500.0, armed_at=1_000_000.0)
    # fresh instance reads from disk
    m2 = TempOverrideManager(path)
    e = m2.get("ggs_1", "light")
    assert e is not None and e["original_mode"] == 1 and e["restore_at"] == 1_000_500.0
    # clear persists too
    m2.clear("ggs_1", "light")
    m3 = TempOverrideManager(path)
    assert m3.get("ggs_1", "light") is None


def test_persist_skips_corrupt_entries():
    import tempfile
    import os
    d = tempfile.mkdtemp()
    path = os.path.join(d, "overrides.json")
    with open(path, "w") as f:
        json.dump({
            "ggs_1|light": {"original_mode": 1, "restore_at": 5.0, "armed_at": 1.0},
            "bad|entry": {"original_mode": "x", "restore_at": 5.0},
            "also|bad": {"restore_at": 5.0},
        }, f)
    m = TempOverrideManager(path)
    assert m.get("ggs_1", "light") is not None
    assert m.get("bad", "entry") is None
    assert m.get("also", "bad") is None


def test_corrupt_json_does_not_crash():
    import tempfile
    import os
    d = tempfile.mkdtemp()
    path = os.path.join(d, "overrides.json")
    with open(path, "w") as f:
        f.write("{not valid json")
    m = TempOverrideManager(path)  # must not raise
    assert m.get("ggs_1", "light") is None
