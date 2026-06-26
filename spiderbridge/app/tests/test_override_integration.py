"""Decision-logic tests for the temporary light override wired into
MITMProxy._light_payload_with_override (no asyncio / MQTT needed — the method
is synchronous and only touches caches + the override manager)."""

import asyncio
import json

from proxy.mitm_proxy import MITMProxy
from proxy.override_manager import MAX_OVERRIDE_SEC

H = 3600


class _StubMqtt:
    def publish(self, *a, **k):
        pass


class _FakeSession:
    def __init__(self, light_state=None, device_state=None):
        self.device_id = "ggs_1"
        self.mac = "AABBCC"
        self.uid = "uid"
        self.light_state = light_state or {}
        self.device_state = device_state or {}
        self.last_nonzero_level = {}


def _proxy():
    # /data/options.json absent in test env → manager stays in-memory.
    return MITMProxy({}, _StubMqtt(), config_path="config/config.yaml")


def _schedule_block(mode=1):
    # photoperiod 06:00–22:00
    return {
        "modeType": mode,
        "mOnOff": 1,
        "mLevel": 80,
        "timePeriod": [{"startTime": 6 * H, "endTime": 22 * H, "enabled": 1, "weekmask": 127}],
    }


def _ppfd_block():
    return {
        "modeType": 12,
        "mOnOff": 1,
        "mLevel": 80,
        "ppfdPeriod": [{"startTime": 6 * H, "endTime": 22 * H, "enabled": 1, "weekmask": 127}],
    }


# ── arming ───────────────────────────────────────────────────────────────────

def test_off_in_schedule_arms_override_and_returns_manual_off():
    p = _proxy()
    s = _FakeSession(light_state={"light": _schedule_block(1)},
                     device_state={"light": _schedule_block(1)})
    payload = p._light_payload_with_override(s, "light", json.dumps({"state": "OFF"}))
    blk = payload["params"]["light"]
    assert blk["modeType"] == 0   # forced Manual so the lamp actually goes dark
    assert blk["mOnOff"] == 0
    ov = p.override_mgr.get("ggs_1", "light")
    assert ov is not None and ov["original_mode"] == 1


def test_off_in_ppfd_arms_override_original_mode_12():
    p = _proxy()
    s = _FakeSession(light_state={"light": _ppfd_block()},
                     device_state={"light": _ppfd_block()})
    payload = p._light_payload_with_override(s, "light", json.dumps({"state": "OFF"}))
    assert payload["params"]["light"]["modeType"] == 0
    assert p.override_mgr.get("ggs_1", "light")["original_mode"] == 12


def test_off_in_manual_does_not_arm():
    p = _proxy()
    s = _FakeSession(light_state={"light": _schedule_block(0)},
                     device_state={"light": _schedule_block(0)})
    payload = p._light_payload_with_override(s, "light", json.dumps({"state": "OFF"}))
    assert payload["params"]["light"]["modeType"] == 0
    assert p.override_mgr.get("ggs_1", "light") is None


def test_off_in_schedule_without_period_does_not_strand():
    # Cold cache: Schedule mode but no timePeriod → no boundary → no override,
    # mode is preserved (Schedule), so the lamp keeps following the plan.
    p = _proxy()
    block = {"modeType": 1, "mOnOff": 1, "mLevel": 80}
    s = _FakeSession(light_state={"light": block}, device_state={"light": block})
    payload = p._light_payload_with_override(s, "light", json.dumps({"state": "OFF"}))
    assert payload["params"]["light"]["modeType"] == 1  # preserved, NOT Manual
    assert p.override_mgr.get("ggs_1", "light") is None


def test_second_off_does_not_rearm():
    p = _proxy()
    s = _FakeSession(light_state={"light": _schedule_block(1)},
                     device_state={"light": _schedule_block(1)})
    p._light_payload_with_override(s, "light", json.dumps({"state": "OFF"}))
    first = p.override_mgr.get("ggs_1", "light")
    # cache now reflects Manual+off would be merged in production; simulate that
    s.light_state["light"]["modeType"] = 0
    p._light_payload_with_override(s, "light", json.dumps({"state": "OFF"}))
    second = p.override_mgr.get("ggs_1", "light")
    assert second["restore_at"] == first["restore_at"]
    assert second["original_mode"] == 1


# ── resuming / clearing ──────────────────────────────────────────────────────

def test_turn_on_while_override_resumes_original_mode():
    p = _proxy()
    s = _FakeSession(light_state={"light": _schedule_block(1)},
                     device_state={"light": _schedule_block(1)})
    p._light_payload_with_override(s, "light", json.dumps({"state": "OFF"}))
    assert p.override_mgr.get("ggs_1", "light") is not None
    payload = p._light_payload_with_override(s, "light", json.dumps({"state": "ON"}))
    assert payload["params"]["light"]["modeType"] == 1  # Schedule restored
    assert p.override_mgr.get("ggs_1", "light") is None


def test_explicit_effect_clears_override():
    p = _proxy()
    s = _FakeSession(light_state={"light": _schedule_block(1)},
                     device_state={"light": _schedule_block(1)})
    p._light_payload_with_override(s, "light", json.dumps({"state": "OFF"}))
    assert p.override_mgr.get("ggs_1", "light") is not None
    payload = p._light_payload_with_override(
        s, "light", json.dumps({"state": "ON", "effect": "Manual"}))
    assert payload["params"]["light"]["modeType"] == 0
    assert p.override_mgr.get("ggs_1", "light") is None


def test_explicit_schedule_effect_without_override_passes_through():
    p = _proxy()
    s = _FakeSession(light_state={"light": _schedule_block(0)},
                     device_state={"light": _schedule_block(0)})
    payload = p._light_payload_with_override(
        s, "light", json.dumps({"state": "ON", "effect": "Schedule"}))
    assert payload["params"]["light"]["modeType"] == 1
    assert p.override_mgr.get("ggs_1", "light") is None


# ── restore path (_restore_one) — the stranding-safety critical path ─────────

class _RestoreSession:
    def __init__(self, inject_ok=True):
        self.device_id = "ggs_1"
        self.mac = "AABBCC"
        self.uid = "uid"
        self.device_state = {"light": _schedule_block(1)}
        self.light_state = {"light": _schedule_block(1)}
        self.last_nonzero_level = {}
        self._inject_ok = inject_ok
        self.injected = []

    async def inject(self, payload):
        self.injected.append(payload)
        return self._inject_ok


def test_restore_clears_override_on_successful_inject():
    p = _proxy()
    sess = _RestoreSession(inject_ok=True)
    p._sessions["ggs_1"] = sess
    p.override_mgr.arm("ggs_1", "light", 1, 100.0, armed_at=100.0)
    asyncio.run(p._restore_one("ggs_1", "light", 1, now=1000.0))
    assert sess.injected and sess.injected[0]["params"]["light"]["modeType"] == 1
    assert p.override_mgr.get("ggs_1", "light") is None


def test_restore_keeps_override_on_failed_inject():
    # silent inject failure must NOT clear the override → lamp not forgotten dark
    p = _proxy()
    sess = _RestoreSession(inject_ok=False)
    p._sessions["ggs_1"] = sess
    p.override_mgr.arm("ggs_1", "light", 1, 100.0, armed_at=100.0)
    asyncio.run(p._restore_one("ggs_1", "light", 1, now=1000.0))
    assert p.override_mgr.get("ggs_1", "light") is not None


def test_restore_keeps_override_when_device_offline():
    p = _proxy()  # no session registered
    p.override_mgr.arm("ggs_1", "light", 1, 100.0, armed_at=100.0)
    asyncio.run(p._restore_one("ggs_1", "light", 1, now=1000.0))
    assert p.override_mgr.get("ggs_1", "light") is not None


def test_restore_evicts_stale_override_past_ttl():
    p = _proxy()  # no session, very old override
    p.override_mgr.arm("ggs_1", "light", 1, 100.0, armed_at=100.0)
    asyncio.run(p._restore_one("ggs_1", "light", 1, now=100.0 + 2 * MAX_OVERRIDE_SEC + 1))
    assert p.override_mgr.get("ggs_1", "light") is None
