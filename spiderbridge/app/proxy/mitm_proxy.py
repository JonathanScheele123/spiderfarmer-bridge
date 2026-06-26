import asyncio
import json
import logging
import ssl
import time
from pathlib import Path
from typing import Dict, Optional

import paho.mqtt.client as mqtt
import yaml

from .mqtt_parser import (
    parse_packets, build_publish,
    MQTT_PUBLISH, MQTT_CONNECT, MQTT_SUBSCRIBE,
)
from .normalizer import normalize_status, fan_extras_topics, light_extras_topics
from ha.discovery import (
    publish_soil_sensor_discovery as _publish_soil_sensor_discovery,
    unpublish_outlet_discovery as _unpublish_outlet_discovery,
)
from .command_handler import translate_command, LIGHT_MODE_TO_EFFECT, _onoff
from .config import HA_OPTIONS_PATH, HA_DEVICES_PATH, HA_OVERRIDES_PATH
from .override_manager import TempOverrideManager, compute_restore_at, MAX_OVERRIDE_SEC

logger = logging.getLogger(__name__)

_MAC_PLACEHOLDER = "AABBCCDDEEFF"

# Diagnostic: log the first time we see a non-CB topic prefix so we can find out
# what standalone PS5/PS10/LC controllers actually publish to (they may not use
# CB). Once we know we can extend support deliberately instead of guessing.
_seen_topic_prefixes: set = set()


class ProxySession:
    """Represents one active GGS Controller connection."""

    def __init__(self, device_id: str, mac: str, uid: str,
                 mqtt_client: mqtt.Client):
        self.device_id = device_id
        self.mac = mac
        self.uid = uid
        self.mqtt_client = mqtt_client
        self._upstream_writer: Optional[asyncio.StreamWriter] = None
        self._client_writer: Optional[asyncio.StreamWriter] = None
        self.device_state: Dict[str, dict] = {}  # module → current state from getDevSta
        self.last_nonzero_level: Dict[str, int] = {}  # module → last brightness > 0
        # SF protocol topic-prefix learned from observed cloud→device traffic.
        # Defaults to "CB" (Control Box) but PS5/PS10/LC may use a different
        # value; using the wrong prefix means our injects are silently
        # ignored by the controller that subscribes elsewhere.
        self.down_topic_prefix: str = "CB"
        # Static discovery publishes outlets 1..10 unconditionally; once the
        # controller reports its actual outlet set we unpublish the rest.
        self._outlet_discovery_pruned: bool = False
        # Full last-known fan/blower blocks for app-parity write paths.
        # getDevSta carries only minimal state ({on, level}); cloud
        # setConfigField traffic carries the schedule/cycle/speeds we
        # need to merge against on HA writes.
        self.fan_state: Dict[str, dict] = {}
        # Same idea for light: getDevSta on some firmwares omits
        # modeType, so we cache it from observed setConfigField traffic
        # and from our own injects. Used by the normalizer as a fallback
        # so the HA effect dropdown stays consistent with what the
        # controller is actually doing.
        self.light_state: Dict[str, dict] = {}
        # Tracks the detached _initial_poll task so handle_client can
        # cancel it on cleanup — otherwise a session that disconnects
        # within the 3 s startup delay leaves the task running and it
        # tries to inject against a closed writer.
        self.initial_poll_task: Optional[asyncio.Task] = None

    def set_upstream(self, writer: asyncio.StreamWriter) -> None:
        self._upstream_writer = writer

    def set_client(self, writer: asyncio.StreamWriter) -> None:
        self._client_writer = writer

    async def inject(self, payload: dict) -> bool:
        """Inject command directly into the device TLS connection. Returns
        ``True`` only when the bytes were actually written + drained — callers
        that gate irreversible state (e.g. clearing a light override) must NOT
        treat a silent no-connection / write error as a delivered command."""
        if self._client_writer is None:
            logger.warning("[%s] inject: no device connection", self.device_id)
            return False
        topic = f"SF/GGS/{self.down_topic_prefix}/API/DOWN/{self.mac.upper().replace(':', '')}"
        raw = build_publish(
            topic=topic,
            message=json.dumps(payload, separators=(',', ':')).encode(),
        )
        try:
            self._client_writer.write(raw)
            await self._client_writer.drain()
            logger.info("[%s] Command injected: %s", self.device_id, payload.get("params", {}))
            return True
        except Exception as e:
            logger.error("[%s] inject error: %s", self.device_id, e)
            return False

    def publish_availability(self, status: str) -> None:
        self.mqtt_client.publish(
            f"spiderfarmer/{self.device_id}/availability",
            status,
            retain=True,
        )


class MITMProxy:
    def __init__(self, config: dict, mqtt_client: mqtt.Client, config_path: str = "config/config.yaml"):
        self.config = config
        self.mqtt_client = mqtt_client
        self._config_path = config_path
        self._sessions: Dict[str, ProxySession] = {}
        self._known_soil_ids: Dict[str, set] = {}  # device_id → set of seen sensor IDs
        # Temporary light overrides: a turn-off in Schedule/PPFD mode flips the
        # lamp to Manual+off now and restores the original mode at the next
        # schedule boundary. Persist only under HA OS (where /data exists);
        # standalone keeps it in-memory.
        persist = HA_OVERRIDES_PATH if Path(HA_OPTIONS_PATH).exists() else None
        self.override_mgr = TempOverrideManager(persist)

    def build_server_ssl_ctx(self) -> ssl.SSLContext:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        # Pin minimum TLS version. Older OpenSSL builds default to TLS 1.0
        # which would let a misbehaving client downgrade the channel.
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.load_cert_chain(
            certfile=self.config["proxy"]["cert_file"],
            keyfile=self.config["proxy"]["key_file"],
        )
        return ctx

    def _build_upstream_ssl_ctx(self) -> ssl.SSLContext:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED
        # SF MQTT server uses a private CA — not in system trust store
        ctx.load_verify_locations(cafile="certs/ca.crt")
        # Present client certificate for mTLS authentication with the real SF server
        ctx.load_cert_chain(
            certfile=self.config["proxy"]["cert_file"],
            keyfile=self.config["proxy"]["key_file"],
        )
        return ctx

    def _find_device_by_mac(self, mac: str) -> Optional[dict]:
        mac_clean = mac.upper().replace(":", "")
        for d in self.config.get("devices", []):
            if d["mac"].upper().replace(":", "") == mac_clean:
                return d
        return None

    def _find_device_by_id(self, device_id: str) -> Optional[dict]:
        for d in self.config.get("devices", []):
            if d["id"] == device_id:
                return d
        return None

    def _auto_detect_mac(self, mac: str) -> Optional[dict]:
        """If a device has the placeholder MAC, replace it with the detected MAC and persist."""
        mac_clean = mac.upper().replace(":", "")
        for dev in self.config.get("devices", []):
            if dev["mac"].upper().replace(":", "") == _MAC_PLACEHOLDER:
                dev["mac"] = mac_clean
                logger.info(
                    "┌─────────────────────────────────────────────┐\n"
                    "│  🕷  SpiderBridge — device detected         │\n"
                    "│  MAC: %-38s  │\n"
                    "│  ID:  %-38s  │\n"
                    "│  updating config.yaml...                    │\n"
                    "└─────────────────────────────────────────────┘",
                    mac_clean, dev.get("friendly_name", dev["id"]),
                )
                self._save_config()
                return dev
        return None

    def _save_config(self) -> None:
        """Persist current in-memory config. Under HA OS the addon writes
        only the devices list to /data/devices.yaml (the rest of the addon
        config is read-only options). Standalone falls back to the full
        config.yaml at the configured path."""
        if Path(HA_OPTIONS_PATH).exists():
            try:
                with open(HA_DEVICES_PATH, "w") as f:
                    yaml.dump(self.config.get("devices", []), f)
                logger.info("/data/devices.yaml aktualisiert.")
            except Exception as e:
                logger.error("Fehler beim Speichern von /data/devices.yaml: %s", e)
        else:
            try:
                with open(self._config_path, "w") as f:
                    yaml.dump(self.config, f, default_flow_style=False, allow_unicode=True)
                logger.info("config.yaml aktualisiert.")
            except Exception as e:
                logger.error("Fehler beim Speichern von config.yaml: %s", e)

    async def poll_session_config(self, sess: ProxySession) -> None:
        """Inject one round of getConfigField for light/light2/fan/blower
        into a single session. Called on session connect (immediate poll)
        and periodically by config_poll_loop."""
        for keypath in (["device", "light"], ["device", "light2"],
                        ["device", "fan"], ["device", "blower"]):
            try:
                await sess.inject({
                    "method": "getConfigField",
                    "pid": sess.mac,
                    "params": {"keyPath": keypath},
                    "msgId": str(int(time.time() * 1000)),
                    "uid": sess.uid,
                })
            except Exception as e:
                logger.debug("config poll inject failed for %s: %s",
                             keypath, e)
            # Space out so we don't flood the controller in one burst
            await asyncio.sleep(0.5)

    async def config_poll_loop(self) -> None:
        """Periodically poll every active controller session. Each session
        also gets an immediate one-shot poll on connect (handle_client),
        so this loop only owns the recurring tick.

        Interval is configurable via proxy.config_poll_interval_sec
        (default 600 = 10 minutes). Set to 0 to disable."""
        interval = int(self.config.get("proxy", {}).get("config_poll_interval_sec", 600))
        if interval <= 0:
            logger.info("Config poll disabled (interval=%s)", interval)
            return
        logger.info("Config poll loop started, interval=%ds", interval)
        while True:
            try:
                await asyncio.sleep(interval)
                for sess in list(self._sessions.values()):
                    await self.poll_session_config(sess)
            except asyncio.CancelledError:
                logger.info("Config poll loop stopped")
                break
            except Exception as e:
                logger.warning("Config poll loop error: %s", e)
                await asyncio.sleep(30)

    async def handle_command(self, topic: str, value: str) -> None:
        """Handle an incoming HA command from local Mosquitto."""
        # topic: spiderfarmer/{device_id}/command/{field}[/{subfield}]/set
        parts = topic.split("/")
        if len(parts) < 5:
            return
        device_id = parts[1]
        field = parts[3]
        subfield = parts[4] if len(parts) >= 6 and parts[4] != "set" else None

        session = self._sessions.get(device_id)
        if session is None:
            logger.warning("Command for %s but no active session", device_id)
            return

        outlet_num = None
        if field.startswith("outlet_") and field[7:].isdigit():
            n = int(field[7:])
            # GGS controllers expose at most 10 outlets — anything else is a
            # malformed topic the bridge should not forward.
            if 1 <= n <= 10:
                outlet_num = n
            else:
                logger.warning("Out-of-range outlet number in command: %s", field)
                return

        # The bare light on/off path is intercepted for the temp-override
        # behaviour (Schedule/PPFD off → Manual+off now, restore later). All
        # other fields — and light *sub*field writes — go straight through.
        if field in ("light", "light2") and subfield is None:
            payload = self._light_payload_with_override(session, field, value)
        else:
            payload = translate_command(field, value, session.mac, session.uid, outlet_num,
                                        device_state=session.device_state, subfield=subfield,
                                        last_nonzero_level=session.last_nonzero_level,
                                        fan_state=session.fan_state,
                                        light_state=session.light_state)
        if payload:
            await self._inject_and_publish(session, payload)

    async def _inject_and_publish(self, session: "ProxySession", payload: dict) -> bool:
        """Inject a translated payload and optimistically reflect it into the
        fan/light caches + HA state topics, so the UI updates without waiting
        for the next controller echo. Shared by the command and restore paths.
        Returns whether the inject actually reached the wire."""
        ok = await session.inject(payload)
        params = payload.get("params", {})
        for k in ("fan", "blower"):
            blk = params.get(k)
            if isinstance(blk, dict):
                session.fan_state[k] = blk
                for tpc, val in fan_extras_topics(session.device_id, k, blk).items():
                    self.mqtt_client.publish(tpc, val, retain=True)
        # Light — refresh the main state/light JSON so the HA effect dropdown
        # reflects the new mode immediately rather than waiting for the next
        # getDevSta (which on some firmwares does not even carry modeType).
        for k in ("light", "light2"):
            blk = params.get(k)
            if isinstance(blk, dict):
                # Merge, not overwrite — partial setConfigField frames (e.g.
                # just on/off or brightness) must not clobber a previously
                # cached modeType, otherwise the next status update falls back
                # to Manual mid-schedule.
                session.light_state.setdefault(k, {}).update(blk)
                # Pass the FULL merged cache to the normalizer, not the partial
                # blk — otherwise editing a sub-field like schedule.timeOnStart
                # republishes state/light with state=OFF, brightness=0 (because
                # blk has no on/level) and clobbers the live light state in HA.
                merged = session.light_state[k]
                refreshed = normalize_status(
                    session.device_id, {"data": {k: merged}},
                    light_cache=session.light_state,
                )
                for tpc, val in refreshed.items():
                    self.mqtt_client.publish(tpc, val, retain=True)
        return ok

    def _light_payload_with_override(
        self, session: "ProxySession", field: str, value: str,
    ) -> Optional[dict]:
        """Resolve a bare light command, applying temporary-override semantics.

        - explicit effect pick → clear any override, translate as-is
        - turn ON while an override is pending → resume the original mode now
        - turn OFF in Schedule/PPFD with no override → arm one: Manual+off now,
          restore the original mode at the next schedule boundary
        - everything else → normal mode-preserving translate
        """
        try:
            cmd = json.loads(value)
        except (ValueError, TypeError):
            cmd = {"state": value}
        cur = session.light_state.get(field) or session.device_state.get(field, {})
        cur_mode = int(cur.get("modeType", 0))
        has_effect = "effect" in cmd
        has_brightness = "brightness" in cmd
        state_on = _onoff(cmd.get("state", "ON"))
        override = self.override_mgr.get(session.device_id, field)

        def _translate(v):
            return translate_command(
                field, v, session.mac, session.uid, None,
                device_state=session.device_state,
                last_nonzero_level=session.last_nonzero_level,
                light_state=session.light_state,
            )

        # Explicit mode pick — the user chose a mode deliberately; drop any
        # pending override so it can't later stomp the user's choice.
        if has_effect:
            if override:
                self.override_mgr.clear(session.device_id, field)
                logger.info("[%s] Temp-Override verworfen — Nutzer wählte Modus %s",
                            session.device_id, cmd.get("effect"))
            return _translate(value)

        # Manual resume: turning the lamp back on cancels the override and
        # restores the original (Schedule/PPFD) mode immediately.
        if override and state_on == 1 and not has_brightness:
            orig = int(override["original_mode"])
            self.override_mgr.clear(session.device_id, field)
            effect = LIGHT_MODE_TO_EFFECT.get(orig, "Schedule")
            logger.info("[%s] Temp-Override aufgehoben (manuelles AN) → Modus %s wiederhergestellt",
                        session.device_id, effect)
            return _translate(json.dumps({"state": "ON", "effect": effect}))

        # Temp-override arm: OFF while in a non-Manual mode, none pending.
        # Transactional ordering — build the Manual+off payload first, then
        # persist the restore record, and only THEN return the payload (the
        # caller injects it afterwards). If either step can't be guaranteed we
        # fall through to the mode-preserving no-op so the lamp stays in its
        # schedule rather than going dark with no way back.
        if (state_on == 0 and not has_brightness and not has_effect
                and not override and cur_mode in (1, 12)):
            restore_at = self._compute_light_restore_at(cur, cur_mode)
            if restore_at is None:
                # Cold cache / no schedule → no computable boundary. Do NOT
                # darken the lamp; warn so the case is visible in the log.
                logger.warning(
                    "[%s] Temp-Override Licht AUS: kein Schedule-Boundary berechenbar "
                    "(Cache kalt?) → kein Override, Licht folgt weiter dem Plan",
                    session.device_id,
                )
            else:
                off_payload = _translate(json.dumps({"state": "OFF", "effect": "Manual"}))
                if off_payload is None:
                    logger.error(
                        "[%s] Temp-Override: Manuell+Aus-Payload nicht baubar → "
                        "kein Override", session.device_id,
                    )
                elif self.override_mgr.arm(session.device_id, field, cur_mode,
                                           restore_at, armed_at=time.time()):
                    logger.info(
                        "[%s] Temp-Override Licht AUS (Modus %s) → Manuell+Aus jetzt, "
                        "Modus-Wiederherstellung in %d s",
                        session.device_id, LIGHT_MODE_TO_EFFECT.get(cur_mode, cur_mode),
                        int(restore_at - time.time()),
                    )
                    return off_payload
                else:
                    # Persist failed → restore record is not durable. Refuse to
                    # darken the lamp (an unrecoverable off is the worst case).
                    logger.error(
                        "[%s] Temp-Override: Persistenz fehlgeschlagen → Licht "
                        "bleibt im Plan (kein Aus)", session.device_id,
                    )

        # Default: mode-preserving translate (current deployed behaviour).
        return _translate(value)

    def _compute_light_restore_at(self, cur: dict, cur_mode: int) -> Optional[float]:
        """Epoch to restore the original mode, from the schedule cached in the
        light block. PPFD uses ppfdPeriod, Schedule uses timePeriod.

        The controller stores startTime/endTime as seconds-since-LOCAL-midnight,
        so we derive now_local from the Pi's local clock. This assumes the Pi
        (HA OS) and the GGS share a timezone — true on a single LAN. A DST jump
        only shifts a restore by ≤1 h once, which at worst resumes the schedule
        slightly early/late; it can never strand the lamp."""
        period = cur.get("ppfdPeriod" if cur_mode == 12 else "timePeriod")
        now = time.time()
        lt = time.localtime(now)
        now_local = lt.tm_hour * 3600 + lt.tm_min * 60 + lt.tm_sec
        return compute_restore_at(now, now_local, period)

    async def override_restore_loop(self) -> None:
        """Every 30 s, restore the original mode for any override whose deadline
        has passed. Hardened so the lamp is never left dark:
          * a failed inject does NOT clear the override → retried next tick;
          * a device offline at the deadline keeps the override (still due) and
            is retried once it reconnects (self-healing across a proxy restart);
          * a per-entry error never blocks the other overrides;
          * an override for a device gone far past its window is evicted so it
            can't fire a stale mode-switch on a future same-id controller."""
        logger.info("Override restore loop started")
        while True:
            try:
                await asyncio.sleep(30)
                now = time.time()
                for device_id, field, orig_mode in self.override_mgr.due(now):
                    try:
                        await self._restore_one(device_id, field, orig_mode, now)
                    except Exception as e:
                        logger.warning("[%s] Override restore error (retry next tick): %s",
                                       device_id, e)
            except asyncio.CancelledError:
                logger.info("Override restore loop stopped")
                break
            except Exception as e:
                logger.warning("Override restore loop error: %s", e)
                await asyncio.sleep(30)

    async def _restore_one(self, device_id: str, field: str, orig_mode: int,
                           now: float) -> None:
        """Restore a single due override. Clears the record ONLY when the
        restore actually reached the controller — a silent inject failure or an
        offline device keeps the override alive for the next tick. A device gone
        past 2× the max window is evicted (stale, not stranded by us)."""
        entry = self.override_mgr.get(device_id, field)
        if entry and now - entry.get("armed_at", now) > 2 * MAX_OVERRIDE_SEC:
            self.override_mgr.clear(device_id, field)
            logger.warning("[%s] Temp-Override verfallen (Gerät zu lange offline) "
                           "→ verworfen", device_id)
            return
        sess = self._sessions.get(device_id)
        if sess is None:
            return  # offline — still due, retry next tick
        effect = LIGHT_MODE_TO_EFFECT.get(orig_mode, "Schedule")
        payload = translate_command(
            field, json.dumps({"state": "ON", "effect": effect}),
            sess.mac, sess.uid, None,
            device_state=sess.device_state,
            last_nonzero_level=sess.last_nonzero_level,
            light_state=sess.light_state,
        )
        if payload is None:
            logger.error("[%s] Temp-Override Restore-Payload nicht baubar (Modus %s) "
                         "→ erneuter Versuch", device_id, effect)
            return
        if await self._inject_and_publish(sess, payload):
            self.override_mgr.clear(device_id, field)
            logger.info("[%s] Temp-Override abgelaufen → Modus %s wiederhergestellt",
                        device_id, effect)
        else:
            logger.warning("[%s] Temp-Override Restore-Inject fehlgeschlagen "
                           "→ erneuter Versuch nächster Tick", device_id)

    async def handle_client(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ) -> None:
        peer = client_writer.get_extra_info("peername")
        logger.info("New connection from %s", peer)
        upstream_writer = None
        nonlocal_session = [None]

        try:
            ssl_ctx = self._build_upstream_ssl_ctx()
            # Verbinde zur sauber aufgelösten IP (am Loop vorbei), aber SNI/Cert
            # weiter über den Hostnamen.
            upstream_reader, upstream_writer = await asyncio.open_connection(
                self.config["proxy"].get("upstream_ip") or self.config["proxy"]["upstream_host"],
                self.config["proxy"]["upstream_port"],
                ssl=ssl_ctx,
                server_hostname=self.config["proxy"]["upstream_host"],
            )

            async def on_connect_packet(client_id: str) -> ProxySession:
                dev = self._find_device_by_mac(client_id)
                if dev is None:
                    # Try auto-detecting: replace placeholder MAC with real one
                    dev = self._auto_detect_mac(client_id)
                if dev is None:
                    logger.warning("Unknown device client_id=%s — session marked as unknown", client_id)
                    sid = f"unknown_{client_id.replace(':', '')}"
                    s = ProxySession(sid, client_id, "", self.mqtt_client)
                else:
                    s = ProxySession(dev["id"], dev["mac"], dev.get("uid", ""),
                                     self.mqtt_client)
                s.set_upstream(upstream_writer)
                s.set_client(client_writer)
                self._sessions[s.device_id] = s
                s.publish_availability("online")
                logger.info("Session erstellt: device_id=%s mac=%s", s.device_id, s.mac)
                # Immediate one-shot config poll so HA caches refresh on
                # restart/reconnect rather than waiting for the next 10-min
                # tick. Detached so the connect path doesn't block on it.
                async def _initial_poll():
                    # Small delay so the controller has finished its CONNECT
                    # handshake before we start firing extra requests at it.
                    await asyncio.sleep(3)
                    await self.poll_session_config(s)
                # Track the task so handle_client can cancel it on cleanup.
                s.initial_poll_task = asyncio.create_task(_initial_poll())
                return s

            async def relay_up():
                buf = b""
                try:
                    while True:
                        try:
                            data = await client_reader.read(4096)
                        except Exception:
                            break
                        if not data:
                            break
                        buf += data
                        packets, buf = parse_packets(buf)
                        for pkt in packets:
                            if pkt.packet_type == MQTT_CONNECT and pkt.client_id:
                                nonlocal_session[0] = await on_connect_packet(pkt.client_id)
                            elif pkt.packet_type == MQTT_SUBSCRIBE and pkt.topics:
                                # Learn the controller's DOWN topic prefix
                                # immediately from its SUBSCRIBE — no SF App
                                # interaction or cloud command needed.
                                sess = nonlocal_session[0]
                                if sess is not None:
                                    for t in pkt.topics:
                                        parts = t.split("/")
                                        if (len(parts) >= 6 and parts[0] == "SF"
                                                and parts[1] == "GGS"
                                                and parts[3] == "API"
                                                and parts[4] == "DOWN"
                                                and parts[2]):
                                            new_prefix = parts[2]
                                            if sess.down_topic_prefix != new_prefix:
                                                logger.info(
                                                    "[%s] DOWN topic prefix learned from SUBSCRIBE: %s (was %s)",
                                                    sess.device_id, new_prefix,
                                                    sess.down_topic_prefix,
                                                )
                                                sess.down_topic_prefix = new_prefix
                            elif pkt.packet_type == MQTT_PUBLISH:
                                if nonlocal_session[0]:
                                    dev_cfg = self._find_device_by_id(nonlocal_session[0].device_id) or {}
                                    _process_publish(nonlocal_session[0], pkt, self.mqtt_client,
                                                     self._known_soil_ids, dev_cfg)
                        try:
                            upstream_writer.write(data)
                            await upstream_writer.drain()
                        except Exception:
                            break
                finally:
                    # Signal EOF to upstream so relay_down unblocks
                    try:
                        upstream_writer.close()
                    except Exception:
                        pass

            async def relay_down():
                # Forward server→device traffic unchanged. Earlier we parsed
                # packets here and mutated setConfigField bodies to keep HA's
                # last command sticky against the SF cloud's corrections, but
                # that fought legitimate app/cloud commands and made the lamp
                # uncontrollable except from bluetooth-paired sessions.
                # Diagnostic-only parsing here logs outlet-related commands
                # from the SF cloud/app so we can compare what the official
                # app sends vs what we send for PS5/PS10 outlet control.
                buf_down = b""
                try:
                    while True:
                        try:
                            data = await upstream_reader.read(4096)
                        except Exception:
                            break
                        if not data:
                            break
                        # Forward bytes unchanged FIRST, then try to parse for logging
                        try:
                            client_writer.write(data)
                            await client_writer.drain()
                        except Exception:
                            break
                        try:
                            buf_down += data
                            packets, buf_down = parse_packets(buf_down)
                            for p in packets:
                                if (p.packet_type == MQTT_PUBLISH and p.topic
                                        and "/API/DOWN/" in p.topic and p.message):
                                    # Learn the cloud's DOWN topic prefix so our
                                    # injects target the same one (PS5/PS10 may
                                    # not be CB).
                                    sess = nonlocal_session[0]
                                    if sess is not None:
                                        topic_parts = p.topic.split("/")
                                        if len(topic_parts) >= 6 and topic_parts[2]:
                                            new_prefix = topic_parts[2]
                                            if sess.down_topic_prefix != new_prefix:
                                                logger.info(
                                                    "[%s] DOWN topic prefix learned: %s (was %s)",
                                                    sess.device_id, new_prefix,
                                                    sess.down_topic_prefix,
                                                )
                                                sess.down_topic_prefix = new_prefix
                                    try:
                                        body = json.loads(p.message)
                                    except Exception:
                                        continue
                                    if body.get("method") != "setConfigField":
                                        continue
                                    params = body.get("params", {})
                                    keypath = params.get("keyPath", [])
                                    if "outlet" in keypath:
                                        logger.debug(
                                            "SF→device outlet command: topic=%s keyPath=%s params=%s",
                                            p.topic, keypath,
                                            json.dumps(params, separators=(',', ':')),
                                        )
                                    # Fan feature-parity capture: log every
                                    # cloud-side setConfigField for fan/blower
                                    # at INFO level on this branch so we can
                                    # reverse-engineer the SF App's fan
                                    # settings screen field-by-field.
                                    if "fan" in keypath or "blower" in keypath:
                                        logger.info(
                                            "[FAN-CAPTURE] keyPath=%s params=%s",
                                            keypath,
                                            json.dumps(params, separators=(',', ':')),
                                        )
                                        sess = nonlocal_session[0]
                                        if sess is not None:
                                            for k in ("fan", "blower"):
                                                if k in keypath and isinstance(params.get(k), dict):
                                                    sess.fan_state[k] = params[k]
                                                    for tpc, val in fan_extras_topics(
                                                            sess.device_id, k, params[k]).items():
                                                        self.mqtt_client.publish(tpc, val, retain=True)
                                    if "light" in keypath or "light2" in keypath:
                                        sess = nonlocal_session[0]
                                        if sess is not None:
                                            for k in ("light", "light2"):
                                                if k in keypath and isinstance(params.get(k), dict):
                                                    # Merge, not overwrite —
                                                    # see same fix in the
                                                    # injected setConfigField
                                                    # path above.
                                                    sess.light_state.setdefault(k, {}).update(params[k])
                                                    # Also push the per-field
                                                    # extras so HA settings
                                                    # entities populate even
                                                    # without a getDevSta echo.
                                                    for tpc, val in light_extras_topics(
                                                            sess.device_id, k, params[k]).items():
                                                        self.mqtt_client.publish(tpc, val, retain=True)
                        except Exception as e:
                            # Never let logging break the relay
                            logger.debug("relay_down parse error (non-fatal): %s", e)
                            buf_down = b""
                finally:
                    # Signal EOF to client so relay_up unblocks if upstream disconnects first
                    try:
                        client_writer.close()
                    except Exception:
                        pass

            # Spawn each relay as a task and cancel the sibling when one
            # exits — gather() alone leaves the other half reading until
            # the underlying socket EOFs, leaking an upstream connection
            # if relay_up dies first (or vice versa).
            up_task = asyncio.create_task(relay_up())
            down_task = asyncio.create_task(relay_down())
            try:
                done, pending = await asyncio.wait(
                    {up_task, down_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for t in pending:
                    t.cancel()
                # Drain so cancellations finish (and any genuine error
                # surfaces via the awaited task).
                for t in done:
                    if t.exception() is not None:
                        raise t.exception()  # type: ignore[misc]
                for t in pending:
                    try:
                        await t
                    except (asyncio.CancelledError, Exception):
                        pass
            except asyncio.CancelledError:
                up_task.cancel()
                down_task.cancel()
                raise

        except ssl.SSLError as e:
            logger.warning("TLS MITM failed (%s) — transparent TCP relay fallback", e)
            await _tcp_relay_fallback(
                client_reader, client_writer,
                self.config["proxy"].get("upstream_ip") or self.config["proxy"]["upstream_host"],
                self.config["proxy"]["upstream_port"],
            )
        except Exception as e:
            logger.error("Connection error from %s: %s", peer, e)
        finally:
            s = nonlocal_session[0]
            if s:
                # Cancel the detached initial-poll if it's still pending so
                # it doesn't run against a closed upstream writer.
                if s.initial_poll_task is not None and not s.initial_poll_task.done():
                    s.initial_poll_task.cancel()
                # Only mark offline + remove from session registry if THIS
                # session is still the current one. After a controller
                # power-cycle the new CONNECT replaces _sessions[device_id]
                # while the old TCP relay tasks are still draining; if we
                # blindly publish offline here we'd flip HA to "unavailable"
                # right after the new session marked the device online.
                current = self._sessions.get(s.device_id)
                if current is s:
                    s.publish_availability("offline")
                    self._sessions.pop(s.device_id, None)
                else:
                    logger.info(
                        "[%s] stale session cleanup — newer session already active, "
                        "skipping offline publish",
                        s.device_id,
                    )
            if upstream_writer:
                try:
                    upstream_writer.close()
                except Exception:
                    pass
            try:
                client_writer.close()
            except Exception:
                pass
            logger.info("Connection from %s closed", peer)


def _process_publish(session: ProxySession, pkt, mqtt_client: mqtt.Client,
                     known_soil_ids: dict, device_cfg: dict) -> None:
    """Normalize a PUBLISH from controller and republish locally."""
    if pkt.topic is None or pkt.message is None:
        return
    # SF protocol: SF/GGS/{prefix}/API/UP/{MAC}. CB is the prefix observed for
    # the Control Box, but standalone PS5/PS10/LC controllers may use a
    # different prefix. Accept any prefix and log the first sighting of each
    # non-CB one so we can add explicit support if needed.
    parts = pkt.topic.split("/")
    if (len(parts) < 6 or parts[0] != "SF" or parts[1] != "GGS"
            or parts[3] != "API" or parts[4] != "UP"):
        return
    prefix = parts[2]
    if prefix != "CB" and prefix not in _seen_topic_prefixes:
        _seen_topic_prefixes.add(prefix)
        logger.info("New SF topic prefix observed: %s (topic=%s)", prefix, pkt.topic)
    try:
        data = json.loads(pkt.message)
    except Exception:
        return
    method = data.get("method")
    # Accept any UP message that carries a data block — getDevSta is the
    # main one, but getConfigField responses (when the controller honors
    # them) come through here too. Setup-Acks without payload data fall
    # through harmlessly because their "data" dict has no module blocks.
    if method not in ("getDevSta", "getConfigField"):
        return
    if method == "getConfigField":
        logger.info("[CONFIG-RESP] data=%s",
                    json.dumps(data.get("data", {}), separators=(',', ':'))[:500])

    # Keep UID up to date from controller messages
    uid = data.get("uid", "")
    if uid and session.uid != uid:
        session.uid = uid

    # Store current module states for use in commands
    d = data.get("data", {})
    for module in ("light", "light2", "blower", "fan", "heater", "humidifier", "dehumidifier"):
        if module in d and isinstance(d[module], dict):
            # Merge instead of replace — getDevSta on some firmwares carries
            # only {on, level} for light/fan, which would wipe cached fields
            # (modeType, schedule, etc) we need to keep. isinstance guard
            # prevents update(None) crashes when firmware sends junk.
            session.device_state.setdefault(module, {}).update(d[module])
    # Also feed light/fan caches from rich UP messages (getConfigField
    # responses, full setConfigField echoes). Same merge semantics so
    # minimal getDevSta echoes don't clobber the cached schedule/cycle.
    for module in ("light", "light2"):
        if module in d and isinstance(d[module], dict):
            session.light_state.setdefault(module, {}).update(d[module])
    for module in ("fan", "blower"):
        if module in d and isinstance(d[module], dict):
            session.fan_state.setdefault(module, {}).update(d[module])

    # Remember last non-zero brightness/speed so OFF→ON restores the
    # previous level for lights AND fans/blowers — without this the
    # toggle handler in command_handler.py defaults to a hard-coded
    # level (5/50/100) on first ON after an OFF, ignoring the user's
    # last setting.
    for module in ("light", "light2", "fan", "blower"):
        if module in d and isinstance(d[module], dict):
            lvl = d[module].get("level", d[module].get("mLevel", 0))
            if isinstance(lvl, (int, float)) and lvl > 0:
                session.last_nonzero_level[module] = int(lvl)

    # Publish discovery for newly seen soil sensor IDs
    seen = known_soil_ids.setdefault(session.device_id, set())
    for s in data.get("data", {}).get("sensors", []):
        sid = s.get("id")
        if sid and sid != "avg" and sid not in seen:
            seen.add(sid)
            _publish_soil_sensor_discovery(mqtt_client, session.device_id, sid, device_cfg)

    # Prune outlet discovery once per session: static discovery publishes
    # 1..10 unconditionally; remove the ones the actual hardware does not
    # have so HA stops showing ghost switches.
    if not session._outlet_discovery_pruned:
        outlet_block = d.get("outlet", {})
        if isinstance(outlet_block, dict) and outlet_block:
            present = {
                int(k[1:]) for k in outlet_block.keys()
                if isinstance(k, str) and k.startswith("O") and k[1:].isdigit()
            }
            if present:
                for n in range(1, 11):
                    if n not in present:
                        _unpublish_outlet_discovery(mqtt_client, session.device_id, n)
                logger.info("[%s] Outlet discovery pruned to %s",
                            session.device_id, sorted(present))
                session._outlet_discovery_pruned = True

    normalized = normalize_status(
        session.device_id, data,
        light_cache=getattr(session, "light_state", None),
        fan_cache=getattr(session, "fan_state", None),
        # getConfigField returns the *configured* mOnOff/mLevel defaults,
        # not the live on/level. Without this flag the 10-min config poll
        # republishes state=ON during a schedule off-phase and HA logs a
        # ~3s "on → off" flicker each cycle.
        is_config_resp=(method == "getConfigField"),
    )
    for norm_topic, value in normalized.items():
        mqtt_client.publish(norm_topic, value, retain=True)


async def _tcp_relay_fallback(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    upstream_host: str,
    upstream_port: int,
) -> None:
    """
    Called when upstream TLS connection fails (e.g. certificate verification error).

    A transparent TCP relay is not feasible here because:
    1. The upstream server (sf.mqtt.spider-farmer.com:8883) is TLS-only
    2. The GGS Controller has already completed a TLS handshake with our server cert

    If cert pinning is the issue (controller rejects our cert), the failure happens
    before handle_client is called, not here. An SSLError here means the upstream
    connection failed — check TROUBLESHOOTING.md.

    We close both connections cleanly and let the controller reconnect.
    """
    logger.warning(
        "Upstream TLS failed — closing connection. "
        "Check TROUBLESHOOTING.md if this persists. "
        "Host: %s:%s", upstream_host, upstream_port
    )
    try:
        client_writer.close()
    except Exception:
        pass
