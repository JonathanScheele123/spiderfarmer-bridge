import json
import logging
import random
import socket
import struct
from pathlib import Path
import yaml

logger = logging.getLogger(__name__)

# Bekannt-gute SF-Cloud-IP (EMQX, issuer O=MZ) als Fallback, falls der saubere
# DNS-Lookup beim Boot scheitert. Stand 2026-06: sf.mqtt.spider-farmer.com.
SF_UPSTREAM_FALLBACK_IP = "18.192.38.50"


def _resolve_via_public_dns(
    host: str,
    fallback: str,
    resolvers=("1.1.1.1", "8.8.8.8"),
    timeout: float = 3.0,
) -> str:
    """A-Record über einen ÖFFENTLICHEN Resolver auflösen — am System-Resolver
    vorbei.

    Im DNS-Interception-Modus zeigt der lokale Resolver (dnsmasq) den Upstream
    ``sf.mqtt.spider-farmer.com`` auf DIESEN Host zurück. Würde der Proxy seinen
    Upstream über den System-Resolver auflösen, verbände er sich mit sich selbst
    → Endlosschleife, GGS fällt ab. Darum hier eine eigenständige UDP-DNS-Abfrage
    direkt an 1.1.1.1/8.8.8.8. Bei jedem Fehler: Fallback-IP.
    """
    qname = b"".join(
        struct.pack("B", len(p)) + p.encode("ascii") for p in host.split(".")
    ) + b"\x00"
    query = (
        struct.pack(">HHHHHH", random.randint(0, 0xFFFF), 0x0100, 1, 0, 0, 0)
        + qname
        + struct.pack(">HH", 1, 1)  # QTYPE=A, QCLASS=IN
    )
    for rs in resolvers:
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(timeout)
            sock.sendto(query, (rs, 53))
            data, _ = sock.recvfrom(512)
            ancount = struct.unpack(">H", data[6:8])[0]
            idx = 12
            while data[idx] != 0:  # Frage-Namen überspringen
                idx += data[idx] + 1
            idx += 5  # 0-Byte + QTYPE(2) + QCLASS(2)
            for _ in range(ancount):
                if data[idx] & 0xC0 == 0xC0:  # komprimierter Name (Pointer)
                    idx += 2
                else:
                    while data[idx] != 0:
                        idx += data[idx] + 1
                    idx += 1
                rtype, _rclass, _ttl, rdlen = struct.unpack(">HHIH", data[idx:idx + 10])
                idx += 10
                if rtype == 1 and rdlen == 4:  # A-Record
                    ip = ".".join(str(b) for b in data[idx:idx + 4])
                    logger.info("Upstream %s via %s → %s (sauber, am Loop vorbei)", host, rs, ip)
                    return ip
                idx += rdlen
        except Exception as e:  # noqa: BLE001 — jeder Fehler → nächster Resolver/Fallback
            logger.warning("Sauberer DNS-Lookup über %s fehlgeschlagen: %s", rs, e)
        finally:
            if sock is not None:
                sock.close()
    logger.warning("Sauberer DNS-Lookup gescheitert — Fallback-IP %s", fallback)
    return fallback

HA_OPTIONS_PATH = "/data/options.json"
HA_DEVICES_PATH = "/data/devices.yaml"
HA_MQTT_PATH = "/data/mqtt.json"  # written by cont-init/02b-mqtt-discovery
HA_OVERRIDES_PATH = "/data/overrides.json"  # pending temporary light overrides


# Hardcoded so HA generates entity_ids with a "ggs_*" prefix consistently
# across both install paths — required for the Lovelace card to find
# the entities. Renaming would silently break <ggs-card>.
GGS_FRIENDLY_NAME = "GGS"


def _default_devices() -> list:
    return [
        {
            "mac": "AABBCCDDEEFF",
            "type": "CB",
            "id": "ggs_1",
            "uid": "",
            "friendly_name": GGS_FRIENDLY_NAME,
        }
    ]


def _load_ha_devices() -> list:
    p = Path(HA_DEVICES_PATH)
    if p.exists():
        try:
            with open(p) as f:
                return yaml.safe_load(f) or _default_devices()
        except yaml.YAMLError as e:
            logger.warning("Corrupt devices.yaml, using defaults: %s", e)
    return _default_devices()


def _load_ha_mqtt() -> dict:
    """Return MQTT broker config written by cont-init/02b-mqtt-discovery.

    When Supervisor has an external MQTT service (e.g. the official
    Mosquitto add-on), the cont-init script writes its host/port/creds
    to ``/data/mqtt.json``. If the file is absent we fall back to the
    addon-local Mosquitto on 127.0.0.1:1883.
    """
    p = Path(HA_MQTT_PATH)
    if p.exists():
        try:
            with open(p) as f:
                data = json.load(f)
            return {
                "host": data.get("host", "127.0.0.1"),
                "port": int(data.get("port", 1883)),
                "username": data.get("username", "") or "",
                "password": data.get("password", "") or "",
            }
        except (json.JSONDecodeError, OSError, ValueError) as e:
            logger.warning("Could not read %s, using local Mosquitto: %s", HA_MQTT_PATH, e)
    return {"host": "127.0.0.1", "port": 1883, "username": "", "password": ""}


def _build_config_from_ha_options(options: dict) -> dict:
    mqtt = _load_ha_mqtt()
    return {
        "hotspot": {
            "enabled": options.get("hotspot_enabled", True),
            "ssid": options.get("ssid", "SF-Bridge"),
            "password": options.get("password", "changeme123"),
            # wlan0 and the SF upstream host are fixed for this add-on (single-purpose design)
            "interface": "wlan0",
            "ip": options.get("hotspot_ip", "192.168.10.1"),
            "channel": options.get("channel", 6),
        },
        "proxy": {
            "listen_host": "0.0.0.0",
            # 18883 not 8883: in HA addon mode we share host_network with
            # the Mosquitto addon, which already binds 0.0.0.0:8883 for
            # MQTT-over-TLS. cont-init/01-hotspot-setup adds an iptables
            # PREROUTING REDIRECT 8883→18883 on wlan0 so the GGS still
            # connects to its hardcoded port 8883.
            "listen_port": 18883,
            # wlan0 and the SF upstream host are fixed for this add-on (single-purpose design)
            "upstream_host": "sf.mqtt.spider-farmer.com",
            # Connect-Ziel = sauber aufgelöste IP (am vergifteten lokalen DNS
            # vorbei), aber TLS-SNI/Cert weiter über upstream_host. Verhindert,
            # dass der Proxy sich im DNS-Interception-Modus selbst aufruft.
            "upstream_ip": _resolve_via_public_dns(
                "sf.mqtt.spider-farmer.com", SF_UPSTREAM_FALLBACK_IP
            ),
            "upstream_port": 8883,
            "cert_file": "certs/server.crt",
            "key_file": "certs/server.key",
        },
        "mosquitto": {
            "host": mqtt["host"],
            "port": mqtt["port"],
            "local_user": mqtt["username"],
            "local_password": mqtt["password"],
            "ha_mqtt_password": "",
        },
        "devices": _load_ha_devices(),
    }


def load_config(path: str = "config/config.yaml") -> dict:
    """Return the application config dict.

    In HA mode (when HA_OPTIONS_PATH exists), reads /data/options.json and
    merges with persisted device MACs from HA_DEVICES_PATH. The ``path``
    argument is ignored in this case.

    In standalone mode, loads and returns the YAML file at ``path``.
    Raises FileNotFoundError if that file does not exist.
    """
    ha_opts = Path(HA_OPTIONS_PATH)
    if ha_opts.exists():
        with open(ha_opts) as f:
            return _build_config_from_ha_options(json.load(f))
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with open(p) as f:
        return yaml.safe_load(f)
