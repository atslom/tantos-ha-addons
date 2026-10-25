#!/usr/bin/env python3
"""Tantos Jolly Archive — HA add-on backend (FastAPI + Ingress).

Channel attribution model (SD-INDEPENDENT, read from the hub's own index): the
XiongMai QVFS ring reuses .bin files across channels over time, so a file's
channel is NOT fixed and must come from the hub itself. The hub records it in the
QVBK allocator table inside the active index file (indexNNN.bin): one 64-byte
record per data .bin, where +0x06==1 marks the file as allocated and +0x24 is the
channel ordinal (0=Панель1, 1=Панель2, …). We read this LIVE each refresh
(hub.channel_files) to build file->channel — no .bin numbers and no channel count
are hardcoded, and it survives an SD swap or an added panel because the hub
rebuilds QVBK for whatever card is in it. The active index is discovered
dynamically (the indexNNN.bin Sofia holds open), never hardcoded.

The event LIST is built per-event from the CLUS region (date + type + 4MB-cluster
pointer) of each allocated file; current_epoch_events still drops any not-yet-
overwritten tail from a file's PREVIOUS channel. A clip is cut from just its
cluster region on demand."""
import datetime as dt
import json
import os
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
import zipfile
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask

from tjhub import Hub

# Supervisor-provided token + base URLs for calling the Home Assistant Core API
# (needs homeassistant_api: true in config.yaml). Used to read Alarmo arm history.
SUP_TOKEN = os.environ.get("SUPERVISOR_TOKEN")
SUP_BASES = ["http://supervisor", "http://172.30.32.2"]

OPTS = json.load(open("/data/options.json")) if os.path.exists("/data/options.json") else {
    "hub_ip": "192.168.0.27", "hub_user": "root", "hub_pass": "",
    "self_ip": "192.168.0.32", "cache_max_gb": 20, "cache_retention_days": 30}

# Channel UNIVERSE only (names) — NO .bin numbers in source. The channel ORDINAL
# the hub stores in QVBK (+0x24) indexes straight into this list: 0=Панель 1 …
# 3=Панель 4, 4=Камера 1 … 7=Камера 4. (Panels 0..2 are validated against traced
# ground truth; camera ordinals assume the natural 4..7 order.)
CHANNELS_UNIVERSE = ["Панель 1", "Панель 2", "Панель 3", "Панель 4",
                     "Камера 1", "Камера 2", "Камера 3", "Камера 4"]
MIN_YEAR = 2025   # user: show years from 2025 on (older = ring leftovers)

DATA = "/data"
# Heavy video caches live OUTSIDE /data, in /share (mapped share:rw), so the
# add-on's /data stays tiny. HA backs up the WHOLE of an add-on's /data (no
# per-subfolder exclude), but /share is NOT part of the add-on backup — so clips
# never bloat backups. Only small, valuable state (QVBK map, settings, viewed DB)
# stays in /data and gets backed up.
CACHE = "/share/tantos_jolly_archive/cache"
FILECACHE = "/share/tantos_jolly_archive/filecache"
for d in (CACHE, FILECACHE):
    os.makedirs(d, exist_ok=True)
DB = os.path.join(DATA, "archive.db")
QMAP_PATH = os.path.join(DATA, "qvbk_map.json")   # last QVBK-derived file->channel
SETTINGS_PATH = os.path.join(DATA, "app_settings.json")
LOCK = threading.Lock()
STATE = {"cov": None, "events": {}, "status": "idle", "built": 0, "stale": [],
         "index": None, "last_fail": 0,
         "armed_intervals": None, "armed_hist_start": None, "armed_ts": 0}
_TZ = {"zone": None}
FAIL_COOLDOWN = 30   # after a failed build, wait this long before retrying on access


# ---------- user settings (persisted in /data, survive add-on restart) ----------
def load_settings():
    s = {"hub_ip": OPTS.get("hub_ip", "192.168.0.27"),
         "self_ip": OPTS.get("self_ip", "192.168.0.32"),
         "hub_user": OPTS.get("hub_user", "root"),
         "hub_pass": OPTS.get("hub_pass", ""),
         "cache_max_gb": OPTS.get("cache_max_gb", 20),
         "cache_retention_days": OPTS.get("cache_retention_days", 30),
         "refresh_sec": OPTS.get("refresh_sec", 90),
         "alarm_entity": OPTS.get("alarm_entity", "alarm_control_panel.alarmo"),
         "alarm_disarmed_state": OPTS.get("alarm_disarmed_state", "disarmed"),
         "alarm_poll_sec": OPTS.get("alarm_poll_sec", 900),
         "visible_channels": []}
    if os.path.exists(SETTINGS_PATH):
        try:
            s.update({k: v for k, v in json.load(open(SETTINGS_PATH, encoding="utf-8")).items() if k in s})
        except Exception:
            pass
    return s


def save_settings(s):
    tmp = SETTINGS_PATH + ".tmp"
    json.dump(s, open(tmp, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    os.replace(tmp, SETTINGS_PATH)


SETTINGS = load_settings()


def make_hub():
    return Hub(SETTINGS["hub_ip"], SETTINGS["hub_user"], SETTINGS["hub_pass"], SETTINGS["self_ip"])


hub = make_hub()


def ordinal_name(o):
    return CHANNELS_UNIVERSE[o] if 0 <= o < len(CHANNELS_UNIVERSE) else "Канал %d" % o


def build_channel_map(qv):
    """QVBK map {fileno: {"ch":ordinal,...}} -> {channel_name: {fileno: None}}.
    evidence is None (trusted): QVBK IS the hub's authoritative attribution."""
    m = {k: {} for k in CHANNELS_UNIVERSE}
    for fno, info in qv.items():
        m.setdefault(ordinal_name(info["ch"]), {})[int(fno)] = None
    return m


def load_qmap():
    """Last QVBK-derived file->channel (so channels show before the first refresh,
    and as a fallback if the hub is unreachable). {fileno: {"ch":ordinal}}."""
    if os.path.exists(QMAP_PATH):
        try:
            return {int(k): {"ch": int(v)} for k, v in
                    json.load(open(QMAP_PATH, encoding="utf-8")).items()}
        except Exception:
            pass
    return {}


def save_qmap(qv):
    out = {str(f): info["ch"] for f, info in qv.items()}
    tmp = QMAP_PATH + ".tmp"
    json.dump(out, open(tmp, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    os.replace(tmp, QMAP_PATH)


CHANNEL_MAP = build_channel_map(load_qmap())


def db():
    c = sqlite3.connect(DB); c.row_factory = sqlite3.Row
    c.execute("CREATE TABLE IF NOT EXISTS viewed(channel TEXT, ts TEXT, PRIMARY KEY(channel,ts))")
    return c


# ---------- indexing ----------
def build_coverage():
    """Refresh = (1) read the hub's OWN file->channel map from QVBK in the active
    index, (2) scan the CLUS region of just those allocated files, (3) rebuild the
    per-channel event lists. If the hub is unreachable, keep the last known map."""
    global CHANNEL_MAP
    STATE["status"] = "reading index (QVBK)…"
    idx, qv = hub.channel_files()
    STATE["index"] = idx
    if qv:
        CHANNEL_MAP = build_channel_map(qv)
        save_qmap(qv)
        files = sorted(qv.keys())
    else:
        files = sorted({f for files in CHANNEL_MAP.values() for f in files})
    STATE["status"] = "scanning CLUS (%d files)…" % len(files)
    cov = hub.pull_clus_files(files)
    STATE["cov"] = cov
    rebuild_events()
    STATE["status"] = "ready"; STATE["built"] = int(time.time())
    return cov


def evidence_ok(cov_entry, ev):
    """True if the file still holds the traced date at the traced cluster
    (i.e. the ring hasn't overwritten/reassigned it). No evidence => trusted."""
    if not ev:
        return True
    want_clus = ev.get("clus"); want_date = ev.get("date")
    if want_clus is None or not want_date:
        return True
    for e in cov_entry["events"]:
        if e.get("clus") == want_clus:
            return e["dt"].strftime("%Y-%m-%d %H:%M:%S") == want_date
    return False   # traced cluster no longer present => stale


def current_epoch_events(cov_entry, ev):
    """A .bin is a ring slot the allocator reuses across channels, so its CLUS
    can hold MULTIPLE epochs: the current channel's fresh run plus not-yet-
    overwritten leftovers from a PRIOR channel (e.g. Aug Камера-3 tail inside an
    Oct Панель-3 file). Within one write epoch clusters fill in time order, so an
    epoch boundary is a BACKWARD time jump in cluster order. Return only the
    epoch segment that contains the evidence cluster (else the newest)."""
    evs = sorted((e for e in cov_entry["events"] if e.get("clus") is not None),
                 key=lambda e: e["clus"])
    if not evs:
        return []
    segs = [[evs[0]]]
    for a, b in zip(evs, evs[1:]):
        if (a["dt"] - b["dt"]).total_seconds() > 3600:   # time went backward => new epoch
            segs.append([b])
        else:
            segs[-1].append(b)
    if len(segs) == 1:
        return segs[0]
    if ev and ev.get("clus") is not None:
        for s in segs:
            if any(e.get("clus") == ev["clus"] for e in s):
                return s
    return max(segs, key=lambda s: max(e["dt"] for e in s))


def rebuild_events():
    """Per channel, gather events from its mapped+validated files, keeping only
    each file's CURRENT epoch (drops prior-channel stale tails). Files whose
    evidence no longer matches are dropped and reported as stale."""
    cov = STATE["cov"] or {}
    evs = {}; stale = []
    for name, files in CHANNEL_MAP.items():
        acc = {}
        for fi, ev in files.items():
            c = cov.get(fi)
            if not c:
                continue
            if not evidence_ok(c, ev):
                stale.append({"channel": name, "file": fi}); continue
            for e in current_epoch_events(c, ev):
                if e["dt"].year < MIN_YEAR:
                    continue
                end = e.get("end")
                if end is None or (end - e["dt"]).total_seconds() < 1:
                    continue   # 0-sec CLUS boundary marker (end==start): the hub
                               # writes it after a clip but doesn't list it, and it
                               # has no real segment to extract — drop it
                key = e["dt"].strftime("%Y-%m-%d %H:%M:%S")
                if key not in acc:
                    acc[key] = {"dt": e["dt"], "end": e.get("end"), "type": e["type"],
                                "clus": e.get("clus"), "file": fi}
        evs[name] = sorted(acc.values(), key=lambda x: x["dt"])
    STATE["events"] = evs; STATE["stale"] = stale


def ensure_coverage(force=False):
    """Keep the clip list fresh. The hub is always recording, so a list built once
    goes stale (new events keep arriving). We lazily REBUILD when the cached list is
    older than `refresh_sec` (so opening a channel after that window picks up the
    latest clips), guarded by LOCK (single build at a time) and a short cooldown so
    a wedged/absent hub isn't hammered — stale data is served meanwhile."""
    ttl = SETTINGS.get("refresh_sec", 90)
    now = time.time()
    if not force and STATE["cov"] is not None and (now - STATE["built"]) < ttl:
        return
    if (not force and STATE["cov"] is not None and STATE["last_fail"]
            and now - STATE["last_fail"] < FAIL_COOLDOWN):
        return   # recently failed → serve stale rather than retry every click
    with LOCK:
        now = time.time()
        if not force and STATE["cov"] is not None and (now - STATE["built"]) < ttl:
            return   # another request refreshed while we waited for the lock
        try:
            build_coverage()
            STATE["last_fail"] = 0
        except Exception:
            STATE["last_fail"] = time.time()
            STATE["status"] = "refresh failed (hub unreachable?)"
            if force or STATE["cov"] is None:
                raise


def chan_events(channel):
    ensure_coverage()
    return STATE["events"].get(channel, [])


# ---------- Alarmo "on guard" tagging (clip recorded while armed) ----------
def ha_api(path):
    """GET the Home Assistant Core API via the Supervisor proxy (needs
    homeassistant_api: true). Returns parsed JSON or None on any failure."""
    if not SUP_TOKEN:
        return None
    for base in SUP_BASES:
        try:
            req = urllib.request.Request(base + path)
            req.add_header("Authorization", "Bearer " + SUP_TOKEN)
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read() or b"null")
        except Exception:
            continue
    return None


def ha_tz():
    if _TZ["zone"] is None:
        cfg = ha_api("/core/api/config") or {}
        try:
            _TZ["zone"] = ZoneInfo(cfg.get("time_zone") or "UTC")
        except Exception:
            _TZ["zone"] = ZoneInfo("UTC")
    return _TZ["zone"]


def _armed(state, disarmed):
    """'On guard' = the entity is in ANY state other than its configured 'disarmed'
    value. Works for any entity type: alarm_control_panel -> 'disarmed',
    binary_sensor/input_boolean/switch -> 'off', or any custom state. The user picks
    the entity AND the disarmed-state freely. unavailable/unknown never count."""
    return state not in (disarmed, "unavailable", "unknown", "", None)


ARMED_LOG = os.path.join(DATA, "armed_log.json")
ARMED_POLL = 900          # seconds between alarm polls (15 min). Exact transition
                          # times come from the entity's last_changed, so a long
                          # poll doesn't lose accuracy — only an arm+disarm that both
                          # happen inside one 15-min gap would be missed (rare).
ARMED_LOCK = threading.Lock()


def _load_armed_log():
    try:
        return json.load(open(ARMED_LOG, encoding="utf-8"))
    except Exception:
        return {"entity": None, "transitions": []}


def _save_armed_log(log):
    tmp = ARMED_LOG + ".tmp"
    json.dump(log, open(tmp, "w", encoding="utf-8"), ensure_ascii=False)
    os.replace(tmp, ARMED_LOG)


def _seed_transitions(ent, tz):
    """Initial history from HA recorder (last ~35d, i.e. whatever it still keeps) as
    [[local_iso, state], …] deduped on change. Seeds the log so the past is covered
    until our own polling takes over going forward (unlimited)."""
    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(days=35)
    path = ("/core/api/history/period/%s?filter_entity_id=%s&minimal_response"
            "&no_attributes&end_time=%s"
            % (urllib.parse.quote(start.isoformat()), urllib.parse.quote(ent),
               urllib.parse.quote(end.isoformat())))
    data = ha_api(path)
    out = []; last = None
    if data and data[0]:
        for s in data[0]:
            lc = s.get("last_changed") or s.get("last_updated"); st = s.get("state")
            if not lc or st is None:
                continue
            try:
                t = dt.datetime.fromisoformat(lc).astimezone(tz).replace(tzinfo=None)
            except Exception:
                continue
            if st != last:
                out.append([t.isoformat(), st]); last = st
    return out


def _rebuild_armed_intervals(log, tz):
    dis = SETTINGS.get("alarm_disarmed_state") or "disarmed"
    pts = []
    for iso, st in log.get("transitions", []):
        try:
            pts.append((dt.datetime.fromisoformat(iso), st))
        except Exception:
            continue
    pts.sort(key=lambda x: x[0])
    intervals = []; cur = None
    for t, st in pts:
        if _armed(st, dis) and cur is None:
            cur = t
        elif not _armed(st, dis) and cur is not None:
            intervals.append((cur, t)); cur = None
    if cur is not None:   # still armed now → open interval until now
        intervals.append((cur, dt.datetime.now(tz).replace(tzinfo=None) + dt.timedelta(minutes=1)))
    STATE["armed_intervals"] = intervals
    STATE["armed_hist_start"] = pts[0][0] if pts else None


def armed_tick():
    """One poll cycle: seed the log on first run / entity change, append the CURRENT
    state if it changed (exact time from last_changed), then rebuild intervals. Runs
    on a background timer so the log grows even when the app is idle — the clip's
    'on-guard' status then works for ANY past clip we've logged, not just the ~10d
    HA recorder keeps."""
    ent = SETTINGS.get("alarm_entity")
    if not ent or not SUP_TOKEN:
        STATE["armed_intervals"] = None
        return
    tz = ha_tz()
    with ARMED_LOCK:
        log = _load_armed_log()
        if log.get("entity") != ent:                 # new / changed entity → re-seed
            log = {"entity": ent, "transitions": _seed_transitions(ent, tz)}
            _save_armed_log(log)
        cur = ha_api("/core/api/states/" + urllib.parse.quote(ent))
        if cur and cur.get("state") is not None:
            st = cur.get("state")
            lc = cur.get("last_changed") or cur.get("last_updated")
            try:
                t = dt.datetime.fromisoformat(lc).astimezone(tz).replace(tzinfo=None)
            except Exception:
                t = dt.datetime.now(tz).replace(tzinfo=None)
            tr = log["transitions"]
            if not tr or tr[-1][1] != st:            # append only on a state change
                tr.append([t.isoformat(), st]); _save_armed_log(log)
        STATE["armed_ts"] = time.time()
        _rebuild_armed_intervals(log, tz)


def ensure_armed():
    """Build intervals on first need (before the poller's first tick / right after the
    entity is enabled). The background poller keeps them fresh afterwards."""
    if not SUP_TOKEN or not SETTINGS.get("alarm_entity"):
        STATE["armed_intervals"] = None
        return
    if STATE["armed_intervals"] is None:
        try:
            armed_tick()
        except Exception:
            if STATE["armed_intervals"] is None:
                STATE["armed_intervals"] = []


def _armed_loop():
    while True:
        try:
            armed_tick()
        except Exception:
            pass
        time.sleep(max(60, int(SETTINGS.get("alarm_poll_sec", ARMED_POLL))))


if SUP_TOKEN:
    threading.Thread(target=_armed_loop, daemon=True, name="armed-poller").start()


def is_armed(dtobj):
    """True/False if the clip time is inside/outside an armed interval; None if the
    feature is off or the time predates the available alarm history (unknown)."""
    iv = STATE.get("armed_intervals")
    if iv is None:
        return None
    hs = STATE.get("armed_hist_start")
    if hs is None or dtobj < hs:
        return None
    for a, b in iv:
        if a <= dtobj <= b:
            return True
    return False


def viewed_set(channel):
    return {r["ts"] for r in db().execute("SELECT ts FROM viewed WHERE channel=?", (channel,))}


def is_viewed(channel, dtobj, vset):
    """Viewed = was played AND its clip is still in cache. If the mp4 was pruned
    the indicator goes back to 'not viewed' (dark) — it needs re-extraction."""
    return dtobj.strftime("%Y-%m-%d %H:%M:%S") in vset and clip_cached(channel, dtobj)


def sel_events(channel, type, unviewed=0, armed=0):
    """Events for a channel after ALL active filters (type + unviewed + armed), so
    the year/month/day pickers only ever offer periods that actually have clips."""
    evs = chan_events(channel)
    if type in ("call", "motion"):
        evs = [e for e in evs if e["type"] == type]
    if unviewed:
        vs = viewed_set(channel)
        evs = [e for e in evs if not is_viewed(channel, e["dt"], vs)]
    if armed:
        ensure_armed()
        evs = [e for e in evs if is_armed(e["dt"]) is True]
    return evs


# ---------- file / clip cache ----------
def clip_path(channel, dtobj):
    safe = "%s_%s" % (channel.replace(" ", "_"), dtobj.strftime("%Y%m%d_%H%M%S"))
    return os.path.join(CACHE, safe + ".mp4")


def clip_cached(channel, dtobj):
    return os.path.exists(clip_path(channel, dtobj))


def file_cache_path(fileno):
    return os.path.join(FILECACHE, "%06d.bin" % fileno)


def get_file(fileno, max_age=3600, force=False):
    p = file_cache_path(fileno)
    if (not force and os.path.exists(p)
            and (time.time() - os.path.getmtime(p)) < max_age and os.path.getsize(p) > 2e8):
        return open(p, "rb").read()
    data = hub.pull_file(fileno)
    if len(data) > 2e8:
        tmp = p + ".tmp"; open(tmp, "wb").write(data); os.replace(tmp, p)
        prune_filecache()
    return data


def prune_filecache(keep=6):
    files = sorted((os.path.join(FILECACHE, f) for f in os.listdir(FILECACHE) if f.endswith(".bin")),
                   key=os.path.getmtime, reverse=True)
    for f in files[keep:]:
        try:
            os.remove(f)
        except OSError:
            pass


def prune_mp4():
    files = [(os.path.join(CACHE, f), os.path.getmtime(os.path.join(CACHE, f)),
              os.path.getsize(os.path.join(CACHE, f))) for f in os.listdir(CACHE) if f.endswith(".mp4")]
    now = time.time(); ret = SETTINGS.get("cache_retention_days", 30) * 86400
    for p, m, s in files:
        if now - m > ret:
            try:
                os.remove(p)
            except OSError:
                pass
    files = [(p, m, s) for p, m, s in files if os.path.exists(p)]
    total = sum(s for _, _, s in files); lim = SETTINGS.get("cache_max_gb", 20) * (1 << 30)
    for p, m, s in sorted(files, key=lambda x: x[1]):
        if total <= lim:
            break
        try:
            os.remove(p); total -= s
        except OSError:
            pass


# ---------- API ----------
app = FastAPI()


@app.get("/api/channels")
def channels():
    """Visible channels: explicit selection if set, else all channels that have
    at least one mapped file (empty slots stay hidden until chosen/traced)."""
    allc = list(CHANNEL_MAP.keys())
    vis = SETTINGS.get("visible_channels") or []
    if vis:
        return [c for c in allc if c in vis]
    return [c for c in allc if CHANNEL_MAP.get(c)]


@app.get("/api/status")
def status():
    return {"status": STATE["status"], "built": STATE["built"],
            "index": STATE.get("index"),
            "map": {k: len(v) for k, v in CHANNEL_MAP.items()},
            "stale": STATE.get("stale", [])}


@app.get("/api/alarm_entities")
def alarm_entities():
    """All alarm_control_panel.* entities from HA (for the settings dropdown), so the
    arm-state source isn't hardcoded — the user picks whichever panel they use."""
    if not SUP_TOKEN:
        return []
    states = ha_api("/core/api/states") or []
    doms = ("alarm_control_panel.", "input_boolean.", "binary_sensor.",
            "switch.", "input_select.")
    out = []
    for s in states:
        eid = s.get("entity_id", "")
        if eid.startswith(doms):
            out.append({"id": eid,
                        "name": (s.get("attributes") or {}).get("friendly_name") or eid,
                        "state": s.get("state")})
    out.sort(key=lambda x: x["id"])
    return out


@app.get("/api/settings")
def get_settings():
    return {"hub_ip": SETTINGS["hub_ip"], "self_ip": SETTINGS["self_ip"],
            "cache_max_gb": SETTINGS["cache_max_gb"],
            "cache_retention_days": SETTINGS["cache_retention_days"],
            "refresh_sec": SETTINGS.get("refresh_sec", 90),
            "alarm_entity": SETTINGS.get("alarm_entity", ""),
            "alarm_disarmed_state": SETTINGS.get("alarm_disarmed_state", "disarmed"),
            "alarm_poll_sec": SETTINGS.get("alarm_poll_sec", 900),
            "alarm_available": bool(SUP_TOKEN),
            "visible_channels": SETTINGS.get("visible_channels") or [],
            "all_channels": list(CHANNEL_MAP.keys()),
            "mapped_channels": [c for c in CHANNEL_MAP if CHANNEL_MAP[c]],
            "map_counts": {k: len(v) for k, v in CHANNEL_MAP.items()}}


@app.post("/api/settings")
async def set_settings(request: Request):
    body = await request.json()
    changed_hub = False
    if "hub_ip" in body and body["hub_ip"]:
        changed_hub = changed_hub or body["hub_ip"] != SETTINGS["hub_ip"]
        SETTINGS["hub_ip"] = str(body["hub_ip"]).strip()
    if "self_ip" in body and body["self_ip"]:
        changed_hub = changed_hub or body["self_ip"] != SETTINGS["self_ip"]
        SETTINGS["self_ip"] = str(body["self_ip"]).strip()
    if "cache_max_gb" in body:
        try:
            SETTINGS["cache_max_gb"] = max(1, int(body["cache_max_gb"]))
        except (ValueError, TypeError):
            pass
    if "cache_retention_days" in body:
        try:
            SETTINGS["cache_retention_days"] = max(1, int(body["cache_retention_days"]))
        except (ValueError, TypeError):
            pass
    if "refresh_sec" in body:
        try:
            SETTINGS["refresh_sec"] = max(10, int(body["refresh_sec"]))
        except (ValueError, TypeError):
            pass
    if "alarm_entity" in body:
        new_ent = str(body["alarm_entity"]).strip()
        if new_ent != SETTINGS.get("alarm_entity"):
            SETTINGS["alarm_entity"] = new_ent
            STATE["armed_intervals"] = None; STATE["armed_ts"] = 0   # force re-fetch
    if "alarm_disarmed_state" in body:
        new_dis = str(body["alarm_disarmed_state"]).strip() or "disarmed"
        if new_dis != SETTINGS.get("alarm_disarmed_state"):
            SETTINGS["alarm_disarmed_state"] = new_dis
            STATE["armed_intervals"] = None; STATE["armed_ts"] = 0   # force re-fetch
    if "alarm_poll_sec" in body:
        try:
            SETTINGS["alarm_poll_sec"] = max(60, int(body["alarm_poll_sec"]))
        except (ValueError, TypeError):
            pass
    if "visible_channels" in body and isinstance(body["visible_channels"], list):
        SETTINGS["visible_channels"] = [c for c in body["visible_channels"] if c in CHANNEL_MAP]
    save_settings(SETTINGS)
    if changed_hub:
        global hub
        hub = make_hub()
    prune_mp4()
    return {"ok": True, "hub_changed": changed_hub}


@app.post("/api/refresh")
def refresh():
    ensure_coverage(force=True)
    return {"ok": True, "status": STATE["status"], "built": STATE["built"]}


@app.get("/api/years")
def years(channel: str, type: str = "all", unviewed: int = 0, armed: int = 0):
    return sorted({e["dt"].year for e in sel_events(channel, type, unviewed, armed)}, reverse=True)


@app.get("/api/months")
def months(channel: str, year: int, type: str = "all", unviewed: int = 0, armed: int = 0):
    return sorted({e["dt"].month for e in sel_events(channel, type, unviewed, armed)
                   if e["dt"].year == year}, reverse=True)


@app.get("/api/days")
def days(channel: str, year: int, month: int, type: str = "all", unviewed: int = 0, armed: int = 0):
    return sorted({e["dt"].day for e in sel_events(channel, type, unviewed, armed)
                   if e["dt"].year == year and e["dt"].month == month}, reverse=True)


@app.get("/api/events")
def events(channel: str, date: str, type: str = "all", unviewed: int = 0, armed: int = 0):
    day = dt.date.fromisoformat(date)
    vset = viewed_set(channel)
    ensure_armed()
    out = []
    for e in sel_events(channel, type, unviewed, armed):
        if e["dt"].date() != day:
            continue
        ts = e["dt"].strftime("%Y-%m-%d %H:%M:%S")
        dur = int((e["end"] - e["dt"]).total_seconds()) if e.get("end") else 0
        out.append({"ts": ts, "type": e["type"], "dur": max(0, dur),
                    "viewed": is_viewed(channel, e["dt"], vset),
                    "armed": is_armed(e["dt"])})
    return out


def ensure_clip(channel, ts):
    """Make sure the event's mp4 is in cache (extract if missing). Returns the
    path or None if it can't be produced. Shared by /api/play and /api/export."""
    tdt = dt.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    out = clip_path(channel, tdt)
    if os.path.exists(out):
        return out
    chosen = next((e for e in chan_events(channel) if e["dt"] == tdt), None)
    if not chosen:
        return None
    # allow audio always; extract_mp4 only muxes it when real sound is present
    # (silent A-law is skipped), so Панель-1 motion with audio keeps its sound.
    fi = chosen["file"]; audio = True; clus = chosen.get("clus")
    anchor = (clus << 22) if clus is not None else None
    ok = 0
    if clus is not None:
        try:
            rdata, ranchor = hub.pull_region(fi, clus)
            ok, _, _ = hub.extract_mp4(rdata, chosen["dt"], chosen.get("end"), out,
                                       allow_audio=audio, anchor_off=ranchor, label=channel)
        except Exception:
            ok = 0
    if not ok:
        ok, _, _ = hub.extract_mp4(get_file(fi), chosen["dt"], chosen.get("end"), out,
                                   allow_audio=audio, anchor_off=anchor, label=channel)
    if not ok:
        ok, _, _ = hub.extract_mp4(get_file(fi, force=True), chosen["dt"], chosen.get("end"), out,
                                   allow_audio=audio, anchor_off=anchor, label=channel)
    prune_mp4()
    return out if (ok and os.path.exists(out)) else None


def ascii_chan(name):
    return (name.replace("Панель", "Panel").replace("Камера", "Camera")
                .replace(" ", "_"))


@app.post("/api/play")
def play(channel: str, ts: str):
    # Only EXTRACT here — do NOT mark viewed. A superseded/aborted load still
    # finishes server-side (threadpool), so marking viewed here would flag clips
    # the user never actually watched. The client marks viewed (/api/viewed)
    # after the video is really shown.
    tdt = dt.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    out = clip_path(channel, tdt)
    if not os.path.exists(out):
        if not any(e["dt"] == tdt for e in chan_events(channel)):
            raise HTTPException(404, "event not found")
        if not ensure_clip(channel, ts):
            raise HTTPException(500, "extract failed")
    return {"url": "media/%s?t=%d" % (os.path.basename(out), int(os.path.getmtime(out)))}


@app.get("/api/export")
def export(channel: str, ts: str):
    """Download selected clips of a channel: one mp4 if a single ts, else a zip.
    ts is ';'-joined 'YYYY-MM-DD HH:MM:SS' values. Clips are extracted if missing
    (the UI pre-extracts with progress, so here they are usually cached)."""
    items = [t.strip() for t in ts.split(";") if t.strip()]
    paths = []
    for t in items:
        p = ensure_clip(channel, t)
        if p:
            paths.append(p)
    if not paths:
        raise HTTPException(500, "no clips produced")
    ac = ascii_chan(channel)
    if len(paths) == 1:
        tdt = dt.datetime.strptime(items[0], "%Y-%m-%d %H:%M:%S")
        fn = "%s_%s.mp4" % (ac, tdt.strftime("%Y%m%d_%H%M%S"))
        return FileResponse(paths[0], media_type="video/mp4", filename=fn)
    day = items[0][:10].replace("-", "")
    zname = "%s_%s_%dclips.zip" % (ac, day, len(paths))
    zpath = os.path.join(CACHE, "_export_%d.zip" % int(time.time() * 1000))
    with zipfile.ZipFile(zpath, "w", zipfile.ZIP_STORED) as z:
        for p in paths:
            b = os.path.basename(p).rsplit("_", 2)   # [chan, YYYYMMDD, HHMMSS.mp4]
            z.write(p, "%s_%s_%s" % (ac, b[-2], b[-1]) if len(b) == 3 else os.path.basename(p))
    return FileResponse(zpath, media_type="application/zip", filename=zname,
                        background=BackgroundTask(os.remove, zpath))


@app.post("/api/viewed")
def viewed(channel: str, ts: str, v: int = 1):
    with db() as c:
        if v:
            c.execute("INSERT OR IGNORE INTO viewed(channel,ts) VALUES(?,?)", (channel, ts))
        else:
            c.execute("DELETE FROM viewed WHERE channel=? AND ts=?", (channel, ts))
    return {"ok": True}


@app.get("/media/{name}")
def media(name: str):
    p = os.path.join(CACHE, name)
    if not os.path.exists(p):
        raise HTTPException(404)
    return FileResponse(p, media_type="video/mp4")


@app.get("/", response_class=HTMLResponse)
def index():
    return open("/app/static/index.html", encoding="utf-8").read()


app.mount("/static", StaticFiles(directory="/app/static"), name="static")
