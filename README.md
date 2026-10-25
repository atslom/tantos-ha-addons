# Tantos Jolli Archive (Home Assistant add-on)

[Русский](README.ru.md)

Remote access to the SD-card recordings of **Tantos Jolli** video-intercom panels and
cameras (XiongMai/Sofia) right from Home Assistant: browse clips per channel and type
(call / motion) with real timestamps, play a clip with an on-screen overlay
(channel + date/time), download a single file or a zip, with a size/age-limited cache.
The clip list auto-refreshes (configurable) and the sidebar panel is available to all
users (not only admins).

## Install (local add-on)

1. Copy the `tantos_jolly_archive/` folder into `/addons/` on the Home Assistant host
   (via the Samba/SSH add-on), or unpack this archive there.
2. **Settings → Add-ons → Add-on Store → ⋮ → Check for updates**.
3. Under **Local add-ons** open **Tantos Jolli Archive → Install**.
4. On the **Configuration** tab set:
   - `hub_ip` — the Tantos hub IP (e.g. `192.168.0.27`);
   - `hub_user` / `hub_pass` — the hub's telnet root credentials;
   - `self_ip` — the Home Assistant IP the hub streams to (e.g. `192.168.0.32`);
   - `cache_max_gb` (default 20), `cache_retention_days` (30);
   - `refresh_sec` (default 90) — clip-list auto-refresh interval.
5. **Start**, then open the web UI (sidebar panel or **Open Web UI**).

The same options (hub IP, cache, visible channels, refresh interval, Alarmo entity)
can also be changed inside the app via the **⚙** button; they persist in
`/data/app_settings.json`.

## Channel attribution (SD-independent)

The hub itself records which `.bin` data file belongs to which channel, in the QVBK
allocator table inside the active index file (`indexNNN.bin`). The add-on reads this
**live on every refresh** — it finds the active index via the open file descriptor of
the Sofia process (it rotates daily, nothing is hardcoded), so swapping the SD card,
adding a panel/camera, or the ring buffer reusing files just works: the map is re-read
from the hub's own index, with no hardcoded file numbers and no manual tracing. The
last map is cached to `/data/qvbk_map.json` as a fallback.

## "On-guard" tagging (optional)

Clips recorded while your alarm was armed get a 🔒 badge and a dedicated filter. In ⚙
pick any entity (`alarm_control_panel`, `input_boolean`, `binary_sensor`, …) and the
state that means "disarmed"; every other state counts as armed. Arm/disarm transitions
are logged to `/data/armed_log.json` (seeded from the HA recorder, then grown by a
background poll), so on-guard status is available for past clips without being limited
by recorder retention. Requires `homeassistant_api: true` (already set in config).

## Notes

- The hub must be reachable over telnet (root); `self_ip` is the HA address on the same LAN.
- Container: `python:3.12-slim-bookworm` + `ffmpeg` (installed at build time).
- `host_network: true`, `ingress: true`, `panel_admin: false` (sidebar for all users).
- Small state (QVBK map, settings, "viewed" DB, armed log) lives in `/data` and is
  backed up (it is tiny).
- The **video cache** (mp4 clips + pulled `.bin`) lives in `/share/tantos_jolly_archive/`
  (NOT `/data`), so it does **not** bloat add-on backups; it rebuilds on demand.

> The device is named **Tantos Jolli**; the add-on slug keeps the legacy spelling
> `tantos_jolly_archive` for compatibility.
