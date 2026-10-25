#!/usr/bin/env python3
"""Tantos Jolly hub backend.

Frame & CLUS timestamps are XiongMai PACKED datetime (real local time), decoded
by pk(). The CLUS header of each data file (at 0x40000) lists the file's event
segments: packed start/end + a TYPE field f10 (@+0x0a): 16 = CALL, 2 = MOTION
(this is the hub's own classification — NOT audio, which is unreliable). So the
event LIST (date + type) is read straight from the small CLUS region; only when a
clip is played do we pull the 256MB data file and cut the event as a CONTIGUOUS
byte run (avoids the fragmentation artefacts of time-window extraction)."""
import datetime as dt
import os
import re
import socket
import struct
import subprocess
import threading
import time

NFILES = 951
CLUS_OFF = 0x40000
OSD_FONT = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"  # has Cyrillic


def pk(v):
    y = 2000 + ((v >> 26) & 0x3f); mo = (v >> 22) & 0xf; d = (v >> 17) & 0x1f
    h = (v >> 12) & 0x1f; mi = (v >> 6) & 0x3f; s = v & 0x3f
    if 2024 <= y <= 2035 and 1 <= mo <= 12 and 1 <= d <= 31 and h <= 23 and mi <= 59 and s <= 59:
        try:
            return dt.datetime(y, mo, d, h, mi, s)
        except ValueError:
            return None
    return None


def etype(f10):
    return "call" if f10 == 16 else "motion"


class Hub:
    def __init__(self, ip, user, pw, self_ip, port=54400):
        self.ip = ip; self.user = user; self.pw = pw
        self.self_ip = self_ip; self.port = port
        self._lock = threading.Lock()

    def _rd(self, s, t, tok=b"#"):
        s.settimeout(t); b = b""; end = time.time() + t
        while time.time() < end:
            try:
                c = s.recv(65536)
            except socket.timeout:
                break
            if not c:
                break
            b += bytes(x for x in c if x != 255)
            if b.rstrip().endswith(tok):
                break
        return b

    def _login(self):
        s = socket.socket(); s.settimeout(10); s.connect((self.ip, 23))
        self._rd(s, 4); s.sendall(self.user.encode() + b"\r\n")
        self._rd(s, 3); s.sendall(self.pw.encode() + b"\r\n"); self._rd(s, 4)
        return s

    def _pull(self, dd_cmd, want=None, acc_timeout=300):
        with self._lock:
            srv = socket.socket(); srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind(("0.0.0.0", self.port)); srv.listen(1); srv.settimeout(acc_timeout)

            def run():
                try:
                    s = self._login()
                    s.sendall(("%s | nc %s %d" % (dd_cmd, self.self_ip, self.port)).encode() + b"\r\n")
                    self._rd(s, acc_timeout, tok=b"~ #"); s.close()
                except Exception:
                    pass
            threading.Thread(target=run, daemon=True).start()
            conn, _ = srv.accept(); conn.settimeout(120); buf = bytearray()
            while True:
                try:
                    c = conn.recv(1 << 20)
                except socket.timeout:
                    break
                if not c:
                    break
                buf += c
                if want and len(buf) >= want:
                    break
            conn.close(); srv.close()
            return bytes(buf)

    def pull_file(self, fileno):
        return self._pull("dd if=/var/fat32_1210/%06d.bin bs=1M 2>/dev/null" % fileno)

    def pull_region(self, fileno, clus, pre=1, nclus=4):
        """Pull ONLY the 4MB clusters around an event: [clus-pre .. clus-pre+nclus).
        ~16MB vs 256MB -> seconds instead of ~2min. Returns (data, anchor_off)
        where anchor_off = byte offset of the EVENT's own cluster inside the
        buffer (= (clus-c0)<<22); pass it to extract_mp4 so the start keyframe is
        picked at the cluster boundary, not a stray same-second frame in the
        pre-roll cluster (which can hold unrelated ring-leftover data)."""
        c0 = max(0, clus - pre)
        skip = c0 * 4            # 4 blocks of 1M per 4MB cluster
        count = nclus * 4
        data = self._pull("dd if=/var/fat32_1210/%06d.bin bs=1M skip=%d count=%d 2>/dev/null"
                          % (fileno, skip, count), want=count * (1 << 20), acc_timeout=120)
        return data, (clus - c0) << 22

    # ---- CLUS: per-file event segments (date + type), cheap ----
    @staticmethod
    def parse_clus(seg):
        """From a file's CLUS region bytes -> list of {dt, end, type, f10, clus} sorted.
        `clus` (seq @+4, low 16 bits) is the event's 4MB cluster index: the event's
        data lives at byte offset clus<<22, so a clip can be cut from just that
        region instead of the whole 256MB file (and it disambiguates duplicate
        wall-clock seconds left in the ring from earlier days)."""
        out = []
        for m in re.finditer(b"CLUS", seg):
            o = m.start(); e = seg[o:o + 64]
            if len(e) < 32:
                continue
            d = pk(struct.unpack_from("<I", e, 20)[0])
            if d is None:
                continue
            f10 = struct.unpack_from("<H", e, 10)[0]
            end = pk(struct.unpack_from("<I", e, 24)[0])
            clus = struct.unpack_from("<I", e, 4)[0] & 0xffff
            out.append({"dt": d, "end": end, "f10": f10, "type": etype(f10), "clus": clus})
        out.sort(key=lambda x: x["dt"])
        return out

    def pull_clus_region(self, nfiles=NFILES, len_kb=16):
        """16KB @0x40000 of every file -> {fi: {'min','max','n','events':[...]}}."""
        cmd = ("for i in $(seq 0 %d); do f=$(printf /var/fat32_1210/%%06d.bin $i); "
               "dd if=$f bs=1024 skip=256 count=%d 2>/dev/null; done"
               % (nfiles - 1, len_kb))
        blob = self._pull(cmd, want=nfiles * len_kb * 1024, acc_timeout=400)
        per = len_kb * 1024; cov = {}
        for fi in range(nfiles):
            ev = self.parse_clus(blob[fi * per:(fi + 1) * per])
            if ev:
                ds = [e["dt"] for e in ev]
                cov[fi] = {"min": min(ds), "max": max(ds), "n": len(ev), "events": ev}
        return cov

    def pull_clus_files(self, filenos, len_kb=16):
        """16KB @0x40000 of ONLY the given files -> {fi: {'min','max','n','events'}}.
        With QVBK telling us exactly which files are allocated, we scan just those
        (tens) instead of all 951 — far faster and gentler on the hub."""
        filenos = list(filenos)
        if not filenos:
            return {}
        lst = " ".join(str(f) for f in filenos)
        cmd = ("for i in %s; do f=$(printf /var/fat32_1210/%%06d.bin $i); "
               "dd if=$f bs=1024 skip=256 count=%d 2>/dev/null; done" % (lst, len_kb))
        blob = self._pull(cmd, want=len(filenos) * len_kb * 1024, acc_timeout=300)
        per = len_kb * 1024; cov = {}
        for idx, fi in enumerate(filenos):
            ev = self.parse_clus(blob[idx * per:(idx + 1) * per])
            if ev:
                ds = [e["dt"] for e in ev]
                cov[fi] = {"min": min(ds), "max": max(ds), "n": len(ev), "events": ev}
        return cov

    # ---- QVFS index: authoritative file->channel map (SD-independent) ----
    def shell(self, cmd, t=10):
        """Run one command over telnet, return its stdout text."""
        with self._lock:
            s = self._login()
            s.sendall(b"export PS1='# '\r\n"); self._rd(s, 2)
            s.sendall(cmd.encode() + b"\r\n")
            out = self._rd(s, t, tok=b"#")
            try:
                s.sendall(b"exit\r\n"); s.close()
            except OSError:
                pass
        return out.decode("latin1", "replace")

    def active_index(self):
        """The indexNNN.bin the hub is CURRENTLY using = the one Sofia holds open
        (never hardcoded — it rotates daily and differs after an SD swap). Primary:
        Sofia's open fd. Fallback: newest-mtime index file."""
        try:
            txt = self.shell("ls -l /proc/`pidof Sofia`/fd 2>/dev/null")
            m = re.findall(r"(index\d+)\.bin", txt)
            if m:
                return m[0]
        except Exception:
            pass
        try:
            txt = self.shell("ls -1t /var/fat32_1210/index*.bin 2>/dev/null")
            m = re.search(r"(index\d+)\.bin", txt)
            if m:
                return m.group(1)
        except Exception:
            pass
        return None

    @staticmethod
    def parse_qvbk(data):
        """QVBK = per-data-file allocator entries (one per .bin, contiguous @0x1000).
        For every ALLOCATED file (+0x06==1): channel ordinal (+0x24), clusters used
        (+0x1e), unix time span (+0x10/+0x14). Returns {fileno: {ch,clus_used,t0,t1}}.
        This is the hub's OWN file->channel map — SD-independent, rebuilt by the hub
        for whatever card is in it; nothing here is hardcoded to a channel count."""
        out = {}; off = 0x1000; n = len(data)
        while off + 64 <= n and off < 0x400000 and data[off:off + 4] == b"QVBK":
            fileno = struct.unpack_from("<H", data, off + 4)[0]
            alloc = struct.unpack_from("<H", data, off + 6)[0]
            if alloc == 1 and fileno < NFILES:
                out[fileno] = {"ch": struct.unpack_from("<H", data, off + 0x24)[0],
                               "clus_used": struct.unpack_from("<H", data, off + 0x1e)[0],
                               "t0": struct.unpack_from("<I", data, off + 0x10)[0],
                               "t1": struct.unpack_from("<I", data, off + 0x14)[0]}
            off += 64
        return out

    def channel_files(self):
        """(active_index_name, {fileno: {ch,...}}) read live from the hub's index."""
        idx = self.active_index()
        if not idx:
            return None, {}
        data = self._pull("dd if=/var/fat32_1210/%s.bin bs=1M 2>/dev/null" % idx,
                          want=8 << 20, acc_timeout=180)
        if len(data) < 0x1000 + 64:
            return idx, {}
        return idx, self.parse_qvbk(data)

    # ---- extraction of one event (bounded by its CLUS start..end) -> mp4 ----
    @staticmethod
    def _osd_vf(start_dt, label=""):
        """-vf: scale to 1680x1080 + hub-style OSD overlay (channel top-left,
        date-time top-right) via drawtext. Text values are single-quoted (handles
        the space in 'Панель 3'); colons in the time escaped as \\: ."""
        tt = start_dt.strftime("%d-%m-%Y %H:%M:%S").replace(":", "\\:")
        common = "fontsize=40:fontcolor=white:box=1:boxcolor=black@0.45:boxborderw=8:y=14"
        vf = "scale=1680:1080,setsar=1"
        if label:
            vf += ",drawtext=fontfile=%s:text='%s':x=18:%s" % (OSD_FONT, label.replace("'", ""), common)
        vf += ",drawtext=fontfile=%s:text='%s':x=w-tw-18:%s" % (OSD_FONT, tt, common)
        return vf

    def extract_mp4(self, data, start_dt, end_dt, out_mp4, allow_audio=False, anchor_off=None, label=""):
        """Recording is continuous (I-frame ~every 1s); an event is the [start..end]
        slice from CLUS. Pick the start SPS I-frame among those whose packed time is
        in [start-1,start+1]; when anchor_off (the event cluster's byte offset in the
        buffer) is given, choose the candidate CLOSEST to that boundary — this avoids
        latching onto a stray same-second frame sitting in the pre-roll cluster amid
        unrelated ring-leftover data (which caused tiny/failed clips for back-to-back
        events). Then take frames forward while packed time stays monotonic and
        <= end_dt (break on backward jump / big gap = fragmentation boundary)."""
        marks = [(m.start(), data[m.start() + 3], struct.unpack_from("<I", data, m.start() + 8)[0])
                 for m in re.finditer(rb"\x00\x00\x01[\xc0-\xef]", data) if m.start() + 12 <= len(data)]
        st = start_dt.timestamp(); en_t = (end_dt.timestamp() if end_dt else st + 20)
        if en_t <= st:
            en_t = st + 20
        cands = []   # (index, offset, time) of SPS I-frames near the clip start
        for i, (o, sid, v) in enumerate(marks):
            if sid != 0xe1:
                continue
            d = pk(v)
            if d is None:
                continue
            dtt = d.timestamp()
            if st - 1 <= dtt <= st + 1:
                nx = marks[i + 1][0] if i + 1 < len(marks) else len(data)
                if b"\x00\x00\x00\x01\x67" in data[o + 4:nx]:
                    cands.append((i, o, dtt))
        if not cands:
            return False, 0, 0
        if anchor_off is not None:
            start = min(cands, key=lambda c: abs(c[1] - anchor_off))[0]
        else:
            start = cands[0][0]
            for i, o, dtt in cands:
                if dtt >= st:
                    start = i; break        # prefer the first keyframe at/after start
        vid = bytearray(); alaw = bytearray(); vts = []; prev = None
        for i in range(start, len(marks)):
            o, sid, v = marks[i]
            d = pk(v)
            if d is None:
                continue
            e = d.timestamp()
            if prev is not None and (e < prev - 1 or e > prev + 3):
                break                       # non-monotonic => fragmentation/next region
            if e > en_t + 1:
                break                       # reached the event end (CLUS)
            nx = marks[i + 1][0] if i + 1 < len(marks) else len(data)
            body = data[o + 4:nx]
            if sid == 0xe3:
                if len(body) > 16:
                    alaw += body[16:]
                prev = e; continue
            if 0xe0 <= sid <= 0xef:
                q = body.find(b"\x00\x00\x00\x01")
                if q < 0:
                    continue
                vid += body[q:]; vts.append(e); prev = e
        if len(vts) < 4:
            return False, 0, 0
        span = max(1, vts[-1] - vts[0]); fps = max(1, min(25, round(len(vts) / span)))
        tmpd = out_mp4 + ".h264"; tmpa = out_mp4 + ".alaw"
        open(tmpd, "wb").write(bytes(vid))
        has_audio = allow_audio and len(set(alaw)) > 10   # audio only for CALLs
        cmd = ["ffmpeg", "-y", "-loglevel", "error", "-r", str(fps), "-f", "h264", "-i", tmpd]
        if has_audio:
            open(tmpa, "wb").write(bytes(alaw))
            cmd += ["-f", "alaw", "-ar", "8000", "-ac", "1", "-i", tmpa]
        cmd += ["-vf", self._osd_vf(start_dt, label), "-c:v", "libx264", "-preset", "veryfast",
                "-pix_fmt", "yuv420p"]
        if has_audio:
            cmd += ["-c:a", "aac", "-shortest"]
        cmd += [out_mp4]
        r = subprocess.run(cmd, capture_output=True, text=True)
        for t in (tmpd, tmpa):
            try:
                os.remove(t)
            except OSError:
                pass
        return r.returncode == 0, len(vts), fps
