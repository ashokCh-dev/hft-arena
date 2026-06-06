"""Telemetry math: latency-vs-load curve, peak TPS, correctness, composite score.

The latency histogram bucket mapping MUST match bot_fleet/fleet.py.
"""
import math
import os
import time
from collections import deque

NBUCKETS = 256
SCALE = 16.0

TARGET_TPS = float(os.environ.get("TARGET_TPS", "100000"))
LAT_REF_US = float(os.environ.get("LAT_REF_US", "2000"))
SUSTAIN = float(os.environ.get("SUSTAIN_RATIO", "0.9"))   # achieved/offered to count as sustained
# Latency is scored at a FIXED offered load so engines are compared apples-to-apples
# (the full curve shows each engine's own saturation knee separately).
REF_LOAD = float(os.environ.get("REF_LOAD", "10000"))


def bucket_us(b: int) -> float:
    """Representative microsecond value for a histogram bucket (its midpoint)."""
    return math.expm1((b + 0.5) / SCALE)


def _percentiles(hist):
    total = sum(hist)
    if total == 0:
        return 0.0, 0.0, 0.0
    targets = {50: total * 0.50, 90: total * 0.90, 99: total * 0.99}
    out, cum = {}, 0
    for b in range(NBUCKETS):
        cum += hist[b]
        for p, thresh in list(targets.items()):
            if cum >= thresh:
                out[p] = bucket_us(b)
                del targets[p]
        if not targets:
            break
    return out.get(50, 0.0), out.get(90, 0.0), out.get(99, 0.0)


class RateBin:
    """Per-offered-rate accumulator (one point on the latency-vs-load curve)."""
    def __init__(self):
        self.sent = 0
        self.acked = 0
        self.hist = [0] * NBUCKETS
        self.first_ts = None
        self.last_ts = None

    def add(self, sent, acked, hist_delta, ts):
        self.sent += sent
        self.acked += acked
        for b, v in hist_delta:
            self.hist[b] += v
        if ts:
            self.first_ts = ts if self.first_ts is None else min(self.first_ts, ts)
            self.last_ts = ts if self.last_ts is None else max(self.last_ts, ts)

    def achieved_tps(self):
        span = (self.last_ts - self.first_ts) if (self.first_ts and self.last_ts) else 0.0
        return self.acked / span if span > 0.5 else 0.0


class Agg:
    """Per-run aggregate of fleet samples."""

    def __init__(self, run_id, submission="?"):
        self.run_id = run_id
        self.submission = submission
        self.sent = 0
        self.acked = 0
        self.errors = 0
        self.corr_pass = 0
        self.corr_total = 0
        self.peak_tps = 0.0
        self.rates = {}            # offered_rate -> RateBin (open-loop sweep points)
        self._win = deque()        # (t, cum_sent) for sliding TPS in the closed phase
        self.last_update = time.time()

    def add(self, sent, acked, errors, corr_pass, corr_total, hist_delta,
            phase="open", rate=0, ts=0.0):
        self.sent += sent
        self.acked += acked
        self.errors += errors
        self.corr_pass += corr_pass
        self.corr_total += corr_total
        if phase == "open" and rate > 0:
            # Latency curve point for this offered rate. Open-loop pacing means
            # low-load points reflect true service time; high-load points reveal
            # the saturation knee.
            self.rates.setdefault(rate, RateBin()).add(sent, acked, hist_delta, ts)
        self.last_update = time.time()

    def tps(self) -> float:
        """Current throughput over a ~2s sliding window (peak captured in closed phase)."""
        now = time.time()
        self._win.append((now, self.sent))
        while self._win and now - self._win[0][0] > 2.0:
            self._win.popleft()
        if len(self._win) < 2:
            return 0.0
        t0, s0 = self._win[0]
        dt = now - t0
        cur = (self.sent - s0) / dt if dt > 0 else 0.0
        self.peak_tps = max(self.peak_tps, cur)
        return cur

    def correctness(self) -> float:
        priority = (self.corr_pass / self.corr_total) if self.corr_total else 1.0
        reliability = (self.acked / self.sent) if self.sent else 1.0
        return priority * reliability

    def curve(self):
        """Sorted latency-vs-load points: [{offered, achieved, p50, p90, p99}]."""
        pts = []
        for offered in sorted(self.rates):
            b = self.rates[offered]
            p50, p90, p99 = _percentiles(b.hist)
            pts.append({"offered": offered, "achieved": round(b.achieved_tps()),
                        "p50_us": round(p50, 1), "p90_us": round(p90, 1),
                        "p99_us": round(p99, 1)})
        return pts

    def _ref_point(self, pts):
        """Curve point nearest the fixed REF_LOAD — the apples-to-apples score load."""
        if not pts:
            return None
        return min(pts, key=lambda p: abs(p["offered"] - REF_LOAD))

    def _max_sustained(self, pts):
        """Highest offered rate the engine kept up with (achieved >= SUSTAIN*offered)."""
        best = 0
        for p in pts:
            if p["achieved"] >= SUSTAIN * p["offered"]:
                best = max(best, p["offered"])
        return best

    def snapshot(self) -> dict:
        cur_tps = self.tps()
        pts = self.curve()
        rp = self._ref_point(pts)
        p50 = rp["p50_us"] if rp else 0.0
        p90 = rp["p90_us"] if rp else 0.0
        p99 = rp["p99_us"] if rp else 0.0

        correctness = self.correctness()
        throughput_norm = min(1.0, self.peak_tps / TARGET_TPS) if TARGET_TPS else 0.0
        latency_score = LAT_REF_US / (LAT_REF_US + p99) if p99 > 0 else 0.0
        reliability = (self.acked / self.sent) if self.sent else 1.0
        crash_penalty = 0.3 if (self.sent > 0 and reliability < 0.5) else 0.0
        score = max(0.0, 0.45 * throughput_norm + 0.35 * latency_score
                    + 0.20 * correctness - crash_penalty)
        return {
            "submission": self.submission,
            "run_id": self.run_id,
            "p50_us": round(p50, 1),
            "p90_us": round(p90, 1),
            "p99_us": round(p99, 1),
            "ref_load": rp["offered"] if rp else 0,          # offered rate latency is scored at
            "max_sustained_tps": self._max_sustained(pts),   # informational: knee location
            "tps": round(cur_tps),
            "peak_tps": round(self.peak_tps),
            "sent": self.sent,
            "acked": self.acked,
            "errors": self.errors,
            "correctness": round(correctness, 4),
            "score": round(score * 1000, 1),
            "curve": pts,
        }
