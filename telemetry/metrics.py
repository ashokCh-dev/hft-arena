"""Telemetry math: latency percentiles, TPS, correctness, composite score.

The latency histogram bucket mapping MUST match bot_fleet/fleet.py.
"""
import math
import os
import time
from collections import deque

NBUCKETS = 256
SCALE = 16.0

TARGET_TPS = float(os.environ.get("TARGET_TPS", "20000"))
LAT_REF_US = float(os.environ.get("LAT_REF_US", "500"))


def bucket_us(b: int) -> float:
    """Representative microsecond value for a histogram bucket (its midpoint)."""
    return math.expm1((b + 0.5) / SCALE)


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
        self.hist = [0] * NBUCKETS
        self.peak_tps = 0.0
        self._win = deque()        # (t, cum_sent) for sliding TPS
        self.last_update = time.time()

    def add(self, sent, acked, errors, corr_pass, corr_total, hist_delta,
            phase="open"):
        self.sent += sent
        self.acked += acked
        self.errors += errors
        self.corr_pass += corr_pass
        self.corr_total += corr_total
        # Latency percentiles come only from the open-loop phase, where arrivals
        # are paced below capacity — so they reflect true service time, not the
        # queue depth of the closed-loop throughput phase (Little's law).
        if phase == "open":
            for b, v in hist_delta:
                self.hist[b] += v
        self.last_update = time.time()

    def tps(self) -> float:
        """Current throughput over a ~2s sliding window."""
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

    def percentiles(self):
        total = sum(self.hist)
        if total == 0:
            return 0.0, 0.0, 0.0
        targets = {50: total * 0.50, 90: total * 0.90, 99: total * 0.99}
        out = {}
        cum = 0
        for b in range(NBUCKETS):
            cum += self.hist[b]
            for p, thresh in list(targets.items()):
                if cum >= thresh:
                    out[p] = bucket_us(b)
                    del targets[p]
            if not targets:
                break
        return out.get(50, 0.0), out.get(90, 0.0), out.get(99, 0.0)

    def correctness(self) -> float:
        priority = (self.corr_pass / self.corr_total) if self.corr_total else 1.0
        reliability = (self.acked / self.sent) if self.sent else 1.0
        return priority * reliability

    def snapshot(self) -> dict:
        cur_tps = self.tps()
        p50, p90, p99 = self.percentiles()
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
            "tps": round(cur_tps),
            "peak_tps": round(self.peak_tps),
            "sent": self.sent,
            "acked": self.acked,
            "errors": self.errors,
            "correctness": round(correctness, 4),
            "score": round(score * 1000, 1),
        }
