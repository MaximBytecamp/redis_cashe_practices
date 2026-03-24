from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Literal


@dataclass
class Metrics:
    cache_hits: int = 0
    cache_misses: int = 0
    db_hits: int = 0
    null_cache_hits: int = 0
    lock_waits: int = 0
    local_cache_hits: int = 0
    latencies: list[float] = field(default_factory=list)
    ttl_values: list[int] = field(default_factory=list)


    def record_latency(self, seconds: float) -> None:
        self.latencies.append(seconds)

    def record_ttl(self, ttl: int) -> None:
        self.ttl_values.append(ttl)

    def hit(self, kind: Literal["cache", "db", "null_cache", "lock_wait", "local"]) -> None:
        match kind:
            case "cache":
                self.cache_hits += 1
            case "db":
                self.db_hits += 1
            case "null_cache":
                self.null_cache_hits += 1
            case "lock_wait":
                self.lock_waits += 1
            case "local":
                self.local_cache_hits += 1

    def reset(self) -> None:
        self.cache_hits = 0
        self.cache_misses = 0
        self.db_hits = 0
        self.null_cache_hits = 0
        self.lock_waits = 0
        self.local_cache_hits = 0
        self.latencies.clear()
        self.ttl_values.clear()


    @property
    def total_requests(self) -> int:
        return self.cache_hits + self.cache_misses

    @property
    def hit_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.cache_hits / self.total_requests * 100

    @property
    def avg_latency_ms(self) -> float:
        if not self.latencies:
            return 0.0
        return sum(self.latencies) / len(self.latencies) * 1000

    @property
    def p50_latency_ms(self) -> float:
        return self._percentile(50)

    @property
    def p95_latency_ms(self) -> float:
        return self._percentile(95)

    @property
    def p99_latency_ms(self) -> float:
        return self._percentile(99)

    def _percentile(self, pct: int) -> float:
        if not self.latencies:
            return 0.0
        s = sorted(self.latencies)
        idx = int(len(s) * pct / 100)
        idx = min(idx, len(s) - 1)
        return s[idx] * 1000

    def summary(self) -> dict:
        return {
            "total_requests": self.total_requests,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "db_hits": self.db_hits,
            "null_cache_hits": self.null_cache_hits,
            "lock_waits": self.lock_waits,
            "local_cache_hits": self.local_cache_hits,
            "hit_rate_%": round(self.hit_rate, 2),
            "avg_latency_ms": round(self.avg_latency_ms, 2),
            "p50_latency_ms": round(self.p50_latency_ms, 2),
            "p95_latency_ms": round(self.p95_latency_ms, 2),
            "p99_latency_ms": round(self.p99_latency_ms, 2),
            "ttl_min": min(self.ttl_values) if self.ttl_values else 0,
            "ttl_max": max(self.ttl_values) if self.ttl_values else 0,
            "ttl_unique": len(set(self.ttl_values)),
        }
    
metrics = Metrics()

class Timer:

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *_):
        self.elapsed = time.perf_counter() - self.start
        metrics.record_latency(self.elapsed)
