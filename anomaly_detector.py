#!/usr/bin/env python3
"""
B-Version: Threaded orchestrator with lightweight metrics, worker structure, watchdog.

Structure:
- Single process.
- Threads:
    - IngestionWorker: accepts TCP connections, parses JSON spins, enqueues (game_id, vendor_id, bet, win)
    - AggregatorWorker: aggregates spins per-second and writes per-second entries to Redis (rpush)
    - AnomalyWorker: every minute reads last 60 per-second entries, updates EWMA buffers, detects anomalies
    - RollupWorker: runs daily rollups (week, month, quarter, year) using per-day RTP entries and total_spins
    - MonitorWorker: collects metrics, checks heartbeats, restarts workers if they die (best-effort)
- Shared objects:
    - spin_queue: queue.Queue for inter-thread communication
    - heartbeats: dict updated by workers so monitor can detect stalls
    - metrics: centralized metrics object

Per-second → per-minute → per-hour → per-day → rollups (week/month/quarter/year)
"""

from __future__ import annotations
import os
import sys
import socket
import json
import time
import redis
import logging
import signal
import threading
import queue
import pandas as pd
import collections
from typing import Dict, Tuple, Any, List, Deque, Optional
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime

# === CONFIGURATION ===
CONFIG = {
    "HOST": "localhost",
    "PORT": 6996,
    "REDIS_HOST": "anomaly-orchestrator-redis-master-master.anomaly-system.svc.cluster.local",
    "REDIS_PORT": 6379,
    "BAND_STRENGTH": 50.0,
    "MARGIN": 0.14,
    "DRIFT_AMPLIFIER": 0.03,
    "MAX_INVERTED_DRIFT": 0.003,
    "DRIFT_EWMA_SPAN": 20,
    "BASE_RTP_VALUE": 0.96,
    "DRIFT_WINDOW" : 10,
    "MAX_QUEUE_SIZE": 100_000,
    "AGG_HISTORY_SECONDS": 172800,  # ~2 days of per-second history
    "BUFFER_EXPIRY_DAYS": 90,
    "LATEST_EXPIRY_DAYS": 120,
    "DAILY_EXPIRY_DAYS": 365 * 5,  # persist daily entries for 5 years so rollups can be computed later
    "ROLLUP_EXPIRY_DAYS": 365 * 5,  # retain rollups for 5 years
    "DAILY_LIST_RETENTION": 365 * 5,
    "EWMA_SPANS": {
        "1hr": 60,
        "24hr": 24,
        "10day": 10,
        "30day": 30,
    },
    "WORKER_WATCHDOG_INTERVAL": 5,  # seconds
    "METRICS_LOG_INTERVAL": 10,     # seconds
    "INACTIVITY_ALERT_SECONDS": 300,
}

HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8083"))

# Override with environment variables if available
CONFIG["HOST"] = os.getenv("HOST", CONFIG["HOST"])  # type: ignore
CONFIG["PORT"] = int(os.getenv("PORT", CONFIG["PORT"]))  # type: ignore
CONFIG["REDIS_HOST"] = os.getenv("REDIS_HOST", CONFIG["REDIS_HOST"])  # type: ignore
CONFIG["REDIS_PORT"] = int(os.getenv("REDIS_PORT", CONFIG["REDIS_PORT"]))  # type: ignore
CONFIG["BAND_STRENGTH"] = float(os.getenv("BAND_STRENGTH", CONFIG["BAND_STRENGTH"]))  # type: ignore
CONFIG["MARGIN"] = float(os.getenv("MARGIN", CONFIG["MARGIN"]))  # type: ignore
CONFIG["DRIFT_AMPLIFIER"] = float(os.getenv("DRIFT_AMPLIFIER", CONFIG["DRIFT_AMPLIFIER"]))  # type: ignore
CONFIG["MAX_QUEUE_SIZE"] = int(os.getenv("MAX_QUEUE_SIZE", CONFIG["MAX_QUEUE_SIZE"]))  # type: ignore

# === LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("rtp_orchestrator")

# === REDIS ===
rdb = redis.Redis(host=CONFIG["REDIS_HOST"], port=CONFIG["REDIS_PORT"], decode_responses=True)

# === UTILITIES ===
def current_ts() -> int:
    return int(time.time())


def ewma(series: List[float], span: int) -> Optional[float]:
    if not series:
        return None
    alpha = 2 / (span + 1)
    val = series[0]
    for x in series[1:]:
        val = alpha * x + (1 - alpha) * val
    return val


def safe_redis_call(func, *args, retries=3, delay=0.5, **kwargs):
    """Retry wrapper for redis operations."""
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except redis.exceptions.ConnectionError as e:
            logger.warning(f"[Redis] Connection error (attempt {attempt}): {e}")
            time.sleep(delay)
        except redis.exceptions.RedisError as e:
            logger.error(f"[Redis] Redis error: {e}")
            break
    return None


# predictive bands function (unchanged)
def calculate_predictive_bands(
    series: List[float],
    current_ewma: float,
    band_strength: float,
    margin: float,
    ewma_span: int,
    drift_window: int,
    max_drift: float,
) -> Dict[str, Any]:

    result = {
        "upper_band": None,
        "lower_band": None,
        "inverted_drift": None,
        "valid": False,
    }

    if series is None or len(series) < (ewma_span + drift_window + 1):
        return result

    try:
        slice_len = ewma_span + drift_window + 5
        recent = series[-slice_len:]
        s = pd.Series(recent)
        ewma_series = s.ewm(span=ewma_span, adjust=False).mean()

        delta = ewma_series.diff()
        recent_drift = delta.rolling(window=drift_window).mean().shift(1)

        if recent_drift is not None and not recent_drift.empty:
            drift_val = recent_drift.iloc[-1]

            inverted = -drift_val
            # Clip the drift
            if inverted > max_drift:
                inverted = max_drift
            elif inverted < -max_drift:
                inverted = -max_drift

            upper = current_ewma + (band_strength * inverted) + margin
            lower = current_ewma - (band_strength * inverted) - margin

            result.update({
                "upper_band": upper,
                "lower_band": lower,
                "inverted_drift": inverted,
                "valid": True,
            })
    except Exception:
        pass

    return result


# === METRICS & HEARTBEATS ===
class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.spins_processed = 0
        self.flushes_done = 0
        self.flush_errors = 0
        self.anomalies_detected = 0
        self.queue_size = 0

    def inc(self, name: str, delta: int = 1):
        with self.lock:
            if hasattr(self, name):
                setattr(self, name, getattr(self, name) + delta)

    def set_queue_size(self, size: int):
        with self.lock:
            self.queue_size = size

    def snapshot(self) -> Dict[str, int]:
        with self.lock:
            return {
                "spins_processed": self.spins_processed,
                "flushes_done": self.flushes_done,
                "flush_errors": self.flush_errors,
                "anomalies_detected": self.anomalies_detected,
                "queue_size": self.queue_size,
            }


# shared objects
spin_queue: queue.Queue = queue.Queue(maxsize=CONFIG["MAX_QUEUE_SIZE"])  # type: ignore
heartbeats: Dict[str, int] = {}  # worker_name -> last_ts
heartbeats_lock = threading.Lock()
metrics = Metrics()
stop_event = threading.Event()


def touch_heartbeat(name: str):
    with heartbeats_lock:
        heartbeats[name] = current_ts()


# === WORKER BASE ===
class Worker(threading.Thread):
    def __init__(self, name: str):
        super().__init__(name=name, daemon=True)
        self._stop_requested = threading.Event()

    def stop(self):
        self._stop_requested.set()

    def stopped(self) -> bool:
        return self._stop_requested.is_set() or stop_event.is_set()

    def run(self):
        raise NotImplementedError


# === INGESTION WORKER ===
class IngestionWorker(Worker):
    def __init__(self, host: str, port: int, spin_queue: queue.Queue):
        super().__init__(name="IngestionWorker")
        self.host = host
        self.port = port
        self.spin_queue = spin_queue
        self.executor = ThreadPoolExecutor(max_workers=64)  # handles client connections
        self.server_sock: Optional[socket.socket] = None

    def handle_client(self, conn: socket.socket):
        buffer = ""
        try:
            conn.settimeout(10.0)
            while not self.stopped():
                try:
                    data = conn.recv(4096)
                except socket.timeout:
                    continue
                if not data:
                    break
                buffer += data.decode(errors="ignore")
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    try:
                        spin = json.loads(line)
                        game_id = spin.get("gameID")
                        vendor_id = spin.get("vendorID")
                        bet = float(spin.get("bet", 0))
                        win = float(spin.get("win", 0))
                        if bet > 0 and game_id and vendor_id:
                            try:
                                self.spin_queue.put((game_id, vendor_id, bet, win), timeout=0.5)
                            except queue.Full:
                                logger.warning("[Ingestion] Spin queue full, dropping spin")
                    except Exception as e:
                        logger.warning(f"[Ingestion] Failed to parse spin: {e}")
        except Exception as e:
            logger.error(f"[Ingestion] Client handler error: {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def run(self):
        touch_heartbeat(self.name)
        logger.info(f"[Ingestion] Starting TCP server on {self.host}:{self.port}")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            self.server_sock = s
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.host, self.port))
            s.listen(100)
            s.settimeout(1.0)
            while not self.stopped():
                try:
                    conn, _ = s.accept()
                    # hand off to executor
                    self.executor.submit(self.handle_client, conn)
                except socket.timeout:
                    # periodic heartbeat
                    touch_heartbeat(self.name)
                    continue
                except Exception as e:
                    logger.error(f"[Ingestion] Accept error: {e}")
                    time.sleep(0.5)
                    touch_heartbeat(self.name)
            logger.info("[Ingestion] Stop requested, shutting down executor")
            self.executor.shutdown(wait=False)
            logger.info("[Ingestion] Exiting")


# === AGGREGATOR WORKER ===
class AggregatorWorker(Worker):
    def __init__(self, spin_queue: queue.Queue):
        super().__init__(name="AggregatorWorker")
        self.spin_queue = spin_queue
        self.current_second = int(time.time())
        self.aggregates: Dict[Tuple[str, str], Dict[str, Any]] = collections.defaultdict(
            lambda: {"bet_sum": 0.0, "win_sum": 0.0, "count": 0}
        )

    def flush_to_redis(self, timestamp: int):
        pipe = rdb.pipeline()
        entries = 0
        try:
            for (game_id, vendor_id), agg in list(self.aggregates.items()):
                if agg["count"] == 0:
                    continue
                key = f"rtp:per_second:{game_id}:{vendor_id}"
                data = {"ts": timestamp, "bet_sum": agg["bet_sum"], "win_sum": agg["win_sum"], "count": agg["count"]}
                pipe.rpush(key, json.dumps(data))
                pipe.ltrim(key, -CONFIG["AGG_HISTORY_SECONDS"], -1)
                pipe.expire(key, CONFIG["BUFFER_EXPIRY_DAYS"] * 24 * 3600)
                entries += 1
            safe_redis_call(pipe.execute)
            metrics.inc("flushes_done", entries)
            logger.debug(f"[Aggregator] Flushed {entries} aggregate keys to Redis")
        except Exception as e:
            metrics.inc("flush_errors", 1)
            logger.error(f"[Aggregator] flush error: {e}")

    def run(self):
        touch_heartbeat(self.name)
        logger.info("[Aggregator] Started")
        while not self.stopped():
            try:
                now = int(time.time())
                # flush if second changed
                if now != self.current_second:
                    self.flush_to_redis(self.current_second)
                    self.aggregates.clear()
                    self.current_second = now
                    touch_heartbeat(self.name)

                try:
                    game_id, vendor_id, bet, win = self.spin_queue.get(timeout=0.5)
                    key = (game_id, vendor_id)
                    agg = self.aggregates[key]
                    agg["bet_sum"] += bet
                    agg["win_sum"] += win
                    agg["count"] += 1
                    metrics.inc("spins_processed", 1)
                except queue.Empty:
                    continue
                except Exception as e:
                    logger.error(f"[Aggregator] Error processing spin: {e}")
            except Exception as e:
                logger.error(f"[Aggregator] Main loop error: {e}")
                time.sleep(1)
        # final flush
        try:
            self.flush_to_redis(int(time.time()))
        except Exception:
            pass
        logger.info("[Aggregator] Exiting")


# === ANOMALY WORKER ===
class AnomalyWorker(Worker):
    def __init__(self):
        super().__init__(name="AnomalyWorker")
        self.buffers: Dict[Tuple[str, str], Dict[str, Any]] = collections.defaultdict(
            lambda: {
                "per_minute_rtp": collections.deque(maxlen=CONFIG["EWMA_SPANS"]["1hr"] + 1),
                "per_hour_rtp": collections.deque(maxlen=CONFIG["EWMA_SPANS"]["24hr"] + 1),
                "per_day_rtp": collections.deque(maxlen=CONFIG["EWMA_SPANS"]["30day"] + 1),
                "ewma_1hr": None,
                "ewma_24hr": None,
                "ewma_10day": None,
                "ewma_30day": None,
                "last_seen": 0,
                "upper_band": None,
                "lower_band": None,
            }
        )

    def load_buffers(self):
        try:
            keys = safe_redis_call(rdb.keys, "ewma_buffer:*") or []
            logger.info(f"[Anomaly] Loading {len(keys)} ewma buffers from redis")
            for key in keys:
                try:
                    _, game_id, vendor_id = key.split(":")
                    raw = safe_redis_call(rdb.get, key)
                    if not raw:
                        continue
                    obj = json.loads(raw)
                    buf = self.buffers[(game_id, vendor_id)]

                    def normalize(raw_list):
                        out = []
                        for v in raw_list:
                            if isinstance(v, dict) and "ts" in v and "value" in v:
                                out.append(v)
                            elif isinstance(v, (float, int)):
                                out.append({"ts": current_ts(), "value": v})
                        return out

                    buf["per_minute_rtp"] = collections.deque(
                        normalize(obj.get("per_minute_rtp", [])),
                        maxlen=CONFIG["EWMA_SPANS"]["1hr"] + 1
                    )
                    buf["per_hour_rtp"] = collections.deque(
                        normalize(obj.get("per_hour_rtp", [])),
                        maxlen=CONFIG["EWMA_SPANS"]["24hr"] + 1
                    )
                    buf["per_day_rtp"] = collections.deque(
                        normalize(obj.get("per_day_rtp", [])),
                        maxlen=CONFIG["EWMA_SPANS"]["30day"] + 1
                    )
                    buf["ewma_1hr"] = obj.get("ewma_1hr")
                    buf["ewma_24hr"] = obj.get("ewma_24hr")
                    buf["ewma_10day"] = obj.get("ewma_10day")
                    buf["ewma_30day"] = obj.get("ewma_30day")
                    buf["last_seen"] = obj.get("last_seen", 0)
                except Exception as e:
                    logger.warning(f"[Anomaly] Skipped corrupted buffer {key}: {e}")
        except Exception as e:
            logger.warning(f"[Anomaly] load_buffers failed: {e}")

    def persist_buffers(self):
        pipe = rdb.pipeline()
        saved = 0
        for (game_id, vendor_id), buf in self.buffers.items():
            try:
                data = {
                    "per_minute_rtp": [{"ts": e["ts"], "value": e["value"]} for e in buf["per_minute_rtp"]],
                    "per_hour_rtp": [{"ts": e["ts"], "value": e["value"]} for e in buf["per_hour_rtp"]],
                    "per_day_rtp": [{"ts": e["ts"], "value": e["value"]} for e in buf["per_day_rtp"]],
                    "ewma_1hr": buf.get("ewma_1hr"),
                    "ewma_24hr": buf.get("ewma_24hr"),
                    "ewma_10day": buf.get("ewma_10day"),
                    "ewma_30day": buf.get("ewma_30day"),
                    "last_seen": buf.get("last_seen", 0),
                }
                key = f"ewma_buffer:{game_id}:{vendor_id}"
                pipe.set(key, json.dumps(data))
                pipe.expire(key, CONFIG["BUFFER_EXPIRY_DAYS"] * 24 * 3600)
                saved += 1
            except Exception as e:
                logger.warning(f"[Anomaly] Failed to persist buffer for {game_id}:{vendor_id}: {e}")
        safe_redis_call(pipe.execute)
        logger.info(f"[Anomaly] Persisted {saved} buffers")

    def compute_rtp_per_spin_from_per_second_entries(self, entries: List[str], cutoff_ts: int) -> Tuple[float, int, float, float]:
        """
        Compute per-spin RTP from per-second aggregated entries without weighting by bet size.
        Returns (per_spin_rtp, total_spins_counted, total_bet_sum, total_win_sum).

        We count only spins/seconds where bet_sum > 0 (consistent with earlier behaviour).
        """
        total_count = 0
        weighted_rtp_sum = 0.0
        bet_sum = 0.0
        win_sum = 0.0
        for e in entries:
            try:
                obj = json.loads(e)
                ts = int(obj.get("ts", 0))
                if ts <= cutoff_ts:
                    continue
                count = int(obj.get("count", 0))
                bsum = float(obj.get("bet_sum", 0.0))
                wsum = float(obj.get("win_sum", 0.0))
                bet_sum += bsum
                win_sum += wsum
                if count > 0 and bsum > 0:
                    # avg RTP for this second
                    avg_rtp_sec = wsum / bsum
                    weighted_rtp_sum += avg_rtp_sec * count
                    total_count += count
            except Exception:
                continue

        if total_count > 0:
            return (weighted_rtp_sum / total_count, total_count, bet_sum, win_sum)
        return (0.0, total_count, bet_sum, win_sum)

    def run(self):
        touch_heartbeat(self.name)
        logger.info("[Anomaly] Started")
        self.load_buffers()

        while not self.stopped():
            try:
                now = current_ts()
                one_minute_ago = now - 60

                keys_iter = safe_redis_call(rdb.scan_iter, "rtp:per_second:*") or []
                game_vendor_pairs = set()
                for k in keys_iter:
                    parts = k.split(":")
                    if len(parts) >= 4:
                        game_vendor_pairs.add((parts[2], parts[3]))

                for game_id, vendor_id in game_vendor_pairs:
                    per_sec_key = f"rtp:per_second:{game_id}:{vendor_id}"
                    entries = safe_redis_call(rdb.lrange, per_sec_key, -CONFIG["AGG_HISTORY_SECONDS"], -1) or []
                    if not entries:
                        continue

                    # compute minute-level per-spin RTP (only from the last minute)
                    rtp, minute_count, minute_bet, minute_win = self.compute_rtp_per_spin_from_per_second_entries(entries, cutoff_ts=one_minute_ago)

                    buf = self.buffers[(game_id, vendor_id)]
                    buf["last_seen"] = now

                    # Cold start padding
                    if len(buf["per_minute_rtp"]) < CONFIG["EWMA_SPANS"]["1hr"]:
                        fill = CONFIG["EWMA_SPANS"]["1hr"] - len(buf["per_minute_rtp"])
                        for _ in range(fill):
                            buf["per_minute_rtp"].append({"ts": now, "value": 0.95})

                    buf["per_minute_rtp"].append({"ts": now, "value": rtp})
                    minute_series = [x["value"] for x in buf["per_minute_rtp"]]

                    # Update 1hr EWMA
                    if len(minute_series) >= CONFIG["EWMA_SPANS"]["1hr"]:
                        buf["ewma_1hr"] = ewma(minute_series[-CONFIG["EWMA_SPANS"]["1hr"]:], CONFIG["EWMA_SPANS"]["1hr"])  # type: ignore

                    # Hourly and daily processing
                    if time.localtime(now).tm_min == 0 and time.localtime(now).tm_sec < 10:
                        # Hourly
                        if len(minute_series) >= 60:
                            hourly_rtp = sum(minute_series[-60:]) / 60.0
                            buf["per_hour_rtp"].append({"ts": now, "value": hourly_rtp})
                            while len(buf["per_hour_rtp"]) > CONFIG["EWMA_SPANS"]["24hr"]:
                                buf["per_hour_rtp"].popleft()
                            hour_series = [x["value"] for x in buf["per_hour_rtp"]]
                            if len(hour_series) >= CONFIG["EWMA_SPANS"]["24hr"]:
                                buf["ewma_24hr"] = ewma(hour_series[-CONFIG["EWMA_SPANS"]["24hr"]:], CONFIG["EWMA_SPANS"]["24hr"])  # type: ignore

                        # Daily (run near midnight hour change)
                        if time.localtime(now).tm_hour == 0:
                            # compute daily per-spin RTP by scanning per-second entries for the last 24h
                            day_cutoff = now - (24 * 3600)
                            daily_rtp, daily_spins, daily_bet_sum, daily_win_sum = self.compute_rtp_per_spin_from_per_second_entries(entries, cutoff_ts=day_cutoff)

                            # push daily entry (includes total_spins so later rollups can use weighted averages)
                            daily_key = f"rtp:daily:{game_id}:{vendor_id}"
                            daily_data = {"ts": now, "daily_rtp": daily_rtp, "total_spins": daily_spins}
                            safe_redis_call(rdb.rpush, daily_key, json.dumps(daily_data))
                            safe_redis_call(rdb.ltrim, daily_key, -CONFIG["DAILY_LIST_RETENTION"], -1)
                            safe_redis_call(rdb.expire, daily_key, CONFIG["DAILY_EXPIRY_DAYS"] * 24 * 3600)

                            # derive daily EWMA / day_series processing
                            buf["per_day_rtp"].append({"ts": now, "value": daily_rtp})
                            while len(buf["per_day_rtp"]) > CONFIG["EWMA_SPANS"]["30day"]:
                                buf["per_day_rtp"].popleft()
                            day_series = [x["value"] for x in buf["per_day_rtp"]]
                            if len(day_series) >= CONFIG["EWMA_SPANS"]["10day"]:
                                buf["ewma_10day"] = ewma(day_series[-CONFIG["EWMA_SPANS"]["10day"]:], CONFIG["EWMA_SPANS"]["10day"])  # type: ignore
                            if len(day_series) >= CONFIG["EWMA_SPANS"]["30day"]:
                                buf["ewma_30day"] = ewma(day_series[-CONFIG["EWMA_SPANS"]["30day"]:], CONFIG["EWMA_SPANS"]["30day"])  # type: ignore

                    # Band calculation and anomaly detection
                    ewma_base, drift_span, series_for_drift = None, 0, []
                    if buf.get("ewma_1hr") is not None:
                        ewma_base = buf["ewma_1hr"]
                        drift_span = CONFIG["EWMA_SPANS"]["1hr"]
                        series_for_drift = minute_series
                    elif buf.get("ewma_24hr") is not None:
                        ewma_base = buf["ewma_24hr"]
                        drift_span = CONFIG["EWMA_SPANS"]["24hr"]
                        series_for_drift = [x["value"] for x in buf["per_hour_rtp"]]
                    elif buf.get("ewma_10day") is not None:
                        ewma_base = buf["ewma_10day"]
                        drift_span = CONFIG["EWMA_SPANS"]["10day"]
                        series_for_drift = [x["value"] for x in buf["per_day_rtp"]]

                    upper_band = None
                    lower_band = None
                    inverted_drift = None

                    if ewma_base is not None and series_for_drift:
                        bands = calculate_predictive_bands(
                            series=series_for_drift,
                            current_ewma=ewma_base,
                            band_strength=CONFIG.get("BAND_STRENGTH", 1.5),
                            margin=CONFIG.get("MARGIN", 0.05),
                            ewma_span=CONFIG.get("DRIFT_EWMA_SPAN", CONFIG["EWMA_SPANS"].get("1hr", 10)),
                            drift_window=CONFIG.get("DRIFT_WINDOW", 3),
                            max_drift=CONFIG.get("MAX_INVERTED_DRIFT", 0.01),
                        )
                        if bands.get("valid"):
                            upper_band = bands["upper_band"]
                            lower_band = bands["lower_band"]
                            inverted_drift = bands["inverted_drift"]

                    buf["upper_band"] = upper_band
                    buf["lower_band"] = lower_band

                    anomaly = False
                    anomaly_direction = None
                    anomaly_severity = 0.0
                    anomaly_ts = now  # default to now
                    current_ewma = buf.get("ewma_1hr")
                    anomaly_rtp = None

                    if upper_band is not None and current_ewma is not None:
                        if current_ewma > upper_band:
                            anomaly = True
                            anomaly_direction = "up"
                            try:
                                anomaly_severity = round((current_ewma - upper_band) / ewma_base, 4)
                            except Exception:
                                anomaly_severity = 0.0
                                
                            #Align anomaly_ts with the actual spike in the immediate past N minutes
                            LOOKBACK_MINUTES = LOOKBACK_MINUTES = min(5, CONFIG.get("DRIFT_WINDOW", 3))
                            recent_minutes = list(buf["per_minute_rtp"])[-LOOKBACK_MINUTES:]

                            # Find the minute with the maximum value above upper_band
                            diffs = [(x["value"] - upper_band, x) for x in recent_minutes if x["value"] > upper_band]

                            if diffs:
                                max_spike = max(diffs, key=lambda d: d[0])[1]
                                anomaly_rtp = max_spike["value"]
                                anomaly_ts = max_spike["ts"]


                    state = {
                        "anomaly_ts":anomaly_ts,
                        "anomaly_rtp": anomaly_rtp,
                        "ts": now,
                        "game_id": game_id,
                        "vendor_id": vendor_id,
                        "rtp": rtp,
                        "ewma_1hr": buf.get("ewma_1hr"),
                        "ewma_24hr": buf.get("ewma_24hr"),
                        "ewma_10day": buf.get("ewma_10day"),
                        "ewma_30day": buf.get("ewma_30day"),
                        "upper_band": upper_band,
                        "lower_band": lower_band,
                        "anomaly": int(anomaly),
                        "anomaly_direction": anomaly_direction,
                        "anomaly_severity": anomaly_severity,
                    }

                    latest_key = f"rtp:latest:{game_id}:{vendor_id}"
                    history_key = f"rtp:history:{game_id}:{vendor_id}"
                    safe_redis_call(rdb.set, latest_key, json.dumps(state), ex=CONFIG["LATEST_EXPIRY_DAYS"] * 24 * 3600)
                    safe_redis_call(rdb.rpush, history_key, json.dumps(state))
                    safe_redis_call(rdb.ltrim, history_key, -10000, -1)
                    safe_redis_call(rdb.expire, history_key, CONFIG["BUFFER_EXPIRY_DAYS"] * 24 * 3600)

                    if anomaly:
                        metrics.inc("anomalies_detected", 1)
                        logger.warning(f"[ALERT] {game_id}-{vendor_id} Anomaly_RTP={anomaly_rtp:.4f} exceeded upper_band={upper_band:.4f} (severity {anomaly_severity:.4f})")
                        anomaly_key = f"anomalies:{game_id}:{vendor_id}"
                        safe_redis_call(rdb.xadd, anomaly_key, {"data": json.dumps(state)}, maxlen=1000, approximate=True)
                        safe_redis_call(rdb.expire, anomaly_key, CONFIG["BUFFER_EXPIRY_DAYS"] * 24 * 3600)

                # Inactivity detection
                inactive_cutoff = now - CONFIG["INACTIVITY_ALERT_SECONDS"]
                for (g, v), b in list(self.buffers.items()):
                    if b.get("last_seen", 0) < inactive_cutoff:
                        logger.warning(f"[Anomaly] No data for {g}-{v} in last {CONFIG['INACTIVITY_ALERT_SECONDS']}s")

                # Periodic persistence
                if now % 300 < 2:
                    self.persist_buffers()

                # Heartbeat
                touch_heartbeat(self.name)

            except Exception as e:
                logger.exception(f"[Anomaly] Unhandled error: {e}")

            # Sleep in short intervals with heartbeat updates
            sleep_time = 60 - (time.time() % 60)
            sleep_interval = 0.5
            slept = 0.0
            while slept < sleep_time:
                if self.stopped():
                    break
                touch_heartbeat(self.name)
                time.sleep(sleep_interval)
                slept += sleep_interval

        # Final persist
        try:
            self.persist_buffers()
        except Exception:
            pass
        logger.info("[Anomaly] Exiting")


# === ROLLUP WORKER ===
class RollupWorker(Worker):
    """Runs once per day (near midnight) and computes week/month/quarter/year rollups
    using per-day entries saved under rtp:daily:<game_id>:<vendor_id>.

    Each rollup key is stored as JSON with fields: avg_rtp, total_spins, last_updated
    Keys:
      - rtp:weekly:<game_id>:<vendor_id>
      - rtp:monthly:<game_id>:<vendor_id>
      - rtp:quarterly:<game_id>:<vendor_id>
      - rtp:yearly:<game_id>:<vendor_id>

    TTL: CONFIG["ROLLUP_EXPIRY_DAYS"] days (5 years by default)
    """
    def __init__(self):
        super().__init__(name="RollupWorker")

    def compute_rollup_from_daily_list(self, daily_list: List[str], days: int) -> Tuple[Optional[float], int]:
        cutoff = current_ts() - (days * 24 * 3600)
        total_weighted = 0.0
        total_spins = 0
        for e in daily_list:
            try:
                obj = json.loads(e)
                ts = int(obj.get("ts", 0))
                if ts < cutoff:
                    continue
                drtp = obj.get("daily_rtp")
                dspins = int(obj.get("total_spins", 0))
                if drtp is None or dspins <= 0:
                    continue
                total_weighted += float(drtp) * dspins
                total_spins += dspins
            except Exception:
                continue
        if total_spins > 0:
            return (total_weighted / total_spins, total_spins)
        return (None, 0)

    def run(self):
        logger.info("[Rollup] Started")
        last_run_day = None  # track last rollup day to avoid double-run in the 10-min window

        while not self.stopped():
            now = current_ts()
            tm = time.localtime(now)

            # Run once per day near midnight, avoid multiple runs
            if tm.tm_hour == 0 and tm.tm_min < 10 and last_run_day != tm.tm_yday:
                try:
                    last_run_day = tm.tm_yday
                    daily_keys = safe_redis_call(rdb.keys, "rtp:daily:*") or []

                    for dk in daily_keys:
                        parts = dk.split(":")
                        if len(parts) < 4:
                            continue
                        game_id, vendor_id = parts[2], parts[3]
                        daily_list = safe_redis_call(rdb.lrange, dk, -CONFIG["DAILY_LIST_RETENTION"], -1) or []

                        # Weekly rollup
                        w_avg, w_spins = self.compute_rollup_from_daily_list(daily_list, 7)
                        if w_avg is not None and w_spins > 0:
                            key = f"rtp:weekly:{game_id}:{vendor_id}"
                            safe_redis_call(rdb.set, key,
                                            json.dumps({"avg_rtp": w_avg, "total_spins": w_spins, "last_updated": now}),
                                            ex=CONFIG["ROLLUP_EXPIRY_DAYS"] * 24 * 3600)

                        # Monthly rollup
                        m_avg, m_spins = self.compute_rollup_from_daily_list(daily_list, 30)
                        if m_avg is not None and m_spins > 0:
                            key = f"rtp:monthly:{game_id}:{vendor_id}"
                            safe_redis_call(rdb.set, key,
                                            json.dumps({"avg_rtp": m_avg, "total_spins": m_spins, "last_updated": now}),
                                            ex=CONFIG["ROLLUP_EXPIRY_DAYS"] * 24 * 3600)

                        # Quarterly rollup
                        q_avg, q_spins = self.compute_rollup_from_daily_list(daily_list, 90)
                        if q_avg is not None and q_spins > 0:
                            key = f"rtp:quarterly:{game_id}:{vendor_id}"
                            safe_redis_call(rdb.set, key,
                                            json.dumps({"avg_rtp": q_avg, "total_spins": q_spins, "last_updated": now}),
                                            ex=CONFIG["ROLLUP_EXPIRY_DAYS"] * 24 * 3600)

                        # Yearly rollup
                        y_avg, y_spins = self.compute_rollup_from_daily_list(daily_list, 365)
                        if y_avg is not None and y_spins > 0:
                            key = f"rtp:yearly:{game_id}:{vendor_id}"
                            safe_redis_call(rdb.set, key,
                                            json.dumps({"avg_rtp": y_avg, "total_spins": y_spins, "last_updated": now}),
                                            ex=CONFIG["ROLLUP_EXPIRY_DAYS"] * 24 * 3600)

                    # Mark last run timestamp for monitoring
                    safe_redis_call(rdb.set, "rollup:last_run",
                                    json.dumps({"ts": now}),
                                    ex=CONFIG["ROLLUP_EXPIRY_DAYS"] * 24 * 3600)

                    logger.info(f"[Rollup] Completed daily rollup at {time.strftime('%Y-%m-%d %H:%M:%S', tm)}")

                except Exception as e:
                    logger.exception(f"[Rollup] Unhandled error during rollup: {e}")

            # Touch heartbeat every second
            touch_heartbeat(self.name)
            time.sleep(1)

        logger.info("[Rollup] Exiting")


# === MONITOR WORKER ===
# === MONITOR WORKER ===
class MonitorWorker(Worker):
    def __init__(self, workers: Dict[str, Worker]):
        super().__init__(name="MonitorWorker")
        self.workers = workers

    def run(self):
        logger.info("[Monitor] Started")
        last_metrics_log = 0

        while not self.stopped():
            try:
                now = current_ts()
                # Heartbeats check
                with heartbeats_lock:
                    for wname, last in list(heartbeats.items()):
                        age = now - last
                        last_seen_str = f"{age} seconds ago" if age >= 0 else "unknown"

                        if age > CONFIG["WORKER_WATCHDOG_INTERVAL"] * 6:
                            logger.error(f"[Monitor] Worker '{wname}' heartbeat stale (last seen {last_seen_str}), attempting restart")
                            # best-effort restart (only for threads we created)
                            if wname in self.workers:
                                w = self.workers[wname]
                                if not w.is_alive():
                                    try:
                                        logger.info(f"[Monitor] Restarting worker {wname}")
                                        new_w = type(w)(*getattr(w, "__init_args__", ()))  # type: ignore
                                        self.workers[wname] = new_w
                                        new_w.start()
                                    except Exception as e:
                                        logger.error(f"[Monitor] Failed to restart {wname}: {e}")
                        else:
                            logger.debug(f"[Monitor] Worker '{wname}' alive, last seen {last_seen_str}")

                # Update queue size metric
                try:
                    metrics.set_queue_size(spin_queue.qsize())
                except Exception:
                    pass

                # Periodic metric logging
                if now - last_metrics_log >= CONFIG["METRICS_LOG_INTERVAL"]:
                    snap = metrics.snapshot()
                    logger.info(
                        f"[Metrics] spins_processed={snap['spins_processed']}, flushes={snap['flushes_done']}, "
                        f"flush_errors={snap['flush_errors']}, anomalies={snap['anomalies_detected']}, "
                        f"queue_size={snap['queue_size']}"
                    )
                    last_metrics_log = now

                # Touch own heartbeat
                touch_heartbeat(self.name)

            except Exception as e:
                logger.exception(f"[Monitor] error: {e}")

            # Sleep a bit
            for _ in range(CONFIG["WORKER_WATCHDOG_INTERVAL"]):
                if self.stopped():
                    break
                time.sleep(1)

        logger.info("[Monitor] Exiting")



# === MAIN ORCHESTRATION ===
def build_workers() -> Dict[str, Worker]:
    ingestion = IngestionWorker(CONFIG["HOST"], CONFIG["PORT"], spin_queue)
    aggregator = AggregatorWorker(spin_queue)
    anomaly = AnomalyWorker()
    rollup = RollupWorker()

    # attach simple __init_args__ used by monitor for restarting attempt
    ingestion.__init_args__ = (CONFIG["HOST"], CONFIG["PORT"], spin_queue)
    aggregator.__init_args__ = (spin_queue,)
    anomaly.__init_args__ = ()
    rollup.__init_args__ = ()

    workers = {
        ingestion.name: ingestion,
        aggregator.name: aggregator,
        anomaly.name: anomaly,
        rollup.name: rollup,
    }
    return workers


def start_all(workers: Dict[str, Worker]) -> MonitorWorker:
    for w in workers.values():
        logger.info(f"[Main] Starting {w.name}")
        w.start()
    monitor = MonitorWorker(workers)
    monitor.start()
    return monitor


def stop_all(workers: Dict[str, Worker], monitor: MonitorWorker):
    logger.info("[Main] Stopping all workers")
    stop_event.set()
    for w in workers.values():
        try:
            w.stop()
        except Exception:
            pass
    try:
        monitor.stop()
    except Exception:
        pass
    # give threads a moment to exit
    time.sleep(1)


class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            now = time.time()
            with heartbeats_lock:
                status = {w: now - ts for w, ts in heartbeats.items()}
            resp = {"alive": True, "workers": status}
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(resp).encode())
        else:
            self.send_response(404)
            self.end_headers()


def start_health_server():
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), SimpleHandler)
    t = threading.Thread(target=server.serve_forever, name="health-server", daemon=True)
    t.start()
    return server


def main():
    logger.info("[Main] RTP Orchestrator (threaded) starting")
    workers = build_workers()
    monitor = start_all(workers)

    # start HTTP health endpoint
    start_health_server()

    # signal handlers
    def _shutdown(signum, frame):
        logger.info(f"[Main] Received signal {signum}, shutting down...")
        stop_all(workers, monitor)
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        logger.info("[Main] KeyboardInterrupt received, shutting down")
        stop_all(workers, monitor)

    # join threads gracefully
    for w in workers.values():
        try:
            w.join(timeout=2)
        except Exception:
            pass
    try:
        monitor.join(timeout=2)
    except Exception:
        pass

    logger.info("[Main] Shutdown complete")
    sys.exit(0)


if __name__ == "__main__":
    main()
