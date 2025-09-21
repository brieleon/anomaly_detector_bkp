import socket
import json
import time
import random
import argparse
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

# --- Win distribution setup ---
win_multipliers = np.array([0, 0.25, 0.5, 1, 5, 10, 50, 100, 500, 1000, 5000])
base_probs = np.array([0.2, 0.45, 0.50, 0.45, 0.13, 0.008, 0.001, 0.00006, 0.00001, 0.0000065, 0.000005])
base_probs /= base_probs.sum()

# --- Anomaly distribution setup (RTP capped ≤ 40) ---
anomaly_probs_raw = np.array([0.01, 0.35, 0.80, 0.46, 0.2, 0.065, 0.001, 0.0006, 0.00001, 0.0000065, 0.000005])
anomaly_probs = anomaly_probs_raw / anomaly_probs_raw.sum()
anomaly_rtp = (win_multipliers * anomaly_probs).sum()


def generate_round(game_id, vendor_id, force_anomaly=False):
    """Create a single round with optional anomaly spike."""
    bet = 1
    if force_anomaly:
        multiplier = np.random.choice(win_multipliers, p=anomaly_probs)
    else:
        multiplier = np.random.choice(win_multipliers, p=base_probs)

    win = bet * multiplier
    return {
        "gameID": game_id,
        "vendorID": vendor_id,
        "bet": int(bet),
        "win": float(round(win, 2)),
        "playerID": f"player_{random.randint(1, 1000000)}"
    }


def simulator_worker(host, port, game_id, vendor_id, rps, batch_size, induce_anomalies, spike_interval, spike_duration, jitter_fraction):
    rounds_per_batch = batch_size
    interval = rounds_per_batch / rps  # seconds per batch

    spike_interval = timedelta(seconds=spike_interval)
    spike_duration = timedelta(seconds=spike_duration)

    # ⏱️ Delay first spike until spike_interval passes
    last_spike_time = datetime.now()
    print(f"[INIT] First spike for {game_id}:{vendor_id} scheduled after {spike_interval.total_seconds()} seconds.")
    in_spike = False

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        print(f"[CONNECTED] {game_id}:{vendor_id}")

        while True:
            now = datetime.now()

            # --- Spike trigger logic ---
            if induce_anomalies:
                time_since_last_spike = now - last_spike_time

                if not in_spike and time_since_last_spike >= spike_interval:
                    in_spike = True
                    last_spike_time = now
                    print(f"[{now}] [SPIKE START] {game_id}:{vendor_id}")
                    print(f"[{now}] [DEBUG] Anomaly RTP during spike: {anomaly_rtp:.2f}")

                elif in_spike and now - last_spike_time >= spike_duration:
                    in_spike = False
                    print(f"[{now}] [SPIKE END] {game_id}:{vendor_id}")

            # --- Generate batch ---
            batch = [
                generate_round(game_id, vendor_id, force_anomaly=in_spike)
                for _ in range(rounds_per_batch)
            ]
            payload = '\n'.join(json.dumps(r) for r in batch) + '\n'
            sock.sendall(payload.encode())

            # --- Apply jitter ---
            jitter = random.uniform(-jitter_fraction, jitter_fraction) * interval
            sleep_time = max(0, interval + jitter)
            time.sleep(sleep_time)

    except Exception as e:
        print(f"[ERROR] {game_id}:{vendor_id} - {e}")
    finally:
        sock.close()


def main():
    parser = argparse.ArgumentParser(description="Simulate real-time gameplay rounds with jitter")
    parser.add_argument("--host", type=str, default="localhost", help="Socket host")
    parser.add_argument("--port", type=int, default=6996, help="Socket port")
    parser.add_argument("--games", type=int, default=3, help="Number of unique game IDs")
    parser.add_argument("--vendors", type=int, default=2, help="Number of unique vendor IDs")
    parser.add_argument("--rps", type=int, default=1000, help="Total rounds per second")
    parser.add_argument("--batch", type=int, default=100, help="Rounds per batch")
    parser.add_argument("--workers", type=int, default=6, help="Number of parallel threads")
    parser.add_argument("--anomaly", action="store_true", help="Induce anomaly spikes")
    parser.add_argument("--spike-interval", type=int, default=5*3600, help="Seconds between spikes (default: 5h)")
    parser.add_argument("--spike-duration", type=int, default=5*60, help="Spike duration in seconds (default: 5 min)")
    parser.add_argument("--jitter", type=float, default=0.2, help="Jitter fraction to vary batch send interval (default: 0.2)")

    args = parser.parse_args()

    total_combinations = args.games * args.vendors
    if args.workers > total_combinations:
        print(f"[WARN] Requested {args.workers} workers but only {total_combinations} unique game/vendor pairs available.")
        args.workers = total_combinations

    # Generate unique (gameID, vendorID) combinations
    all_combinations = [(f"game{g+1}", f"vendor{v+1}") for g in range(args.games) for v in range(args.vendors)]
    random.shuffle(all_combinations)
    assigned_pairs = all_combinations[:args.workers]

    rps_per_worker = args.rps // args.workers

    print(f"🚀 Launching simulation: {args.rps} RPS across {args.workers} workers...")
    print(f"📊 Spike every {args.spike_interval}s for {args.spike_duration}s" if args.anomaly else "📊 Anomaly disabled")
    print(f"⚙️ Jitter enabled with fraction: {args.jitter}")

    executor = ThreadPoolExecutor(max_workers=args.workers)
    for i, (game_id, vendor_id) in enumerate(assigned_pairs):
        executor.submit(
            simulator_worker,
            args.host,
            args.port,
            game_id,
            vendor_id,
            rps_per_worker,
            args.batch,
            args.anomaly,
            args.spike_interval,
            args.spike_duration,
            args.jitter
        )

    executor.shutdown(wait=True)


if __name__ == "__main__":
    main()
