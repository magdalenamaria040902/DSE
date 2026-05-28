from __future__ import annotations

import argparse
import random
import time
from datetime import datetime, timezone

from pymongo import MongoClient, ReturnDocument, WriteConcern


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MongoDB consumer for events_queue")
    p.add_argument(
        "--uri",
        default="mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0",
        help="MongoDB URI (replica-set recommended)",
    )
    p.add_argument("--db", default="hospital", help="Database name")
    p.add_argument("--queue", default="events_queue", help="Queue collection")
    p.add_argument("--results", default="events_results", help="Results collection")
    p.add_argument("--max-messages", type=int, default=2000, help="Stop after consuming this many")
    p.add_argument("--idle-timeout-s", type=int, default=15, help="Stop if queue stays empty this long")
    p.add_argument("--poll-ms", type=int, default=100, help="Sleep between empty polls")
    p.add_argument("--work-ms-min", type=int, default=5, help="Simulated work lower bound")
    p.add_argument("--work-ms-max", type=int, default=20, help="Simulated work upper bound")
    p.add_argument(
        "--write-concern",
        choices=["1", "majority"],
        default="majority",
        help="Write concern for updates/inserts",
    )
    p.add_argument(
        "--worker-name",
        default="consumer.py",
        help="Logical consumer identifier stored in queue/results",
    )
    return p.parse_args()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def ms_between(a: datetime, b: datetime) -> float:
    if a.tzinfo is None:
        a = a.replace(tzinfo=timezone.utc)
    if b.tzinfo is None:
        b = b.replace(tzinfo=timezone.utc)
    return (b - a).total_seconds() * 1000.0


def main() -> None:
    args = parse_args()

    w_value = 1 if args.write_concern == "1" else "majority"
    client = MongoClient(args.uri)
    db = client[args.db]

    queue = db.get_collection(args.queue, write_concern=WriteConcern(w=w_value))
    results = db.get_collection(args.results, write_concern=WriteConcern(w=w_value))

    queue.create_index([("state", 1), ("created_at", 1)])

    consumed = 0
    idle_start = time.perf_counter()
    t0 = time.perf_counter()

    print(
        f"[consumer:{args.worker_name}] queue={args.db}.{args.queue} max_messages={args.max_messages} "
        f"w={args.write_concern}"
    )

    while consumed < args.max_messages:
        event = queue.find_one_and_update(
            {"state": "NEW"},
            {
                "$set": {
                    "state": "PROCESSING",
                    "claimed_at": now_utc(),
                    "consumer": args.worker_name,
                }
            },
            sort=[("created_at", 1)],
            return_document=ReturnDocument.AFTER,
        )

        if event is None:
            if (time.perf_counter() - idle_start) >= args.idle_timeout_s:
                print(f"[consumer:{args.worker_name}] idle timeout reached ({args.idle_timeout_s}s), stopping.")
                break
            time.sleep(args.poll_ms / 1000.0)
            continue

        idle_start = time.perf_counter()

        work_ms = random.randint(args.work_ms_min, args.work_ms_max)
        time.sleep(work_ms / 1000.0)

        done_at = now_utc()
        created_at = event.get("created_at", done_at)
        end_to_end_ms = ms_between(created_at, done_at)

        queue.update_one(
            {"_id": event["_id"]},
            {
                "$set": {
                    "state": "DONE",
                    "done_at": done_at,
                    "work_ms": work_ms,
                    "end_to_end_ms": end_to_end_ms,
                }
            },
        )

        results.insert_one(
            {
                "event_id": event["_id"],
                "encounter_id": event.get("encounter_id"),
                "patient_nbr": event.get("patient_nbr"),
                "event_type": event.get("event_type"),
                "consumer": args.worker_name,
                "created_at": created_at,
                "done_at": done_at,
                "work_ms": work_ms,
                "end_to_end_ms": end_to_end_ms,
            }
        )

        consumed += 1
        if consumed % 100 == 0:
            elapsed = time.perf_counter() - t0
            rate = consumed / elapsed if elapsed > 0 else 0.0
            print(f"[consumer:{args.worker_name}] consumed={consumed} avg_rate={rate:.1f} msg/s")

    total_s = time.perf_counter() - t0
    print(
        f"[consumer:{args.worker_name}] DONE consumed={consumed} total_s={total_s:.2f} "
        f"throughput={consumed / total_s:.1f} msg/s"
    )


if __name__ == "__main__":
    main()
