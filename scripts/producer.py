from __future__ import annotations

import argparse
import random
import time
from datetime import datetime, timezone

from pymongo import MongoClient, WriteConcern


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MongoDB producer for events_queue")
    p.add_argument(
        "--uri",
        default="mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0",
        help="MongoDB URI (replica-set recommended)",
    )
    p.add_argument("--db", default="hospital", help="Database name")
    p.add_argument("--source", default="admissions", help="Source collection")
    p.add_argument("--queue", default="events_queue", help="Queue collection")
    p.add_argument("--messages", type=int, default=2000, help="Total messages to publish")
    p.add_argument("--batch-size", type=int, default=100, help="Messages per insert batch")
    p.add_argument("--sleep-ms", type=int, default=100, help="Sleep between batches")
    p.add_argument(
        "--write-concern",
        choices=["1", "majority"],
        default="majority",
        help="Write concern for queue inserts",
    )
    p.add_argument(
        "--reset-queue",
        action="store_true",
        help="Drop queue collection before publishing",
    )
    p.add_argument(
        "--worker-name",
        default="producer.py",
        help="Logical producer identifier stored with each event",
    )
    return p.parse_args()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def main() -> None:
    args = parse_args()

    w_value = 1 if args.write_concern == "1" else "majority"
    client = MongoClient(args.uri)
    db = client[args.db]

    source = db[args.source]
    queue = db.get_collection(args.queue, write_concern=WriteConcern(w=w_value))

    if args.reset_queue:
        queue.drop()

    source_count = source.estimated_document_count()
    if source_count == 0:
        raise SystemExit("Source collection is empty. Import admissions first.")

    produced = 0
    t0 = time.perf_counter()

    print(
        f"[producer:{args.worker_name}] source={args.db}.{args.source} queue={args.db}.{args.queue} "
        f"messages={args.messages} batch={args.batch_size} w={args.write_concern}"
    )

    while produced < args.messages:
        n = min(args.batch_size, args.messages - produced)

        sample_docs = list(source.aggregate([{"$sample": {"size": n}}]))

        events = []
        for d in sample_docs:
            events.append(
                {
                    "event_type": random.choice(
                        [
                            "encounter_created",
                            "risk_recompute",
                            "readmission_check",
                        ]
                    ),
                    "encounter_id": d.get("encounter_id"),
                    "patient_nbr": d.get("patient_nbr"),
                    "admission_type_id": d.get("admission_type_id"),
                    "readmitted": d.get("readmitted"),
                    "state": "NEW",
                    "created_at": now_utc(),
                    "producer": args.worker_name,
                }
            )

        queue.insert_many(events, ordered=False)
        produced += len(events)

        elapsed = time.perf_counter() - t0
        rate = produced / elapsed if elapsed > 0 else 0.0
        print(f"[producer:{args.worker_name}] produced={produced}/{args.messages} avg_rate={rate:.1f} msg/s")

        if args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000.0)

    total_s = time.perf_counter() - t0
    print(
        f"[producer:{args.worker_name}] DONE produced={produced} total_s={total_s:.2f} "
        f"throughput={produced / total_s:.1f} msg/s"
    )


if __name__ == "__main__":
    main()
