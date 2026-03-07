from __future__ import annotations

import argparse
import json

from shared.dlq_inspector import build_replay_payload, parse_dlq_record, replay_target_topic


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect a DLQ record and print replay payload")
    parser.add_argument("--input", required=True, help="Path to JSON file containing a DLQ record")
    args = parser.parse_args()

    with open(args.input, encoding="utf-8") as handle:
        raw = json.load(handle)

    record = parse_dlq_record(raw)
    replay_topic = replay_target_topic(record)
    replay_payload = build_replay_payload(record)

    print(json.dumps({"replay_topic": replay_topic, "replay_payload": replay_payload}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
