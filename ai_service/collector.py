"""Poll cache node hot-key feeds and submit them to the advisor."""

from __future__ import annotations

import argparse
import json
import time
from urllib.error import URLError
from urllib.request import Request, urlopen


def fetch_json(url: str) -> dict:
    with urlopen(url, timeout=3) as response:
        return json.loads(response.read())


def send_json(url: str, payload: object) -> dict:
    request = Request(url, json.dumps(payload).encode(), {"Content-Type": "application/json"}, method="POST")
    with urlopen(request, timeout=5) as response:
        return json.loads(response.read())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", action="append", required=True,
                        help="cache metrics base URL; repeat for each node, e.g. http://127.0.0.1:17401")
    parser.add_argument("--advisor", default="http://127.0.0.1:8000")
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.interval <= 0:
        parser.error("--interval must be positive")
    had_errors = False
    while True:
        for base_url in args.node:
            try:
                feed = fetch_json(base_url.rstrip("/") + "/hotkeys")
                observations = []
                for key in feed.get("keys", []):
                    observations.append({**key, "node_id": feed["node_id"], "node_load": feed["node_load"]})
                if observations:
                    result = send_json(args.advisor.rstrip("/") + "/predict/batch", observations)
                    for prediction in result["predictions"]:
                        print(json.dumps(prediction, sort_keys=True), flush=True)
            except (URLError, TimeoutError, ValueError) as error:
                had_errors = True
                print(f"advisor collection failed for {base_url}: {error}", flush=True)
        if args.once:
            return 1 if had_errors else 0
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
