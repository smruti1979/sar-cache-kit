#!/usr/bin/env python3
"""Prime the shared prompt prefix (SOPs, tool definitions, incident brief) on a local
OpenAI-compatible server (for example vLLM) so the first real agent call hits the
prefix cache. Run after every server restart and after any change to the prefix.

usage: warm_prefix.py --url http://127.0.0.1:8000 --model MODEL --prefix-file prefix.txt
"""
import argparse
import json
import urllib.request


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8000")
    ap.add_argument("--model", required=True)
    ap.add_argument("--prefix-file", required=True)
    args = ap.parse_args()
    with open(args.prefix_file, encoding="utf-8") as f:
        prefix = f.read()
    body = {"model": args.model, "max_tokens": 1,
            "messages": [{"role": "system", "content": prefix},
                         {"role": "user", "content": "ready"}]}
    req = urllib.request.Request(args.url + "/v1/chat/completions",
                                 data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=300) as r:
        usage = json.load(r).get("usage", {})
    print("prefix primed:", usage)


if __name__ == "__main__":
    main()
