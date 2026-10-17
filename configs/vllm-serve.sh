#!/usr/bin/env bash
# Local LLM serving with prefix caching and a quantized KV cache.
# Verify flag names against your installed vLLM version before use.
set -euo pipefail
MODEL="${MODEL:?set MODEL to a local model path or id}"
exec vllm serve "$MODEL" \
  --enable-prefix-caching \
  --kv-cache-dtype fp8 \
  --gpu-memory-utilization 0.90 \
  --max-model-len 32768 \
  --max-num-seqs 32 \
  --port 8000
# After start, run tools/warm_prefix.py to prime the shared SOP prefix.
