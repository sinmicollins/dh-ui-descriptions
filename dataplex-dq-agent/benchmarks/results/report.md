# FuelIX model benchmark — DQ rule generation

Generated 2026-08-05 14:04. Gate: all hard checks pass on all repeats; rubric >= 85.0 when a golden exists.

## S1

| Model | Gates | Rubric | F1 | Median latency (s) | Tokens/run | $/run | Notes |
|---|---|---|---|---|---|---|---|
| mistral-small-3.2-24b | 1/1 | 68.2 | 0.545 | 44.44 | 8112 | 0.0 |  |
| gpt-5-nano | 1/2 | 75.3 | 0.667 | 69.36 | 13274 | 0.004198 | HTTPError: 400 Client Error: Bad Request for url: https://api.fuelix.ai/v1/chat/completions |
| wasikan-qwen-3-next-80b | 0/2 | - | - | 121.03 | 0 | 0.0 | ReadTimeout: HTTPSConnectionPool(host='api.fuelix.ai', port=443): Read timed out. (read timeout=120); HTTPError: 500 Server Error: Internal Server Error for url: https://api.fuelix.ai/v1/chat/completions |

**No model met the bar (all gates on all repeats + rubric >= 85.0) — escalate to the next tier.**

## S5 projection — all datasets (500 tables, computed)

| Model | Basis | Rate-limit cap (tables/min) | Sequential wall time (h) | Total cost ($) |
|---|---|---|---|---|
| mistral-small-3.2-24b | S1(1t) | - | 6.2 | 0.0 |
| gpt-5-nano | S1(1t) | 11300 | 9.6 | 2.1 |
