"""Byte-drift lock: the five LLM prompt constants must stay exactly as they
were before the productionization refactor (TELUS GenAI prompt standards are
pinned). Baseline SHA-256 hashes were recorded from the pre-refactor tree."""
import hashlib

import dq_generation as gen

BASELINE_SHA256 = {
    "_SYS_TABLE_DESC": "2eeb026816974e888e05601bf15c01ac3ce0b8e1a6e03580d45e4fcf05e21256",
    "_SYS_COL_DESC": "f550d1023a882b12f5e4f1283f158ff8614d77317749be323bb6b9cd9a1af1ef",
    "_SYS_PLAN": "4ddea3f3f3833a991e669689471741411a1a7079fb7dc8e9a321335e475b029a",
    "_SYS_RULES": "edec1a9c02c49f522e17fa53c7d5ed0b90eb02e9fbc7274fe5aa71533e603581",
    "_DESC_LABEL": "ce780850295020b6010d053640d4a57494306bd0a18a399f0e7410263961f85e",
}


def test_prompt_constants_byte_identical():
    for name, expected in BASELINE_SHA256.items():
        actual = hashlib.sha256(getattr(gen, name).encode("utf-8")).hexdigest()
        assert actual == expected, f"{name} drifted from the locked TELUS prompt"
