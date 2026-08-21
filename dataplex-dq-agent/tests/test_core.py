"""dq_core: FuelIX client (retries/errors), identifier allowlist, scan-id
cascade equivalence, DPS cron reader, LLM rename validation."""
import itertools
from types import SimpleNamespace

import pytest
import requests

import dq_core
from conftest import FakeResp


# --- call_fuelix ---------------------------------------------------------------
def _http(responses):
    """http stub whose post() pops canned responses; records payloads."""
    sent = []

    def post(url, json=None, timeout=None, headers=None):
        sent.append(dict(json))  # snapshot: call_fuelix mutates the payload
        resp = responses.pop(0)
        if isinstance(resp, Exception):
            raise resp
        return resp

    return SimpleNamespace(post=post), sent


def test_call_fuelix_surfaces_gateway_error_body():
    http, _ = _http([FakeResp({}, 400, text="Request exceeds the context window")])
    with pytest.raises(RuntimeError) as ei:
        dq_core.call_fuelix("sys", "user", "KEY", "model", http=http, sleep=lambda s: None)
    assert "400" in str(ei.value) and "Request exceeds the context window" in str(ei.value)


def test_call_fuelix_requires_key():
    with pytest.raises(RuntimeError, match="No FuelIX API key set. Enter one in the sidebar."):
        dq_core.call_fuelix("sys", "user", "", "model")


def test_call_fuelix_retries_transient_then_succeeds():
    ok = FakeResp({"choices": [{"message": {"content": "hi"}}], "usage": {"total_tokens": 5}})
    http, _ = _http([FakeResp({}, 429, text="slow down"), ok])
    slept = []
    out = dq_core.call_fuelix("sys", "user", "KEY", "model", http=http, sleep=slept.append)
    assert out == "hi" and slept == [1]


def test_call_fuelix_gives_up_after_bounded_attempts():
    http, _ = _http([requests.Timeout("t"), requests.ConnectionError("c"),
                     requests.Timeout("t")])
    slept = []
    with pytest.raises(RuntimeError, match="unreachable after 3 attempts"):
        dq_core.call_fuelix("sys", "user", "KEY", "model", http=http, sleep=slept.append)
    assert slept == [1, 2]


def test_call_fuelix_temperature_retry_is_free_and_single():
    ok = FakeResp({"choices": [{"message": {"content": "hi"}}]})
    http, sent = _http([FakeResp({}, 400, text="temperature not supported"), ok])
    slept = []
    out = dq_core.call_fuelix("sys", "user", "KEY", "model", http=http, sleep=slept.append)
    assert out == "hi" and slept == []                      # no attempt consumed
    assert "temperature" in sent[0] and "temperature" not in sent[1]


def test_call_fuelix_guards_response_shape():
    http, _ = _http([FakeResp({"unexpected": True})])
    with pytest.raises(RuntimeError, match="unexpected response shape"):
        dq_core.call_fuelix("sys", "user", "KEY", "model", http=http, sleep=lambda s: None)


# --- bq_ident allowlist ----------------------------------------------------------
def test_bq_ident_accepts_normal_names():
    assert dq_core.bq_ident("src-proj", "project") == "src-proj"
    assert dq_core.bq_ident("domain.com:proj", "project") == "domain.com:proj"
    assert dq_core.bq_ident("ent_actvn", "dataset") == "ent_actvn"
    assert dq_core.bq_ident("bq_actvn_txn", "table") == "bq_actvn_txn"
    assert dq_core.bq_ident("updt_ts", "column") == "updt_ts"


@pytest.mark.parametrize("value,kind", [
    ("src_ds`; DROP TABLE x; --", "dataset"),
    ("a.b", "dataset"),
    ("", "table"),
    ("t name", "table"),
    ("1col", "column"),
    ("col-name", "column"),
    ("Proj", "project"),
])
def test_bq_ident_rejects_injection_and_junk(value, kind):
    with pytest.raises(ValueError, match="invalid BigQuery"):
        dq_core.bq_ident(value, kind)


# --- read_existing_cron (DPS) ------------------------------------------------------
def test_read_existing_cron(tmp_path):
    def write(name, text):
        p = tmp_path / name
        p.write_text(text, encoding="utf-8")
        return str(p)

    good = write("cron_good.yaml",
                 "governance:\n  consumer-governance:\n    dataplex-dp:\n"
                 "      execution_spec:\n        trigger:\n          schedule:\n"
                 "            cron: \"0 8 * * 0\"\n      scans: {}\n")
    assert dq_core.read_existing_cron(good) == "0 8 * * 0"
    no_cron = write("cron_missing.yaml",
                    "governance:\n  consumer-governance:\n    dataplex-dp:\n"
                    "      execution_spec:\n        trigger:\n          schedule: {}\n")
    assert dq_core.read_existing_cron(no_cron) == ""
    wrong_shape = write("cron_shape.yaml", "governance:\n  consumer-governance: [1, 2]\n")
    assert dq_core.read_existing_cron(wrong_shape) == ""
    scalar_doc = write("cron_scalar.yaml", "just a string\n")
    assert dq_core.read_existing_cron(scalar_doc) == ""
    assert dq_core.read_existing_cron(str(tmp_path / "nope.yaml")) == ""
    int_cron = write("cron_int.yaml",
                     "governance:\n  consumer-governance:\n    dataplex-dp:\n"
                     "      execution_spec:\n        trigger:\n          schedule:\n"
                     "            cron: 5\n")
    assert dq_core.read_existing_cron(int_cron) == "5"


# --- _abbreviate_tokens equivalence + build_scan_id cascade -------------------------
def _original_abbreviate(text, abbrev):
    out = []
    for part in text.split("_"):
        if part.lower() in abbrev:
            if abbrev[part.lower()]:
                out.append(abbrev[part.lower()])
        else:
            out.append(part)
    return "_".join(out)


ABBREV = {"account": "acct", "activation": "actvn", "the": "", "of": "", "x": "x"}


def test_abbreviate_tokens_equivalence():
    tokens = ["account", "Activation", "THE", "of", "keep", "", "x", "9"]
    for n in (1, 2, 3):
        for combo in itertools.product(tokens, repeat=n):
            text = "_".join(combo)
            assert dq_core._abbreviate_tokens(text, ABBREV) == \
                _original_abbreviate(text, ABBREV), text


def test_build_scan_id_cascade():
    assert dq_core.build_scan_id("dh1", "ent_actvn", "bq_account_table", ABBREV) \
        == "dh1_ent_actvn_account_table"      # fits: full-name cascade step wins
    assert dq_core.build_scan_id("dh1", "ent_actvn", "bq_account_activation_of_the_table",
                                 ABBREV) == "dh1_ent_actvn_acct_actvn_table"


# --- llm_rename_scan_ids: model suggestions never bypass validation ------------------
def test_llm_rename_validates_every_suggestion():
    call_llm = lambda s, u: ('{"tbl_long": "dh1_ok_id", "tbl_bad": "WRONG-ID",'
                             ' "tbl_taken": "dh1_taken"}')
    out = dq_core.llm_rename_scan_ids(
        [{"table": "tbl_long", "candidate": "x", "reason": "r"},
         {"table": "tbl_bad", "candidate": "y", "reason": "r"},
         {"table": "tbl_taken", "candidate": "z", "reason": "r"}],
        {"dh1_taken"}, prefix="dh1", dataset="ds", call_llm=call_llm)
    assert out == {"tbl_long": "dh1_ok_id"}
