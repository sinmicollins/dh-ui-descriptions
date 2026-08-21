# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Streamlit-free core for the Dataplex DQ/DPS spec generators: FuelIX client,
BigQuery metadata access, the scan-id build/validate cascade, governance-YAML
inventory, YAML quoting/repair/validation, and the DPS field/render logic.

BigQuery clients and LLM callables are passed in by the caller (dependency
injection); the Streamlit wrappers/caches live in dq_ui, the DQ-app generation
pipeline in dq_generation."""
import csv
import functools
import glob
import itertools
import json
import logging
import math
import os
import re
import sys
import time

import google.auth.exceptions
import keyring
import keyring.errors
import requests
import yaml
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    """Idempotent app-startup logging: stderr, level from DQ_LOG_LEVEL
    (default INFO). basicConfig is a no-op once the root logger has handlers,
    so Streamlit reruns don't stack duplicate handlers."""
    logging.basicConfig(level=os.environ.get("DQ_LOG_LEVEL", "INFO").upper(),
                        stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")


# Failure families the UI boundaries turn into on-page messages instead of a
# crashed page. Neither logs nor messages ever carry sample rows, description
# text, or credentials — the policy-tag firewall extends to logging.
KNOWN_ERRORS = (
    GoogleAPIError,                          # BigQuery / Secret Manager calls
    google.auth.exceptions.GoogleAuthError,  # ADC/credential resolution
    requests.RequestException, keyring.errors.KeyringError, yaml.YAMLError,
    RuntimeError, ValueError, KeyError, IndexError, TypeError, OSError,
)

# --- FuelIX (TELUS LLM gateway) ------------------------------------------------
FUELIX_API_URL = "https://api.fuelix.ai/v1/chat/completions"
FUELIX_MODELS_URL = "https://api.fuelix.ai/v1/models"
# Same service/username as the companion CLI so one stored key serves all tools.
KEYRING_SERVICE = "gemini-3.1-flash-lite"
KEYRING_USERNAME = "gpt_api_key"
FUELIX_TIMEOUT_SECONDS = 120
DEFAULT_MODEL = "mistral-small-3.2-24b"
_NON_CHAT_PATTERNS = ("embedding", "whisper", "transcribe", "tts", "dall-e", "imagen", "-image")
FALLBACK_MODELS = [
    "mistral-small-3.2-24b", "claude-sonnet-4-6-anthropic", "gpt-5",
    "gemini-3.1-flash-lite", "gemini-3.5-flash", "gemini-3.1-pro-preview",
]
_FUELIX_RETRY_STATUSES = (429, 500, 502, 503, 504)
_FUELIX_MAX_ATTEMPTS = 3

# --- Dataplex conventions --------------------------------------------------------
SOURCE_PROJECTS = {
    ("dv", "dh1"): "cio-datahub-enterprise-dv-e8ff",
    ("dv", "dh2"): "cio-datahub-lake-dv-783079",
    ("pr", "dh1"): "cio-datahub-enterprise-pr-183a",
    ("pr", "dh2"): "cio-datahub-lake-pr-58ee8d",
}
MAX_SCAN_ID_LEN = 36
ABBREV_FILE = os.path.join(os.path.dirname(__file__), "abbreviations.csv")
# Orchestrator checkout: env override first, else a sibling of this repo.
DEFAULT_REPO_ROOT = os.environ.get("DATAPLEX_REPO_ROOT") or os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "datahub-orchestrator-dataplex"))


# --- FuelIX access -----------------------------------------------------------------
def get_fuelix_api_key() -> str:
    """Env-var override first, else the OS keyring."""
    env_key = os.environ.get("FUELIX_API_KEY") or os.environ.get("GPT_API_KEY")
    if env_key:
        return env_key.strip()
    try:
        return (keyring.get_password(KEYRING_SERVICE, KEYRING_USERNAME) or "").strip()
    except (keyring.errors.KeyringError, OSError) as e:
        logger.warning("keyring read failed: %s", e)
        return ""


def call_fuelix(system_instruction, user_content, api_key, model, temperature=0.0,
                *, http=requests, sleep=time.sleep) -> str:
    """One chat-completions call with bounded retries for transient failures
    (429/5xx/timeouts, exponential backoff). Reasoning models reject
    non-default temperature with a 400; that case is retried once without the
    field and does not consume a network attempt. `http`/`sleep` are
    dependency-injection seams for tests."""
    if not api_key:
        raise RuntimeError("No FuelIX API key set. Enter one in the sidebar.")
    payload = {
        "model": model,
        "messages": [{"role": "system", "content": system_instruction},
                     {"role": "user", "content": user_content}],
        "temperature": temperature,
    }
    attempt, temp_retried, started = 1, False, time.monotonic()
    while True:
        try:
            resp = http.post(
                FUELIX_API_URL, json=payload, timeout=FUELIX_TIMEOUT_SECONDS,
                headers={"Authorization": f"Bearer {api_key}",
                         "Content-Type": "application/json"},
            )
        except (requests.Timeout, requests.ConnectionError) as e:
            logger.warning("FuelIX %s attempt %d/%d failed: %s", model, attempt,
                           _FUELIX_MAX_ATTEMPTS, type(e).__name__)
            if attempt >= _FUELIX_MAX_ATTEMPTS:
                raise RuntimeError(
                    f"FuelIX unreachable after {attempt} attempts: {e}") from e
            sleep(2 ** (attempt - 1))
            attempt += 1
            continue
        if (resp.status_code == 400 and not temp_retried
                and "temperature" in resp.text and "temperature" in payload):
            del payload["temperature"]  # free retry: not a network failure
            temp_retried = True
            continue
        if resp.status_code in _FUELIX_RETRY_STATUSES and attempt < _FUELIX_MAX_ATTEMPTS:
            logger.warning("FuelIX %s HTTP %d on attempt %d/%d — retrying", model,
                           resp.status_code, attempt, _FUELIX_MAX_ATTEMPTS)
            sleep(2 ** (attempt - 1))
            attempt += 1
            continue
        if not resp.ok:
            # Surface the gateway's own message (context-length, bad model, ...)
            # instead of raise_for_status()'s body-less HTTPError.
            raise RuntimeError(f"FuelIX {resp.status_code}: {resp.text[:300]}")
        try:
            body = resp.json()
            content = body["choices"][0]["message"]["content"]
        except (ValueError, KeyError, IndexError, TypeError) as e:
            raise RuntimeError(f"FuelIX returned an unexpected response shape: {e}") from e
        logger.info("FuelIX %s ok in %.1fs (attempt %d, prompt_chars=%d, usage=%s)",
                    model, time.monotonic() - started, attempt,
                    len(system_instruction) + len(user_content), body.get("usage"))
        return content


def is_chat_model(model_id: str) -> bool:
    mid = (model_id or "").lower()
    return bool(mid) and not any(p in mid for p in _NON_CHAT_PATTERNS)


def fetch_fuelix_models(api_key: str, http=requests) -> list:
    """Sorted chat-model ids from /v1/models; [] on any failure."""
    if not api_key:
        return []
    try:
        resp = http.get(FUELIX_MODELS_URL, timeout=30,
                        headers={"Authorization": f"Bearer {api_key}"})
        resp.raise_for_status()
        ids = (m.get("id") for m in resp.json().get("data", []) if m.get("id"))
        return sorted(m for m in ids if is_chat_model(m))
    except (requests.RequestException, ValueError, KeyError, TypeError, AttributeError) as e:
        logger.warning("FuelIX model list fetch failed: %s", e)
        return []


# JSON allows only \" \\ \/ \b \f \n \r \t \uXXXX; models writing a regex
# routinely emit a bare \d, which json.loads rejects. Doubling stray
# backslashes recovers the object instead of failing the run.
_STRAY_JSON_ESCAPE = re.compile(r'\\(?!["\\/bfnrtu])')


def parse_llm_json(raw: str):
    """Best-effort JSON object from an LLM reply (tolerates fences and prose);
    None if nothing parses. Escape repair runs only after strict parsing fails."""
    cleaned = re.sub(r"```(json)?", "", raw, flags=re.IGNORECASE).strip()
    start, end = cleaned.find("{"), cleaned.rfind("}")
    candidates = [cleaned] + ([cleaned[start:end + 1]] if 0 <= start < end else [])
    for text in candidates + [_STRAY_JSON_ESCAPE.sub(r"\\\\", c) for c in candidates]:
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            continue
    return None


# --- BigQuery ----------------------------------------------------------------------
_BQ_IDENT_RES = {
    "project": re.compile(r"^[a-z0-9][a-z0-9.:-]{0,62}$"),
    "dataset": re.compile(r"^[A-Za-z0-9_]{1,1024}$"),
    "table": re.compile(r"^[A-Za-z0-9_]{1,1024}$"),
    "column": re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,299}$"),
}


def bq_ident(value, kind: str) -> str:
    """`value` validated against the BigQuery lexical rules for `kind`.
    Identifier slots (project/dataset/table/column names) cannot be query
    parameters, so anything user-typed is allowlisted before being spliced
    into SQL. Deliberately narrower than BigQuery's exotic flexible names:
    the datahub estate is snake_case, and a clear early error beats an
    injection surface."""
    value = str(value or "")
    if not _BQ_IDENT_RES[kind].fullmatch(value):
        raise ValueError(f"invalid BigQuery {kind} identifier: {value!r}")
    return value


def list_dataset_tables(client, project: str, dataset: str) -> list:
    project, dataset = bq_ident(project, "project"), bq_ident(dataset, "dataset")
    query = f"""
    SELECT table_name FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
    WHERE table_type = 'BASE TABLE' ORDER BY table_name
    """
    return [row["table_name"] for row in client.query(query)]


# Audit-column candidates, highest priority first (matched case-insensitively
# against TIMESTAMP/DATETIME/DATE columns).
AUDIT_PRIORITY = [
    "last_updt_ts", "last_update_ts", "src_last_updt_ts", "last_updt_tms",
    "last_upd_ts", "updt_ts", "update_ts", "last_updt_dt",
    "create_ts", "created_ts", "creation_ts", "create_dt", "__source_ts_ms",
]
AUDIT_REGEX = re.compile(r"(updt|update|audit|source_ts)", re.IGNORECASE)


def fetch_table_metadata(client, project: str, dataset: str, tables: tuple) -> dict:
    """{table: {partition_column, partition_column_type, require_partition_filter,
    temporal_columns}} from INFORMATION_SCHEMA. Both jobs are submitted before
    either result is read, so they run concurrently."""
    project, dataset = bq_ident(project, "project"), bq_ident(dataset, "dataset")
    meta = {t: {"partition_column": "", "partition_column_type": "",
                "require_partition_filter": False, "temporal_columns": []}
            for t in tables}
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("tables", "STRING", list(tables))])
    cols_job = client.query(f"""
    SELECT table_name, column_name, data_type, is_partitioning_column
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name IN UNNEST(@tables)
      AND (is_partitioning_column = 'YES' OR data_type IN ('TIMESTAMP', 'DATETIME', 'DATE'))
    ORDER BY table_name, ordinal_position
    """, job_config=job_config)
    opts_job = client.query(f"""
    SELECT table_name, option_value
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLE_OPTIONS`
    WHERE option_name = 'require_partition_filter' AND table_name IN UNNEST(@tables)
    """, job_config=job_config)
    for row in cols_job:
        m = meta.get(row["table_name"])
        if m is None:
            continue
        dtype = (row["data_type"] or "").upper()
        if row["is_partitioning_column"] == "YES":
            m["partition_column"], m["partition_column_type"] = row["column_name"], dtype
        if dtype in ("TIMESTAMP", "DATETIME", "DATE"):
            m["temporal_columns"].append((row["column_name"], dtype))
    for row in opts_job:
        m = meta.get(row["table_name"])
        if m is not None:
            m["require_partition_filter"] = str(row["option_value"]).strip().lower() == "true"
    return meta


def pick_audit_column(temporal_columns: list) -> str:
    """First audit-style temporal column: priority list, then regex fallback."""
    by_lower = {name.lower(): name for name, _typ in temporal_columns}
    for cand in AUDIT_PRIORITY:
        if cand in by_lower:
            return by_lower[cand]
    for name, _typ in temporal_columns:
        if AUDIT_REGEX.search(name):
            return name
    return ""


# --- Scan-id build + validation ---------------------------------------------------------
@functools.lru_cache(maxsize=8)
def _load_abbreviations(path: str, mtime: float) -> dict:
    """full_word -> abbreviation (lowercased); empty value = drop the token.
    lru_cache returns a shared object — callers treat it as read-only."""
    abbrev = {}
    with open(path, encoding="utf-8-sig", newline="") as f:
        for i, row in enumerate(csv.reader(f)):
            if not row:
                continue
            key = row[0].strip().lower()
            if key and not (i == 0 and key == "full_word"):
                abbrev[key] = row[1].strip().lower() if len(row) > 1 else ""
    return abbrev


def load_abbreviations(path: str = ABBREV_FILE) -> dict:
    try:
        return _load_abbreviations(path, os.path.getmtime(path))
    except OSError:
        return {}


def strip_bq_prefix(table: str) -> str:
    return table[3:] if table.lower().startswith("bq_") else table


def _abbreviate_tokens(text: str, abbrev: dict) -> str:
    """Map each _-token through abbrev; drop-tokens (mapped to '') disappear."""
    mapped = ((part, abbrev.get(part.lower(), part)) for part in text.split("_"))
    return "_".join(m for part, m in mapped if m or part.lower() not in abbrev)


def trim_job_id(job_id: str, abbrev: dict) -> str:
    """Strip trailing drop-tokens / underscores; never below one token."""
    parts = job_id.split("_")
    while len(parts) > 1 and (parts[-1] == "" or abbrev.get(parts[-1].lower()) == ""):
        parts.pop()
    return "_".join(parts)


def build_scan_id(prefix: str, dataset: str, table: str, abbrev: dict | None = None,
                  max_len: int = MAX_SCAN_ID_LEN) -> str:
    """Deterministic <prefix>_<dataset>_<table> id. Cascade: full name, then
    abbreviated table tokens, then abbreviated dataset tokens. May still exceed
    max_len if unshrinkable; the validator flags that."""
    abbrev = load_abbreviations() if abbrev is None else abbrev
    table = strip_bq_prefix(table)
    short_table = _abbreviate_tokens(table, abbrev)
    candidate = ""
    for ds, tbl in ((dataset, table), (dataset, short_table),
                    (_abbreviate_tokens(dataset, abbrev), short_table)):
        candidate = re.sub(r"[^a-z0-9_]", "_",
                           trim_job_id(f"{prefix}_{ds}_{tbl}", abbrev).lower())
        if len(candidate) <= max_len:
            return candidate
    return candidate


def validate_scan_id(candidate: str, prefix: str, forbidden: set,
                     max_len: int = MAX_SCAN_ID_LEN):
    """(ok, reason): length, ^<prefix>_[a-z0-9_]+$, no trailing separator,
    unique across repo + this run."""
    if not candidate:
        return False, "empty scan id"
    if len(candidate) > max_len:
        return False, f"length {len(candidate)} > {max_len}"
    if not re.match(rf"^{re.escape(prefix)}_[a-z0-9_]+$", candidate):
        return False, f"does not match ^{prefix}_[a-z0-9_]+$"
    if candidate.endswith(("_", "-")):
        return False, "ends with a separator"
    if candidate in forbidden:
        return False, "collides with an existing job id"
    return True, ""


def resolve_scan_id(prefix: str, dataset: str, table: str, forbidden: set,
                    max_len: int = MAX_SCAN_ID_LEN) -> str:
    """build_scan_id plus guaranteed fallbacks: always returns a valid id that
    is <= max_len and not in forbidden. Cascade: abbreviation (build_scan_id),
    then hard truncation, then numeric suffix _2, _3, ..."""
    candidate = build_scan_id(prefix, dataset, table, max_len=max_len)
    if len(candidate) > max_len:
        candidate = candidate[:max_len].rstrip("_-")
    if candidate in forbidden:
        for n in itertools.count(2):
            suffix = f"_{n}"
            alt = candidate[: max_len - len(suffix)].rstrip("_-") + suffix
            if alt not in forbidden:
                candidate = alt
                break
    return candidate


# --- Governance-YAML inventory (mtime-cached) ----------------------------------------------
@functools.lru_cache(maxsize=256)
def _load_yaml(path: str, mtime: float):
    try:
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f)
    except (OSError, yaml.YAMLError):
        return None


def load_yaml(path: str):
    """Parsed YAML (None if unreadable), cached until the file's mtime changes.
    lru_cache returns a shared object — callers treat it as read-only."""
    try:
        return _load_yaml(path, os.path.getmtime(path))
    except OSError:
        return None


def read_scans(path: str, top_key: str) -> dict:
    """The scans dict of a governance YAML ({} on any problem)."""
    try:
        document = load_yaml(path)
        scans = document["governance"]["consumer-governance"][top_key]["scans"] if document is not None else {}
    except (KeyError, TypeError):
        return {}
    return scans if isinstance(scans, dict) else {}


def collect_repo_scan_ids(gov_dir: str, prefix: str) -> set:
    """Scan keys across <prefix>_dps_*.yaml and <prefix>_dqs_*.yaml — DPS and
    DQ ids share one Dataplex namespace, so both count as forbidden."""
    ids = set()
    for pattern, key in ((f"{prefix}_dps_*.yaml", "dataplex-dp"),
                         (f"{prefix}_dqs_*.yaml", "dataplex-dq")):
        for path in glob.glob(os.path.join(gov_dir, pattern)):
            ids |= read_scans(path, key).keys()
    return ids


def merge_file_text(existing_text, header, blocks) -> str:
    """Append after existing scans, or create with header; one blank line
    between scan blocks and a single trailing newline."""
    body = "\n\n".join(blocks)
    base = (existing_text.rstrip("\n") + "\n\n" + body
            if existing_text is not None else header + body)
    return base.rstrip("\n") + "\n"


# --- YAML scalar rendering + repair + validation ---------------------------------------------
# Both apps emit governance YAML as text so the hand-maintained layout survives
# byte-for-byte; correct quoting is therefore this module's job.
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f-\x9f]")
_DQ_ESCAPES: dict = {c: f"\\x{c:02x}" for c in [*range(0x20), *range(0x7f, 0xa0)]}
_DQ_ESCAPES.update({ord("\\"): "\\\\", ord('"'): '\\"', ord("\n"): "\\n",
                    ord("\r"): "\\r", ord("\t"): "\\t"})


def yaml_quote(value) -> str:
    r"""`value` as a correctly quoted YAML scalar. Double-quoted style treats
    backslash as an escape introducer, so `"^\d{10}$"` is a parse error; values
    containing a backslash render single-quoted (no escapes, '' for a literal
    quote). Everything else keeps the double-quoted house style."""
    text = "" if value is None else str(value)
    if "\\" in text and not _CONTROL_CHARS.search(text):
        return "'" + text.replace("'", "''") + "'"
    return '"' + text.translate(_DQ_ESCAPES) + '"'


def finite_float(value):
    """`value` as a finite float, else None. Guards slots rendered as bare
    numbers against "99%", None, NaN, inf and booleans."""
    if value is None or isinstance(value, bool):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(num) or math.isinf(num) else num


def normalize_yaml_text(text: str) -> str:
    """Fix mechanical problems: CRLF/CR endings, tabs (illegal in YAML) to two
    spaces, trailing whitespace, guaranteed final newline."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    out = "\n".join(ln.replace("\t", "  ").rstrip() for ln in text.split("\n"))
    return out if out.endswith("\n") else out + "\n"


# Single-line double-quoted scalars in value position. Scanned backslash-aware
# ((?:[^"\\\n]|\\.)*) so an escaped quote \" inside a VALID scalar does not
# terminate the match and get the scalar mis-tokenized into corruption.
_LEGACY_BAD_SCALAR = re.compile(r'(?<=[:\[,])([ \t]*)"((?:[^"\\\n]|\\.)*)"')


def repair_invalid_escapes(text: str) -> str:
    r"""Repair the legacy naive '"'+value+'"' quoting: rewrite only double-quoted
    scalars that contain a backslash AND fail to parse in isolation
    (`regex: "^\d{10}$"` -> `regex: '^\d{10}$'`). Valid scalars stay
    byte-identical; line count is preserved. Callers must re-validate."""
    def _fix(match):
        inner = match.group(2)
        if "\\" not in inner:
            return match.group(0)
        try:
            yaml.safe_load('"' + inner + '"')
            return match.group(0)
        except yaml.YAMLError:
            return match.group(1) + yaml_quote(inner)
    return _LEGACY_BAD_SCALAR.sub(_fix, text)


def check_yaml_text(text: str, expected_scan_ids=(), top_key: str | None = None):
    """(ok, reason) for a rendered governance file. Beyond parsing, confirms
    every expected scan id survived into the parsed tree, so a quoting slip
    that parses into the wrong shape is caught too."""
    try:
        parsed = yaml.safe_load(text)
    except yaml.YAMLError as e:
        return False, str(e)
    if top_key is None:
        return True, ""
    try:
        scans = parsed["governance"]["consumer-governance"][top_key]["scans"]
    except (KeyError, TypeError):
        return False, f"parsed YAML has no governance.consumer-governance.{top_key}.scans"
    if not isinstance(scans, dict):
        return False, "`scans` did not parse as a mapping"
    missing = [s for s in expected_scan_ids if s not in scans]
    return (False, "scan(s) absent after parsing: " + ", ".join(missing)) if missing else (True, "")


def render_file_header(top_key: str, cron, publishing, export_ds) -> str:
    """Shared top-of-file wrapper + global settings, ending at `scans:`."""
    return (
        f"governance:\n  consumer-governance:\n    {top_key}:\n\n"
        "      execution_spec:\n        trigger:\n          schedule:\n"
        f"            cron: {yaml_quote(cron)}\n\n"
        f"      catalog_publishing_enabled: {str(bool(publishing)).lower()}\n\n"
        "      post_scan_actions:\n        bigquery_export:\n"
        f"          dataset_id: {yaml_quote(export_ds)}\n\n"
        "      scans:\n"
    )


# --- DPS (data-profiling scan) logic ------------------------------------------------------
def resolve_field(meta: dict):
    """The CLI's 6-step cascade. Returns (field, source_note); an empty field
    means manual review (the user can still type one in the editor)."""
    pc, pct, pf = (meta["partition_column"], meta["partition_column_type"],
                   meta["require_partition_filter"])
    if pct == "TIMESTAMP" and pc:                       # 1. cheapest: pruning
        return pc, "partition column (TIMESTAMP)"
    if pf:                                              # 2-3. filter required
        if pct in ("DATETIME", "DATE") and pc:
            return pc, f"partition column ({pct}, filter required)"
        return "", (f"require_partition_filter=TRUE but partition column type "
                    f"{pct or 'unknown'} is non-temporal — manual review")
    audit = pick_audit_column(meta["temporal_columns"])
    if audit:                                           # 4. audit column
        return audit, "audit column"
    if pct in ("DATETIME", "DATE") and pc:              # 5. temporal partition
        return pc, f"partition column ({pct})"
    return "", "no temporal partition or audit column found — manual review"


def load_repo_dps_tables(gov_dir: str, prefix: str) -> dict:
    """{(project, dataset, table): job_id} across every DPS file — used to
    skip tables that already have a profiling scan (additive-only)."""
    out = {}
    for path in sorted(glob.glob(os.path.join(gov_dir, f"{prefix}_dps_*.yaml"))):
        for job_id, block in read_scans(path, "dataplex-dp").items():
            src = block.get("data_source") if isinstance(block, dict) else None
            if not isinstance(src, dict):
                continue
            key = tuple(str(src.get(k) or "").strip()
                        for k in ("project_id", "dataset_id", "table_id"))
            if all(key) and key not in out:
                out[key] = job_id
    return out


def read_existing_cron(yaml_path: str) -> str:
    """Global cron of an existing DPS file ('' if unreadable/absent)."""
    node = load_yaml(yaml_path)
    for key in ("governance", "consumer-governance", "dataplex-dp",
                "execution_spec", "trigger", "schedule"):
        node = node.get(key) if isinstance(node, dict) else None
    return str(node.get("cron") or "").strip() if isinstance(node, dict) else ""


def render_dps_scan_block(job_id, project_id_, dataset_id_, table_id_, field_column, cron) -> str:
    # DATE() is polymorphic over TIMESTAMP/DATETIME/DATE, so one row_filter
    # template covers every resolvable field type.
    row_filter = f"DATE({field_column}) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)"
    return "\n".join([
        f"        {job_id}:",
        "          data_source:",
        f"            project_id: {yaml_quote(project_id_)}",
        f"            dataset_id: {yaml_quote(dataset_id_)}",
        f"            table_id: {yaml_quote(table_id_)}",
        "          execution_spec:",
        f"            field: {yaml_quote(field_column)}",
        "            trigger:",
        "              schedule:",
        f"                cron: {yaml_quote(cron)}",
        "          data_profile_spec:",
        f"            row_filter: {yaml_quote(row_filter)}",
    ])


def llm_rename_scan_ids(failing: list, forbidden: set, *, prefix: str,
                        dataset: str, call_llm) -> dict:
    """{table: new_id}, keeping only suggestions that pass validate_scan_id."""
    system_instruction = (
        "You rename BigQuery Dataplex scan job ids that violate naming rules. "
        "Return ONLY a JSON object mapping table_name to a new job id. Every id must:\n"
        f"1. be <= {MAX_SCAN_ID_LEN} characters;\n"
        f"2. match ^{prefix}_[a-z0-9_]+$ (start with the literal prefix '{prefix}_');\n"
        "3. not end with an underscore or hyphen;\n"
        "4. not collide with the forbidden ids nor with each other.\n"
        "Abbreviate tokens of the dataset/table name rather than inventing unrelated words.")
    prompt = json.dumps({"dataset": dataset, "rows": failing,
                         "forbidden_ids": sorted(forbidden)}, separators=(",", ":"))
    data = parse_llm_json(call_llm(system_instruction, prompt))
    if not isinstance(data, dict):
        return {}
    accepted, taken = {}, set(forbidden)
    for row in failing:
        cand = str(data.get(row["table"], "")).strip()
        if validate_scan_id(cand, prefix, taken)[0]:
            accepted[row["table"]] = cand
            taken.add(cand)
    return accepted
