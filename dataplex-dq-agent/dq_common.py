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
"""Shared helpers for the Dataplex DQ/DPS spec-generator Streamlit apps:
FuelIX access, cached BigQuery client, the scan-id build/validate cascade,
governance-YAML inventory, YAML quoting/repair/validation, and shared UI."""
import csv
import glob
import itertools
import json
import math
import os
import re

import keyring
import requests
import streamlit as st
import yaml
from google.cloud import bigquery

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

# --- Dataplex conventions --------------------------------------------------------
SOURCE_PROJECTS = {
    ("dv", "dh1"): "cio-datahub-enterprise-dv-e8ff",
    ("dv", "dh2"): "cio-datahub-lake-dv-783079",
    ("pr", "dh1"): "cio-datahub-enterprise-pr-183a",
    ("pr", "dh2"): "cio-datahub-lake-pr-58ee8d",
}
MAX_SCAN_ID_LEN = 36
ABBREV_FILE = os.path.join(os.path.dirname(__file__), "abbreviations.csv")
DEFAULT_REPO_ROOT = r"C:\Users\T976160\Desktop\Git Repos\datahub-orchestrator-dataplex"

_APP_CSS = """<style>
.main-header {font-size: 2.2rem; color: #1E3A8A; font-weight: bold; margin-bottom: 0.5rem;}
.stButton>button {background-color: #1E3A8A; color: white; font-weight: bold;
                  border-radius: 0.375rem; border: none;}
.stButton>button:hover {background-color: #1E40AF; color: white;}
</style>"""


# --- FuelIX access -----------------------------------------------------------------
def get_fuelix_api_key() -> str:
    """Env-var override first, else the OS keyring."""
    env_key = os.environ.get("FUELIX_API_KEY") or os.environ.get("GPT_API_KEY")
    if env_key:
        return env_key.strip()
    try:
        return (keyring.get_password(KEYRING_SERVICE, KEYRING_USERNAME) or "").strip()
    except Exception:
        return ""


def call_fuelix(system_instruction, user_content, api_key, model, temperature=0.0) -> str:
    """One chat-completions call. Reasoning models reject non-default
    temperature with a 400; that case is retried once without the field."""
    if not api_key:
        raise RuntimeError("No FuelIX API key set. Enter one in the sidebar.")
    payload = {
        "model": model,
        "messages": [{"role": "system", "content": system_instruction},
                     {"role": "user", "content": user_content}],
        "temperature": temperature,
    }
    for attempt in (1, 2):
        resp = requests.post(
            FUELIX_API_URL, json=payload, timeout=FUELIX_TIMEOUT_SECONDS,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        )
        if (attempt == 1 and resp.status_code == 400
                and "temperature" in resp.text and "temperature" in payload):
            del payload["temperature"]
            continue
        if not resp.ok:
            # Surface the gateway's own message (context-length, bad model, ...)
            # instead of raise_for_status()'s body-less HTTPError.
            raise RuntimeError(f"FuelIX {resp.status_code}: {resp.text[:300]}")
        return resp.json()["choices"][0]["message"]["content"]
    raise RuntimeError("call_fuelix: retry loop exhausted")


def is_chat_model(model_id: str) -> bool:
    mid = (model_id or "").lower()
    return bool(mid) and not any(p in mid for p in _NON_CHAT_PATTERNS)


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fuelix_models(api_key: str) -> list:
    """Sorted chat-model ids from /v1/models; [] on any failure."""
    if not api_key:
        return []
    try:
        resp = requests.get(FUELIX_MODELS_URL, timeout=30,
                            headers={"Authorization": f"Bearer {api_key}"})
        resp.raise_for_status()
        ids = (m.get("id") for m in resp.json().get("data", []) if m.get("id"))
        return sorted(m for m in ids if is_chat_model(m))
    except Exception:
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


# --- BigQuery (cached) ----------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def get_bq_client(project: str, location: str) -> bigquery.Client:
    return bigquery.Client(project=project, location=location)


@st.cache_data(ttl="15m", show_spinner=False)
def list_dataset_tables(project: str, dataset: str, location: str) -> list:
    query = f"""
    SELECT table_name FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
    WHERE table_type = 'BASE TABLE' ORDER BY table_name
    """
    return [row["table_name"] for row in get_bq_client(project, location).query(query)]


# Audit-column candidates, highest priority first (matched case-insensitively
# against TIMESTAMP/DATETIME/DATE columns).
AUDIT_PRIORITY = [
    "last_updt_ts", "last_update_ts", "src_last_updt_ts", "last_updt_tms",
    "last_upd_ts", "updt_ts", "update_ts", "last_updt_dt",
    "create_ts", "created_ts", "creation_ts", "create_dt", "__source_ts_ms",
]
AUDIT_REGEX = re.compile(r"(updt|update|audit|source_ts)", re.IGNORECASE)


@st.cache_data(ttl="15m", show_spinner=False)
def fetch_table_metadata(project: str, dataset: str, location: str, tables: tuple) -> dict:
    """{table: {partition_column, partition_column_type, require_partition_filter,
    temporal_columns}} from INFORMATION_SCHEMA. Both jobs are submitted before
    either result is read, so they run concurrently."""
    meta = {t: {"partition_column": "", "partition_column_type": "",
                "require_partition_filter": False, "temporal_columns": []}
            for t in tables}
    client = get_bq_client(project, location)
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
@st.cache_data(show_spinner=False)
def _load_abbreviations(path: str, mtime: float) -> dict:
    """full_word -> abbreviation (lowercased); empty value = drop the token."""
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
@st.cache_data(show_spinner=False, max_entries=256)
def _load_yaml(path: str, mtime: float):
    try:
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f)
    except (OSError, yaml.YAMLError):
        return None


def load_yaml(path: str):
    """Parsed YAML (None if unreadable), cached until the file's mtime changes."""
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


# --- Shared UI blocks --------------------------------------------------------------------------
def setup_page(title: str):
    st.set_page_config(page_title=title, layout="wide")
    st.markdown(_APP_CSS, unsafe_allow_html=True)
    st.write(f'<div class="main-header">{title}</div>', unsafe_allow_html=True)


def sidebar_connection():
    """Returns (environment, instance, source_project_id, repo_root)."""
    st.sidebar.header("Connection Settings")
    environment = st.sidebar.selectbox("Environment", ["dv", "pr"])
    instance = st.sidebar.selectbox(
        "Datahub Instance", ["dh1", "dh2"],
        help="dh1 = enterprise, dh2 = lake. Also the file/job-id prefix.")
    source_project_id = SOURCE_PROJECTS[(environment, instance)]
    st.sidebar.text_input("Source Project (derived)", value=source_project_id, disabled=True)
    repo_root = st.sidebar.text_input(
        "Dataplex Repo Root", value=DEFAULT_REPO_ROOT,
        help="YAML is written under <repo>/edemm/<env>/governance/.")
    return environment, instance, source_project_id, repo_root


def sidebar_fuelix(model_label: str, model_help: str):
    """FuelIX key management + model picker. Returns (api_key, model_name)."""
    api_key = get_fuelix_api_key()
    if not api_key:
        st.sidebar.warning("No FuelIX API key found — enter one below.")
    new_key = st.sidebar.text_input("FuelIX API Key", type="password",
                                    help="Saved to the OS keyring and reused.")
    if new_key:
        try:
            keyring.set_password(KEYRING_SERVICE, KEYRING_USERNAME, new_key.strip())
            api_key = new_key.strip()
        except Exception as e:
            st.sidebar.error(f"Keyring save failed: {e}")
    if st.sidebar.button("Refresh model list"):
        fetch_fuelix_models.clear()
    live = fetch_fuelix_models(api_key)
    models = list(live) or list(FALLBACK_MODELS)
    if DEFAULT_MODEL not in models:
        models.insert(0, DEFAULT_MODEL)
    model = st.sidebar.selectbox(model_label, models,
                                 index=models.index(DEFAULT_MODEL), help=model_help)
    if api_key and not live:
        st.sidebar.caption("Models endpoint unreachable — using fallback list.")
    return api_key, model


def sidebar_scan_settings(header: str, cron_default: str, cron_help: str):
    """Returns (cron, export_dataset, publishing_enabled)."""
    st.sidebar.header(header)
    cron = st.sidebar.text_input("Scan Schedule (cron)", value=cron_default, help=cron_help)
    export_ds = st.sidebar.text_input("BigQuery Export Dataset", value="default")
    publishing = st.sidebar.checkbox("Catalog Publishing Enabled", value=True)
    return cron, export_ds, publishing


def select_tables(project: str, dataset: str, location: str, state_key: str) -> list:
    """Single/list/entire-dataset table picker."""
    mode = st.radio("Input Type", ["Single Table", "List of Tables", "Entire Dataset"])
    if mode == "Single Table":
        table = st.text_input("Table Name", value="bq_actvn_servreq_transaction").strip()
        return [table] if table else []
    if mode == "List of Tables":
        raw = st.text_input("Table Names (comma separated)", value="bq_actvn_servreq_transaction")
        return [t.strip() for t in raw.split(",") if t.strip()]
    if st.button("Fetch Tables from INFORMATION_SCHEMA"):
        try:
            st.session_state[state_key] = list_dataset_tables(project, dataset, location)
            st.success(f"Found {len(st.session_state[state_key])} tables.")
        except Exception as e:
            st.error(f"Error retrieving tables: {e}")
    return st.session_state.get(state_key, [])


def read_target(target_path: str):
    """(exists, text) for the output file. A read failure blocks the page —
    proceeding with text=None would silently overwrite the file on deploy."""
    if not os.path.exists(target_path):
        return False, None
    try:
        with open(target_path, encoding="utf-8") as f:
            return True, f.read()
    except OSError as e:
        st.error(f"Cannot read {target_path}: {e}")
        st.stop()


def show_target(environment, output_filename, target_path, file_exists, top_key):
    st.write(f"Target file: `edemm/{environment}/governance/{output_filename}`")
    if file_exists:
        st.info(f"File exists — new scans append after the "
                f"{len(read_scans(target_path, top_key))} already present.")


def render_preview_and_deploy(existing_text, header, blocks, target_path,
                              governance_dir, output_filename, file_exists,
                              scan_ids=(), top_key=None):
    """Full-file preview and gated exits. The merged text is parse-checked once
    here and every route out — download, workspace copy, repo write — is gated
    on that result. An existing file broken by legacy quoting or mechanical
    whitespace is repaired in memory first, with the changes surfaced."""
    existing_ok, existing_error, repaired, id_clash = True, "", [], []
    if existing_text:
        existing_ok, existing_error = check_yaml_text(existing_text)
        if not existing_ok:
            cand = repair_invalid_escapes(normalize_yaml_text(existing_text))
            if cand != existing_text and check_yaml_text(cand, (), top_key)[0]:
                repaired = [(n, o, w) for n, (o, w) in enumerate(
                    zip(existing_text.splitlines(), cand.splitlines()), 1) if o != w]
                existing_text, existing_ok, existing_error = cand, True, ""
                if top_key:
                    # While unparseable, the file's ids were invisible to
                    # collect_repo_scan_ids; recheck to avoid duplicate keys.
                    scans = yaml.safe_load(cand)["governance"]["consumer-governance"][top_key]["scans"]
                    id_clash = [s for s in scan_ids if s in scans]

    full_text = merge_file_text(existing_text, header, blocks) if blocks else (existing_text or "")
    ok, err = (True, "") if not blocks else check_yaml_text(full_text, scan_ids, top_key)
    deployable = ok and not id_clash

    if blocks:
        if id_clash:
            st.error("Scan id(s) already present in the existing file (hidden until its "
                     "quoting was repaired) — appending blocked: `" + "`, `".join(id_clash)
                     + "`. Rename the affected scans or fix the repo file first.")
        elif ok:
            st.success(f"YAML check passed — {len(scan_ids) or len(blocks)} scan(s) "
                       "verified present after parsing.")
        elif not existing_ok:
            st.error("The existing file is invalid YAML — fix it in the repo first "
                     f"(this run's scans render fine):\n```\n{existing_error}\n```")
        else:
            st.error("Generated YAML failed validation — generator bug, please report:\n"
                     f"```\n{err}\n```")
        if repaired:
            detail = "\n".join(f"- line {n}: `{o.strip()}` -> `{w.strip()}`"
                               for n, o, w in repaired)
            st.warning("Existing file auto-repaired in memory (written on deploy):\n" + detail)
        st.text_area("File preview", value=full_text, height=420)
        st.download_button(f"Download {output_filename}", full_text, output_filename,
                           "text/yaml", disabled=not deployable)

    st.write("### Deployment")
    can_deploy = bool(blocks) and deployable
    exits = (("Write to Dataplex repo", target_path,
              f"Wrote {target_path} (+{len(blocks)} scan(s)). "
              "Commit and push to deploy via the orchestrator."),
             ("Save copy to workspace root",
              os.path.join(os.path.dirname(__file__), "..", output_filename),
              f"Saved {output_filename}."))
    for column, (label, path, done_msg) in zip(st.columns(2), exits):
        with column:
            if st.button(label, disabled=not can_deploy):
                try:
                    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
                    with open(path, "w", encoding="utf-8", newline="\n") as f:
                        f.write(full_text)
                    st.success(done_msg)
                except OSError as e:
                    st.error(f"Write failed: {e}")
