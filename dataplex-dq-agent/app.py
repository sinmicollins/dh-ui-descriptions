
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

import json
import re
import os
import csv
import glob
import yaml
import requests
import keyring
import streamlit as st
from google.cloud import bigquery
from google.auth import default

# ---------------------------------------------------------
# FuelIX (TELUS LLM gateway) configuration
# ---------------------------------------------------------
# OpenAI-compatible chat-completions endpoint, hit with a Bearer API key stored
# in the OS keyring (Windows Credential Manager / macOS Keychain) under the same
# service/username as the companion CLI (generate_dataplex_dps_yaml_with_gpt.py),
# so a key configured once is shared across both tools. Avoids Vertex AI and the
# Gemini Developer API entirely.
FUELIX_API_URL = "https://api.fuelix.ai/v1/chat/completions"
FUELIX_MODELS_URL = "https://api.fuelix.ai/v1/models"
KEYRING_SERVICE = "gemini-3.1-flash-lite"
KEYRING_USERNAME = "gpt_api_key"
FUELIX_TIMEOUT_SECONDS = 120

# Model dropdown is populated live from FUELIX_MODELS_URL. DEFAULT_MODEL is pre-
# selected; FALLBACK_MODELS is used only when the endpoint can't be reached (no
# key / offline) so the app still runs.
DEFAULT_MODEL = "wasikan-v2-2"

# The /v1/models catalog also lists non-chat models (embeddings, speech, image).
# Those can't be used with the chat-completions endpoint, so they're filtered out
# of the picker by matching these substrings in the model id (case-insensitive).
_NON_CHAT_PATTERNS = (
    "embedding",   # text-embedding-*
    "whisper",     # whisper-1 (speech-to-text)
    "transcribe",  # gpt-4o-transcribe*
    "tts",         # tts-1, tts-1-hd (text-to-speech)
    "dall-e",      # dall-e-3 (image generation)
    "imagen",      # imagen-* (image generation)
    "-image",      # gemini-3.1-flash-image (image generation)
)


def is_chat_model(model_id: str) -> bool:
    """True unless the id looks like an embedding / audio / image model."""
    mid = (model_id or "").lower()
    return bool(mid) and not any(p in mid for p in _NON_CHAT_PATTERNS)


FALLBACK_MODELS = [
    "wasikan-v2-2",
    "wasikan-llama-3-3-70b",
    "wasikan-qwen-3-next-80b",
    "claude-sonnet-4-5",
    "gpt-5",
    "gemini-3.1-flash-lite",
    "gemini-3.5-flash",
    "gemini-3.1-pro-preview",
]

# ---------------------------------------------------------
# Dataplex DQ scan conventions (mirror generate_dataplex_dps_yaml_with_gpt.py)
# ---------------------------------------------------------
# Source (scanned-data) projects, keyed by (environment, datahub instance).
# dh1 == enterprise projects, dh2 == lake projects.
SOURCE_PROJECTS = {
    ("dv", "dh1"): "cio-datahub-enterprise-dv-e8ff",
    ("dv", "dh2"): "cio-datahub-lake-dv-783079",
    ("pr", "dh1"): "cio-datahub-enterprise-pr-183a",
    ("pr", "dh2"): "cio-datahub-lake-pr-58ee8d",
}

# Max scan/job-id length. Matches the companion CLI's JOB_ID_MAX_LEN. Names that
# exceed it are shortened token-by-token using abbreviations.csv (same method as
# generate_dataplex_dps_yaml_with_gpt.py); anything still too long is flagged by
# the validator.
MAX_SCAN_ID_LEN = 36

# Abbreviation table used to shrink scan ids to <= MAX_SCAN_ID_LEN. Lives next to
# this file so the app can load it via __file__.
ABBREV_FILE = os.path.join(os.path.dirname(__file__), "abbreviations.csv")

DEFAULT_REPO_ROOT = r"C:\Users\T976160\Desktop\Git Repos\datahub-orchestrator-dataplex"


def get_fuelix_api_key() -> str:
    """Resolve the FuelIX API key: env-var override first, else the OS keyring."""
    env_key = os.environ.get("FUELIX_API_KEY") or os.environ.get("GPT_API_KEY")
    if env_key:
        return env_key.strip()
    try:
        stored = keyring.get_password(KEYRING_SERVICE, KEYRING_USERNAME)
    except Exception:
        stored = None
    return (stored or "").strip()


def call_fuelix(system_instruction, user_content, api_key, model, temperature=0.1):
    """POST one chat-completions request to FuelIX and return the reply text."""
    if not api_key:
        raise RuntimeError(
            "No FuelIX API key set. Enter one in the sidebar (it's saved to the OS keyring)."
        )
    resp = requests.post(
        FUELIX_API_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": system_instruction},
                {"role": "user", "content": user_content},
            ],
            "temperature": temperature,
        },
        timeout=FUELIX_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"]


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fuelix_models(api_key: str):
    """Return the sorted list of model IDs exposed by the FuelIX /v1/models
    endpoint. Returns [] on any failure so the caller falls back to a static
    list. Cached (per key) so it isn't re-fetched on every rerun."""
    if not api_key:
        return []
    try:
        resp = requests.get(
            FUELIX_MODELS_URL,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        ids = (m.get("id") for m in data.get("data", []) if m.get("id"))
        return sorted(m for m in ids if is_chat_model(m))
    except Exception:
        return []

# Page configuration
st.set_page_config(
    page_title="Dataplex DQ Spec Generator Agent",
    page_icon="⚙️",
    layout="wide"
)

st.markdown("""
    <style>
    .main-header {
        font-size: 2.2rem;
        color: #1E3A8A;
        font-weight: bold;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.0rem;
        color: #475569;
        margin-bottom: 2rem;
    }
    .card {
        background-color: #F8FAFC;
        padding: 1.5rem;
        border-radius: 0.5rem;
        border: 1px solid #E2E8F0;
        margin-bottom: 1.5rem;
    }
    .stButton>button {
        background-color: #1E3A8A;
        color: white;
        font-weight: bold;
        border-radius: 0.375rem;
        border: none;
    }
    .stButton>button:hover {
        background-color: #1E40AF;
        color: white;
    }
    .hitl-feedback {
        background-color: #FFFBEB;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #F59E0B;
        margin-top: 1rem;
    }
    </style>
""", unsafe_allow_html=True)

st.write('<div class="main-header">Dataplex Auto-DQ Spec Generator Agent</div>', unsafe_allow_html=True)
# ---------------------------------------------------------
# Sidebar Settings
# ---------------------------------------------------------
st.sidebar.header("Connection Settings")

# Target env + datahub instance drive: the source project (table listing +
# data_source), the governance folder, and the dh1/dh2 file & job-id prefix.
environment = st.sidebar.selectbox(
    "Environment", ["dv", "pr"],
    help="dv -> edemm/dv/governance, pr -> edemm/pr/governance."
)
instance = st.sidebar.selectbox(
    "Datahub Instance", ["dh1", "dh2"],
    help="dh1 = enterprise projects, dh2 = lake projects. Also the file/job-id prefix."
)
source_project_id = SOURCE_PROJECTS[(environment, instance)]
st.sidebar.text_input(
    "Source Project (derived)", value=source_project_id, disabled=True,
    help="Project of the scanned data — derived from Environment + Instance. Used for table "
         "listing (INFORMATION_SCHEMA) and each scan's data_source.project_id."
)

repo_root = st.sidebar.text_input(
    "Dataplex Repo Root", value=DEFAULT_REPO_ROOT,
    help="Local path to the datahub-orchestrator-dataplex repo. YAML is written under "
         "<repo>/edemm/<env>/governance/."
)

project_id = st.sidebar.text_input("GCP Project ID (results table)", value="cio-datahub-governance-dv-b5c1")
dataset_id = st.sidebar.text_input(
    "Results Dataset ID (export)", value="dv_datahub_01_ne1_edemm_dp_export_result",
    help="Dataset that holds the scan RESULTS/profiling table. Used in the query's FROM clause."
)
bq_location = st.sidebar.text_input("BigQuery Data Location", value="northamerica-northeast1")
profile_table_name = st.sidebar.text_input("Profiling Results Table", value="dv_datahub_01_ne1_governance_scan_results")
source_dataset_id = st.sidebar.text_input(
    "Source Dataset (scanned data)", value="ent_cust_cust",
    help="Dataset of the ORIGINAL table(s) being scanned. Drives the output file name "
         "(<prefix>_dqs_<dataset>.yaml) and each scan's data_source.dataset_id."
)
# --- FuelIX API key: stored in the OS keyring, shared with the CLI tool ---
# Resolved before the model picker so the /v1/models list can be fetched with it.
fuelix_api_key = get_fuelix_api_key()
if fuelix_api_key:
    st.sidebar.success("FuelIX API key loaded from keyring.")
else:
    st.sidebar.warning("No FuelIX API key found — enter one below to save it.")
_new_key = st.sidebar.text_input(
    "FuelIX API Key (proxy.ai.telus.com)",
    type="password",
    help="Saved to the OS keyring (Windows Credential Manager) and reused on future runs."
)
if _new_key:
    try:
        keyring.set_password(KEYRING_SERVICE, KEYRING_USERNAME, _new_key.strip())
        fuelix_api_key = _new_key.strip()
        st.sidebar.success("Saved FuelIX API key to keyring.")
    except Exception as e:
        st.sidebar.error(f"Could not save key to keyring: {e}")

# --- Model picker: populated live from the FuelIX /v1/models catalog ---
if st.sidebar.button("🔄 Refresh model list"):
    fetch_fuelix_models.clear()
_live_models = fetch_fuelix_models(fuelix_api_key)
model_options = list(_live_models) if _live_models else list(FALLBACK_MODELS)
if DEFAULT_MODEL not in model_options:
    model_options.insert(0, DEFAULT_MODEL)
model_name = st.sidebar.selectbox(
    "Model (FuelIX)",
    model_options,
    index=model_options.index(DEFAULT_MODEL),
    help="All models exposed by the FuelIX gateway (api.fuelix.ai/v1/models). Default: wasikan-v2-2.",
)
if _live_models:
    st.sidebar.caption(f"{len(_live_models)} models loaded from FuelIX.")
else:
    st.sidebar.caption("⚠️ Using fallback model list (couldn't reach /v1/models — check API key).")

# --- Dataplex DQ scan settings (global block written into the governance YAML) ---
st.sidebar.header("DQ Scan Settings")
scan_cron = st.sidebar.text_input(
    "Scan Schedule (cron)", value="0 7 * * *",
    help="Global execution_spec.trigger.schedule.cron for a NEW dataset file. Existing files keep their cron."
)
export_dataset = st.sidebar.text_input(
    "BigQuery Export Dataset", value="default",
    help="post_scan_actions.bigquery_export.dataset_id for the scan results."
)
catalog_publishing_enabled = st.sidebar.checkbox(
    "Catalog Publishing Enabled", value=True,
    help="Sets catalog_publishing_enabled in a newly created spec file."
)

# Derived paths / prefixes used throughout.
governance_dir = os.path.join(repo_root, "edemm", environment, "governance")
file_prefix = f"{instance}_dqs_"          # e.g. dh1_dqs_
job_prefix = instance                     # e.g. dh1 (job ids: dh1_<dataset>_<table>)
output_filename = f"{file_prefix}{source_dataset_id}.yaml"


# Initialize BQ Client
bq_client = None
try:
    bq_client = bigquery.Client(project=project_id, location=bq_location)
except Exception as e:
    st.sidebar.error(f"BQ Client Init Failed: {e}")

# Model access is via FuelIX — see call_fuelix() / get_fuelix_api_key() above.


# ---------------------------------------------------------
# Input Panel
# ---------------------------------------------------------
st.write("### Target Selection")
st.caption(
    f"Env **{environment}** / **{instance}** → source project `{source_project_id}`, "
    f"output `edemm/{environment}/governance/{output_filename}`."
)
selection_mode = st.radio("Input Type", ["Single Table", "List of Tables", "Entire Dataset"])

table_names = []
if selection_mode == "Single Table":
    single_table = st.text_input("Table Name", value="bq_acct")
    if single_table:
        table_names = [single_table.strip()]
elif selection_mode == "List of Tables":
    table_list_str = st.text_input("Table Names (comma separated)", value="bq_acct")
    if table_list_str:
        table_names = [t.strip() for t in table_list_str.split(",") if t.strip()]
else:
    st.info(
        f"Tables will be listed from `{source_project_id}.{source_dataset_id}` "
        "INFORMATION_SCHEMA.TABLES."
    )
    if st.button("Fetch Tables from INFORMATION_SCHEMA"):
        if bq_client is None:
            st.error("BigQuery client failed to initialize. Cannot fetch tables.")
        else:
            try:
                # Identifiers can't be parameterized; source_project_id is derived from a
                # fixed mapping and source_dataset_id is a controlled config value.
                query = f"""
                SELECT table_name
                FROM `{source_project_id}.{source_dataset_id}.INFORMATION_SCHEMA.TABLES`
                WHERE table_type = 'BASE TABLE'
                ORDER BY table_name
                """
                query_job = bq_client.query(query)
                table_names = [row["table_name"] for row in query_job]
                st.session_state["fetched_tables"] = table_names
                st.success(f"Found {len(table_names)} tables: {', '.join(table_names)}")
            except Exception as e:
                st.error(f"Error retrieving tables: {e}")
    # Persist a fetched list across reruns so downstream steps still see it.
    if not table_names and st.session_state.get("fetched_tables"):
        table_names = st.session_state["fetched_tables"]

# ---------------------------------------------------------
# Helper Functions — profiling
# ---------------------------------------------------------
def get_column_profiles(tables):
    """Retrieve latest profiling metrics for target tables from BigQuery"""
    if bq_client is None:
        raise RuntimeError("BigQuery client is not initialized.")
    query = f"""
    WITH RankedProfiles AS (
      SELECT
        data_source.table_id as table_name,
        column_name,
        column_type,
        column_mode,
        percent_null,
        percent_unique,
        min_value,
        max_value,
        average_value,
        standard_deviation,
        top_n,
        ROW_NUMBER() OVER (
          PARTITION BY data_source.table_id, column_name
          ORDER BY job_start_time DESC
        ) as rank
      FROM `{project_id}.{dataset_id}.{profile_table_name}`
      WHERE data_source.dataset_id = @source_dataset_id
        AND data_source.table_id IN UNNEST(@table_names)
    )
    SELECT * FROM RankedProfiles WHERE rank = 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source_dataset_id", "STRING", source_dataset_id),
            bigquery.ArrayQueryParameter("table_names", "STRING", tables),
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)
    results = []
    for row in query_job:
        # Format top_n records to list of dicts
        row_dict = dict(row)
        top_n_val = []
        top_n_data = row_dict.get("top_n")
        if isinstance(top_n_data, list):
            for item in top_n_data:
                top_n_val.append({
                    "value": item.get("value"),
                    "count": item.get("count"),
                    "percent": item.get("percent")
                })

        results.append({
            "table_name": row["table_name"],
            "column_name": row["column_name"],
            "column_type": row["column_type"],
            "column_mode": row["column_mode"],
            "percent_null": row["percent_null"],
            "percent_unique": row["percent_unique"],
            "min_value": row["min_value"],
            "max_value": row["max_value"],
            "average_value": row["average_value"],
            "top_n": top_n_val
        })
    return results


# ---------------------------------------------------------
# Helper Functions — scan-id build + validation
# (mirrors generate_dataplex_dps_yaml_with_gpt.py: build_job_id / _validate_gpt_job_id)
# ---------------------------------------------------------
def load_abbreviations(path: str = ABBREV_FILE) -> dict:
    """Load full_word -> abbreviation from abbreviations.csv.

    Keys and values are lowercased (scan ids must be lowercase). An empty value is
    a 'drop this token' marker, matching the companion CLI's abbreviation sheet.
    """
    abbrev = {}
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if not row:
                    continue
                key = row[0].strip().lower()
                if not key or (i == 0 and key == "full_word"):
                    continue
                val = row[1].strip().lower() if len(row) > 1 else ""
                abbrev[key] = val
    except OSError:
        pass  # missing file -> no abbreviation (validator will flag overflow)
    return abbrev


def strip_bq_prefix(table: str) -> str:
    """Drop a leading bq_ from a table name for the scan-id (bq_acct -> acct)."""
    return table[3:] if table.lower().startswith("bq_") else table


def _abbreviate_tokens(text: str, abbrev: dict) -> str:
    """Replace each underscore token with its abbreviation; empty abbrev drops it."""
    out = []
    for part in text.split("_"):
        key = part.lower()
        if key in abbrev:
            short = abbrev[key]
            if short:
                out.append(short)
            # empty short -> drop the token entirely
        else:
            out.append(part)
    return "_".join(out)


def trim_job_id(job_id: str, abbrev: dict) -> str:
    """Strip trailing drop-token abbreviations and stray trailing underscores.
    Never reduces below a single token (so the prefix can't be eaten)."""
    parts = job_id.split("_")
    while len(parts) > 1:
        last = parts[-1].lower()
        if last in abbrev and abbrev[last] == "":
            parts.pop()
            continue
        if parts[-1] == "":
            parts.pop()
            continue
        break
    return "_".join(parts)


def _finalize_scan_id(candidate: str) -> str:
    """Lowercase + sanitize to [a-z0-9_]."""
    return re.sub(r"[^a-z0-9_]", "_", candidate.lower())


def build_scan_id(prefix: str, dataset: str, table: str, abbrev: dict | None = None,
                  max_len: int = MAX_SCAN_ID_LEN) -> str:
    """Deterministic scan/job id: <prefix>_<dataset>_<stripped_table>.

    Mirrors the companion build_job_id: try the full name, then abbreviate table
    tokens, then dataset tokens, until it fits max_len. Result is lowercased and
    sanitized to [a-z0-9_]. May still exceed max_len if unshrinkable — the
    validator flags that."""
    abbrev = abbrev or {}
    stripped = strip_bq_prefix(table)

    candidate = trim_job_id(f"{prefix}_{dataset}_{stripped}", abbrev)
    if len(_finalize_scan_id(candidate)) <= max_len:
        return _finalize_scan_id(candidate)

    short_table = _abbreviate_tokens(stripped, abbrev)
    candidate = trim_job_id(f"{prefix}_{dataset}_{short_table}", abbrev)
    if len(_finalize_scan_id(candidate)) <= max_len:
        return _finalize_scan_id(candidate)

    short_dataset = _abbreviate_tokens(dataset, abbrev)
    candidate = trim_job_id(f"{prefix}_{short_dataset}_{short_table}", abbrev)
    return _finalize_scan_id(candidate)


# Loaded once per session; passed into build_scan_id.
ABBREV = load_abbreviations()


def validate_scan_id(candidate: str, prefix: str, forbidden: set, max_len: int = MAX_SCAN_ID_LEN):
    """Standalone validation (outside the LLM). Returns (ok, reason).

    1. Length must be <= max_len characters.
    2. Must match ^<prefix>_[a-z0-9_]+$ (lowercase letters, digits, underscores only;
       must start with the literal '<prefix>_').
    3. Must NOT end with an underscore or hyphen.
    4. Must NOT collide with any job id already created (repo + this run).
    """
    if not candidate:
        return False, "empty scan id"
    if len(candidate) > max_len:
        return False, f"length {len(candidate)} > {max_len}"
    if not re.match(rf"^{re.escape(prefix)}_[a-z0-9_]+$", candidate):
        return False, f"does not match ^{prefix}_[a-z0-9_]+$"
    if candidate.endswith("_") or candidate.endswith("-"):
        return False, "ends with a separator (_ or -)"
    if candidate in forbidden:
        return False, "collides with an existing job id (must be unique across the repo + this run)"
    return True, ""


def read_existing_scan_ids(yaml_path: str) -> set:
    """Return the set of scan keys already present in a dataplex-dq YAML file."""
    try:
        with open(yaml_path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        scans = data["governance"]["consumer-governance"]["dataplex-dq"]["scans"]
    except (OSError, KeyError, TypeError, yaml.YAMLError):
        return set()
    return set(scans.keys()) if isinstance(scans, dict) else set()


def collect_repo_scan_ids(gov_dir: str, prefix: str) -> set:
    """Union of scan keys across every <prefix>_dqs_*.yaml file in the governance dir."""
    ids = set()
    pattern = os.path.join(gov_dir, f"{prefix}_dqs_*.yaml")
    for path in glob.glob(pattern):
        ids |= read_existing_scan_ids(path)
    return ids


# ---------------------------------------------------------
# Helper Functions — YAML rendering (dataplex-dq scan format)
# ---------------------------------------------------------
_I_DASH = " " * 14   # list dash for a rule item
_I_KEY = " " * 16    # rule keys
_I_SUB = " " * 18    # expectation children


def _fmt_threshold(value) -> str:
    """1 / 1.0 -> '1.0', 0.99 -> '0.99'."""
    v = float(value)
    if v == int(v):
        return f"{int(v)}.0"
    return repr(v)


def _fmt_num(value) -> str:
    """Whole numbers render without a decimal (min_value: 0), else as-is."""
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float) and value == int(value):
        return str(int(value))
    return str(value)


def _q(value) -> str:
    return '"' + str(value) + '"'


def rule_is_valid(rule: dict) -> bool:
    """A renderable rule needs name, dimension, and a known expectation type."""
    exp = rule.get("expectation") or {}
    return bool(rule.get("name") and rule.get("dimension") and exp.get("type"))


def render_rule(rule: dict) -> list:
    """Render one rule dict to indented YAML lines matching the target format."""
    lines = []
    exp = rule.get("expectation") or {}
    etype = exp.get("type")
    column = rule.get("column")

    if column:
        lines.append(_I_DASH + f'- column: {_q(column)}')
        lines.append(_I_KEY + f'name: {_q(rule["name"])}')
    else:
        lines.append(_I_DASH + f'- name: {_q(rule["name"])}')
    lines.append(_I_KEY + f'dimension: {_q(rule["dimension"])}')

    # Table-level expectations carry no threshold; column-level ones do.
    if etype != "table_condition_expectation" and rule.get("threshold") is not None:
        lines.append(_I_KEY + f'threshold: {_fmt_threshold(rule["threshold"])}')

    if etype == "non_null_expectation":
        lines.append(_I_KEY + "non_null_expectation: true")
    elif etype == "uniqueness_expectation":
        lines.append(_I_KEY + "uniqueness_expectation: true")
    elif etype == "range_expectation":
        lines.append(_I_KEY + "range_expectation:")
        if exp.get("min_value") is not None:
            lines.append(_I_SUB + f'min_value: {_fmt_num(exp["min_value"])}')
        if exp.get("max_value") is not None:
            lines.append(_I_SUB + f'max_value: {_fmt_num(exp["max_value"])}')
    elif etype == "set_expectation":
        values = exp.get("values", []) or []
        rendered = ", ".join(_q(v) for v in values)
        lines.append(_I_KEY + "set_expectation:")
        lines.append(_I_SUB + f"values: [{rendered}]")
        if exp.get("ignore_null"):
            lines.append(_I_SUB + "ignore_null: true")
    elif etype == "regex_expectation":
        lines.append(_I_KEY + "regex_expectation:")
        lines.append(_I_SUB + f'regex: {_q(exp.get("regex", ""))}')
        if exp.get("ignore_null"):
            lines.append(_I_SUB + "ignore_null: true")
    elif etype == "table_condition_expectation":
        lines.append(_I_KEY + "table_condition_expectation:")
        lines.append(_I_SUB + f'sql_expression: {_q(exp.get("sql_expression", ""))}')
    return lines


def render_scan_block(scan_id, project_id_, dataset_id_, table_id_, rules) -> str:
    """Render a single scan block (indented 8 spaces under `scans:`)."""
    lines = [
        f"        {scan_id}:",
        "          data_source:",
        f'            project_id: "{project_id_}"',
        f'            dataset_id: "{dataset_id_}"',
        f'            table_id: "{table_id_}"',
        "          data_quality_spec:",
        "            rules:",
    ]
    for rule in rules:
        if rule_is_valid(rule):
            lines.extend(render_rule(rule))
    return "\n".join(lines)


def render_file_header(cron, publishing, export_ds) -> str:
    """Top-of-file governance/dataplex-dq wrapper + global settings, ending at `scans:`."""
    return (
        "governance:\n"
        "  consumer-governance:\n"
        "    dataplex-dq:\n"
        "\n"
        "      execution_spec:\n"
        "        trigger:\n"
        "          schedule:\n"
        f'            cron: "{cron}"\n'
        "\n"
        f"      catalog_publishing_enabled: {'true' if publishing else 'false'}\n"
        "\n"
        "      post_scan_actions:\n"
        "        bigquery_export:\n"
        f'          dataset_id: "{export_ds}"\n'
        "\n"
        "      scans:\n"
    )


def merge_file_text(existing_text, header, blocks) -> str:
    """Compose final file text. Append after existing scans, or create with header."""
    body = "\n\n".join(blocks)
    if existing_text is not None:
        return existing_text.rstrip("\n") + "\n\n" + body + "\n"
    return header + body + "\n"


def yaml_path_for(gov_dir, prefix, dataset) -> str:
    return os.path.join(gov_dir, f"{prefix}_dqs_{dataset}.yaml")


# ---------------------------------------------------------
# LLM steps
# ---------------------------------------------------------
def generate_action_plan_via_gemini(profile_json):
    """Step 1: Ask the model to generate an Action Plan with justifications."""
    system_instruction = """
    You are an expert Google Cloud Dataplex data quality engineer and data analyst.
    Your task is to analyze the provided BigQuery column profiling data and propose a step-by-step data quality rules Action Plan for a Dataplex DQ (data quality) scan.

    In your plan:
    1. Identify which specific columns are good candidates for Dataplex DQ scan rules such as
       non_null_expectation (COMPLETENESS), uniqueness_expectation (UNIQUENESS),
       range_expectation / set_expectation / regex_expectation (VALIDITY), or table-level
       table_condition_expectation (VOLUME / FRESHNESS).
    2. Provide a clear justification for each rule suggestion based on the statistical metrics
       (e.g., "Recommend range_expectation for antenna_face because values range between 1 and 3 without outliers",
       or "Recommend uniqueness_expectation for acct_id because percent_unique is 100%").
    3. Do NOT write any YAML/JSON yet. Focus only on analysis and justification in clear markdown formatting.
    """

    prompt = f"""
    Analyze this column profiling data and draft a data quality action plan:
    {json.dumps(profile_json, indent=2)}
    """

    return call_fuelix(system_instruction, prompt, fuelix_api_key, model_name, temperature=0.1)


def generate_rules_via_gemini(profile_json, action_plan, hitl_feedback, uploaded_file=None):
    """Step 2: Return {table_name: [rule_dict, ...]}.

    The model produces ONLY the per-column rule set as JSON. Scan ids, data_source
    blocks, the governance wrapper and file placement are built deterministically in
    Python so the output is guaranteed to match the datahub-orchestrator-dataplex format.
    """
    system_instruction = """
    You are an expert Google Cloud Dataplex data quality engineer.
    From column profiling data, an action plan, and user feedback, produce Dataplex DQ scan RULES.

    You MUST return ONLY a single JSON object (no markdown, no commentary) of this exact shape:

    {
      "tables": {
        "<table_name>": {
          "rules": [
            {
              "column": "<column_name>",          // omit for table-level rules
              "name": "<rule-name>",               // lowercase, hyphens only (NO underscores)
              "dimension": "COMPLETENESS|UNIQUENESS|VALIDITY|VOLUME|FRESHNESS",
              "threshold": 0.99,                    // float; omit for table-level rules
              "expectation": { "type": "...", ... } // exactly one, see below
            }
          ]
        }
      }
    }

    Expectation objects (choose based on the profiling stats and action plan):
    - {"type": "non_null_expectation"}                              // dimension COMPLETENESS, threshold 0.99
    - {"type": "uniqueness_expectation"}                            // dimension UNIQUENESS,   threshold 1.0
    - {"type": "range_expectation", "min_value": 0, "max_value": 9} // dimension VALIDITY, threshold 0.99 (min and/or max)
    - {"type": "set_expectation", "values": ["A","B"], "ignore_null": true} // dimension VALIDITY, threshold 0.99 (ignore_null optional)
    - {"type": "regex_expectation", "regex": "^.{9}$", "ignore_null": true} // dimension VALIDITY, threshold 0.99 (ignore_null optional)
    - {"type": "table_condition_expectation", "sql_expression": "COUNT(*) > 0"} // dimension VOLUME or FRESHNESS; NO column, NO threshold

    Rules:
    - Use the table_name keys EXACTLY as they appear in the profiling data.
    - Only reference columns that appear in the profiling data. Do NOT invent columns.
    - `name` must be lowercase and contain only letters, numbers and hyphens
      (e.g. "acct-id-not-null", "valid-card-brand-values").
    - You MUST incorporate all user feedback/adjustments.
    - Return ONLY the JSON object.
    """

    # FuelIX chat-completions is text-only. If a reference file was uploaded,
    # inline its text content into the prompt (best-effort decode). Binary
    # formats (PDF / XLSX) can't be inlined here — warn and skip them.
    uploaded_text = ""
    if uploaded_file is not None:
        raw = uploaded_file.read()
        for enc in ("utf-8", "latin-1"):
            try:
                uploaded_text = raw.decode(enc)
                break
            except (UnicodeDecodeError, AttributeError):
                uploaded_text = ""
        if not uploaded_text.strip():
            st.warning(
                f"Uploaded file '{getattr(uploaded_file, 'name', 'file')}' could not be read as "
                "text (binary formats like PDF/XLSX aren't supported by the FuelIX text endpoint). "
                "Ignoring it — paste key requirements into the feedback box instead."
            )

    prompt = f"""
    Profiling Data:
    {json.dumps(profile_json, indent=2)}

    Proposed Action Plan:
    {action_plan}

    User Adjustments/HITL Feedback:
    {hitl_feedback}

    Reference document contents (if any):
    {uploaded_text if uploaded_text.strip() else "(none provided)"}

    Return the JSON object of rules per table.
    """

    raw_text = call_fuelix(system_instruction, prompt, fuelix_api_key, model_name, temperature=0.1)

    cleaned = re.sub(r"```(json)?", "", raw_text, flags=re.IGNORECASE).strip()
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        start, end = cleaned.find("{"), cleaned.rfind("}")
        if start == -1 or end == -1:
            raise ValueError("Model did not return JSON. Raw response:\n" + raw_text)
        data = json.loads(cleaned[start:end + 1])

    tables = data.get("tables", data) if isinstance(data, dict) else {}
    out = {}
    for tbl, spec in tables.items():
        if isinstance(spec, dict):
            out[tbl] = spec.get("rules", []) or []
        elif isinstance(spec, list):
            out[tbl] = spec
        else:
            out[tbl] = []
    return out


def build_scan_plan(table_order, rules_by_table):
    """Build ordered per-table scan plan with deterministic ids + validation.

    Returns (plan, blocks). `plan` rows carry table/scan_id/status/ok/rule_count for
    the report; `blocks` is the list of rendered YAML scan blocks that passed.
    """
    forbidden = collect_repo_scan_ids(governance_dir, job_prefix)
    target_path = yaml_path_for(governance_dir, job_prefix, source_dataset_id)
    forbidden |= read_existing_scan_ids(target_path)

    plan, blocks = [], []
    assigned = set()
    for table in table_order:
        rules = [r for r in (rules_by_table.get(table) or []) if rule_is_valid(r)]
        scan_id = build_scan_id(job_prefix, source_dataset_id, table, ABBREV)
        ok, reason = validate_scan_id(scan_id, job_prefix, forbidden | assigned)
        if ok and not rules:
            ok, reason = False, "no valid rules generated"
        status = "ok — will add" if ok else f"skip: {reason}"
        if ok:
            blocks.append(render_scan_block(scan_id, source_project_id, source_dataset_id, table, rules))
            assigned.add(scan_id)
        plan.append({
            "table": table,
            "scan_id": scan_id,
            "rules": len(rules),
            "status": status,
        })
    return plan, blocks


# ---------------------------------------------------------
# Step 1: Formulate Action Plan
# ---------------------------------------------------------
if len(table_names) > 0:
    st.write("---")
    st.write("### Step 1: Formulate Action Plan")

    if st.button("Generate Action Plan"):
        with st.spinner("Retrieving profiling statistics and drafting plan..."):
            try:
                profiles = get_column_profiles(table_names)
                st.session_state["profiles"] = profiles
                st.session_state["table_order"] = table_names
                st.session_state.pop("rules_by_table", None)

                if not profiles:
                    st.warning("No profiling data found in BigQuery for the selected table(s).")
                else:
                    plan = generate_action_plan_via_gemini(profiles)
                    st.session_state["action_plan"] = plan
                    st.success("Action plan drafted!")
            except Exception as e:
                st.error(f"Error generating action plan: {e}")

if "action_plan" in st.session_state:
    st.markdown("#### Proposed Action Plan from Agent")
    st.info("Review the statistical rule proposals below and provide adjustments in the feedback section.")
    st.markdown(st.session_state["action_plan"])

    st.write("---")
    st.write("### Step 2: Human-in-the-Loop Feedback & Rule Generation")

    # HITL Text Box
    hitl_feedback = st.text_area(
        "Apply Custom Business Knowledge / Override Rules",
        value="The plan looks good. Please proceed.",
        help="Provide directions to override rules (e.g. 'Remove volume check', 'Add Antarctica to geo_country allowed set')"
    )

    # File Uploader for rules logic
    uploaded_file = st.file_uploader(
        "Upload Rules Reference/Instructions (PDF, CSV, Excel, TXT)",
        type=["pdf", "csv", "xlsx", "xls", "txt"]
    )

    if st.button("Generate DQ Rules"):
        with st.spinner("Incorporating feedback and generating rules..."):
            try:
                rules_by_table = generate_rules_via_gemini(
                    st.session_state["profiles"],
                    st.session_state["action_plan"],
                    hitl_feedback,
                    uploaded_file
                )
                st.session_state["rules_by_table"] = rules_by_table
                st.success("DQ rules generated!")
            except Exception as e:
                st.error(f"Error generating rules: {e}")

# ---------------------------------------------------------
# Step 3: Build scan plan, preview, validate, and deploy
# ---------------------------------------------------------
if "rules_by_table" in st.session_state:
    table_order = st.session_state.get("table_order", table_names)
    plan, blocks = build_scan_plan(table_order, st.session_state["rules_by_table"])

    target_path = yaml_path_for(governance_dir, job_prefix, source_dataset_id)
    file_exists = os.path.exists(target_path)
    existing_text = None
    if file_exists:
        try:
            with open(target_path, encoding="utf-8") as f:
                existing_text = f.read()
        except OSError as e:
            st.error(f"Could not read existing file {target_path}: {e}")

    header = render_file_header(scan_cron, catalog_publishing_enabled, export_dataset)
    full_text = merge_file_text(existing_text, header, blocks) if blocks else (existing_text or "")

    st.write("---")
    st.write("### Step 3: Scan-ID Validation Report")
    st.caption(
        f"Job-id rules: length ≤ {MAX_SCAN_ID_LEN}, must match `^{job_prefix}_[a-z0-9_]+$`, "
        "no trailing separator, unique across the repo + this run."
    )
    st.dataframe(plan, use_container_width=True)

    n_ok = len(blocks)
    n_skip = len(plan) - n_ok
    if n_ok:
        st.success(f"{n_ok} scan(s) will be added." + (f" {n_skip} skipped." if n_skip else ""))
    else:
        st.warning("No scans passed validation — nothing to add. See the report above.")

    st.write(f"### Target file: `edemm/{environment}/governance/{output_filename}`")
    st.info(
        (f"File exists — new scans will be **appended** ({len(read_existing_scan_ids(target_path))} already present)."
         if file_exists else
         "File does not exist yet — it will be **created** with the global settings header.")
    )

    if blocks:
        preview_label = "Full file (after append)" if file_exists else "New file preview"
        st.text_area(preview_label, value=full_text, height=420)

        st.download_button(
            label=f"Download {output_filename}",
            data=full_text,
            file_name=output_filename,
            mime="text/yaml"
        )

    # ---------------- Deployment ----------------
    st.write("### Deployment")
    st.caption(
        "Writes the spec into the datahub-orchestrator-dataplex repo. Commit & push there "
        "to deploy via the orchestrator (no gcloud call from this app)."
    )
    col1, col2 = st.columns(2)

    with col1:
        if st.button("Add to Dataplex repo (create/append)", disabled=not blocks):
            try:
                os.makedirs(governance_dir, exist_ok=True)
                final_text = merge_file_text(existing_text, header, blocks)
                with open(target_path, "w", encoding="utf-8", newline="\n") as f:
                    f.write(final_text)
                # Round-trip parse safety check.
                with open(target_path, encoding="utf-8") as f:
                    yaml.safe_load(f)
                action = "Appended to" if file_exists else "Created"
                st.success(f"{action} {target_path} (+{len(blocks)} scan(s)).")
                st.info("Commit and push this file to deploy via the orchestrator.")
            except yaml.YAMLError as e:
                st.error(f"Write aborted — YAML round-trip parse failed: {e}")
            except Exception as e:
                st.error(f"Failed to write file: {e}")

    with col2:
        if st.button("Save a copy to workspace root", disabled=not blocks):
            try:
                workspace_file_path = os.path.join(os.path.dirname(__file__), f"../{output_filename}")
                with open(workspace_file_path, "w", encoding="utf-8", newline="\n") as f:
                    f.write(full_text)
                st.success(f"Saved local copy: {output_filename}")
            except Exception as e:
                st.error(f"Failed to write file: {e}")
