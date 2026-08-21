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
"""Streamlit layer shared by both spec-generator apps: cached wrappers around
dq_core (clients are resolved inside the wrapper bodies, so cache keys stay
plain primitives and no secret or client object is ever hashed), the page and
sidebar blocks, and the gated YAML preview/deploy flow."""
import hashlib
import logging
import os

import keyring
import keyring.errors
import streamlit as st
import yaml
from google.cloud import bigquery

import dq_core

logger = logging.getLogger(__name__)

_APP_CSS = """<style>
.main-header {font-size: 2.2rem; color: #1E3A8A; font-weight: bold; margin-bottom: 0.5rem;}
.stButton>button {background-color: #1E3A8A; color: white; font-weight: bold;
                  border-radius: 0.375rem; border: none;}
.stButton>button:hover {background-color: #1E40AF; color: white;}
</style>"""


def st_notify(level: str, text: str) -> None:
    """UI seam injected into dq_core/dq_generation: level is the st API name
    (info/warning/error/caption/...), so message texts and timing stay exactly
    as they were when the pipeline called streamlit directly."""
    getattr(st, level)(text)


# --- Cached wrappers (cache keys identical to the pre-split decorators) ------------
@st.cache_resource(show_spinner=False)
def get_bq_client(project: str, location: str) -> bigquery.Client:
    return bigquery.Client(project=project, location=location)


@st.cache_data(ttl="15m", show_spinner=False)
def list_dataset_tables(project: str, dataset: str, location: str) -> list:
    return dq_core.list_dataset_tables(get_bq_client(project, location), project, dataset)


@st.cache_data(ttl="15m", show_spinner=False)
def fetch_table_metadata(project: str, dataset: str, location: str, tables: tuple) -> dict:
    return dq_core.fetch_table_metadata(get_bq_client(project, location),
                                        project, dataset, tables)


def _key_digest(api_key: str) -> str:
    return hashlib.sha256(api_key.encode()).hexdigest()[:16] if api_key else ""


@st.cache_data(ttl=3600, show_spinner=False)
def _cached_models(key_digest: str, _api_key: str) -> list:
    """Model list cached on a digest of the key — the secret itself is the
    unhashed `_api_key` and never becomes part of the cache index."""
    return dq_core.fetch_fuelix_models(_api_key)


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
    source_project_id = dq_core.SOURCE_PROJECTS[(environment, instance)]
    st.sidebar.text_input("Source Project (derived)", value=source_project_id, disabled=True)
    repo_root = st.sidebar.text_input(
        "Dataplex Repo Root", value=dq_core.DEFAULT_REPO_ROOT,
        help="YAML is written under <repo>/edemm/<env>/governance/.")
    return environment, instance, source_project_id, repo_root


def sidebar_fuelix(model_label: str, model_help: str):
    """FuelIX key management + model picker. Returns (api_key, model_name)."""
    api_key = dq_core.get_fuelix_api_key()
    if not api_key:
        st.sidebar.warning("No FuelIX API key found — enter one below.")
    new_key = st.sidebar.text_input("FuelIX API Key", type="password",
                                    help="Saved to the OS keyring and reused.")
    if new_key:
        try:
            keyring.set_password(dq_core.KEYRING_SERVICE, dq_core.KEYRING_USERNAME,
                                 new_key.strip())
            api_key = new_key.strip()
        except keyring.errors.KeyringError as e:
            logger.warning("keyring save failed: %s", e)
            st.sidebar.error(f"Keyring save failed: {e}")
    if st.sidebar.button("Refresh model list"):
        _cached_models.clear()
    live = _cached_models(_key_digest(api_key), api_key)
    models = list(live) or list(dq_core.FALLBACK_MODELS)
    if dq_core.DEFAULT_MODEL not in models:
        models.insert(0, dq_core.DEFAULT_MODEL)
    model = st.sidebar.selectbox(model_label, models,
                                 index=models.index(dq_core.DEFAULT_MODEL), help=model_help)
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
        except dq_core.KNOWN_ERRORS as e:
            logger.exception("table list fetch failed")
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
        logger.exception("cannot read deploy target %s", target_path)
        st.error(f"Cannot read {target_path}: {e}")
        st.stop()


def show_target(environment, output_filename, target_path, file_exists, top_key):
    st.write(f"Target file: `edemm/{environment}/governance/{output_filename}`")
    if file_exists:
        st.info(f"File exists — new scans append after the "
                f"{len(dq_core.read_scans(target_path, top_key))} already present.")


def render_preview_and_deploy(existing_text, header, blocks, target_path,
                              governance_dir, output_filename, file_exists,
                              scan_ids=(), top_key=None):
    """Full-file preview and gated exits. The merged text is parse-checked once
    here and every route out — download, workspace copy, repo write — is gated
    on that result. An existing file broken by legacy quoting or mechanical
    whitespace is repaired in memory first, with the changes surfaced."""
    existing_ok, existing_error, repaired, id_clash = True, "", [], []
    if existing_text:
        existing_ok, existing_error = dq_core.check_yaml_text(existing_text)
        if not existing_ok:
            cand = dq_core.repair_invalid_escapes(dq_core.normalize_yaml_text(existing_text))
            if cand != existing_text and dq_core.check_yaml_text(cand, (), top_key)[0]:
                repaired = [(n, o, w) for n, (o, w) in enumerate(
                    zip(existing_text.splitlines(), cand.splitlines()), 1) if o != w]
                existing_text, existing_ok, existing_error = cand, True, ""
                if top_key:
                    # While unparseable, the file's ids were invisible to
                    # collect_repo_scan_ids; recheck to avoid duplicate keys.
                    scans = yaml.safe_load(cand)["governance"]["consumer-governance"][top_key]["scans"]
                    id_clash = [s for s in scan_ids if s in scans]

    full_text = dq_core.merge_file_text(existing_text, header, blocks) if blocks else (existing_text or "")
    ok, err = (True, "") if not blocks else dq_core.check_yaml_text(full_text, scan_ids, top_key)
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
                    logger.info("deploy write: %s (%d scan block(s))", path, len(blocks))
                    st.success(done_msg)
                except OSError as e:
                    logger.exception("deploy write failed: %s", path)
                    st.error(f"Write failed: {e}")
