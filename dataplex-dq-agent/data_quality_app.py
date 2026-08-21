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
"""Dataplex Auto-DQ (dataplex-dq) spec generator — UI shell.

The LLM drafts an action plan and per-column rules from BigQuery profiling
results; scan ids, data_source blocks and file placement are deterministic.
The pipeline lives in dq_generation (streamlit-free, dependency-injected),
shared plumbing in dq_core, and the shared UI/caching layer in dq_ui."""
import logging
import os
from datetime import date

import streamlit as st

import dq_core
import dq_generation as gen
from dq_ui import (
    fetch_table_metadata, get_bq_client, read_target, render_preview_and_deploy,
    select_tables, setup_page, show_target, sidebar_connection, sidebar_fuelix,
    sidebar_scan_settings, st_notify,
)

dq_core.configure_logging()
logger = logging.getLogger(__name__)

# --- Page + sidebar -----------------------------------------------------------
setup_page("Dataplex Auto-DQ Spec Generator")

environment, instance, source_project_id, repo_root = sidebar_connection()
project_id = st.sidebar.text_input("GCP Project ID (results table)",
                                   value="cio-datahub-governance-pr-8ba3")
dataset_id = st.sidebar.text_input(
    "Results Dataset ID", value="pr_datahub_01_ne1_edemm_dp_export_result",
    help="Dataset holding the scan RESULTS/profiling table.")
bq_location = st.sidebar.text_input("BigQuery Data Location", value="northamerica-northeast1")
profile_table_name = st.sidebar.text_input("Profiling Results Table",
                                           value="pr_datahub_01_ne1_governance_scan_results")
source_dataset_id = st.sidebar.text_input(
    "Source Dataset (scanned data)", value="ent_actvn",
    help="Dataset of the tables being scanned; drives the output file name "
         "and each scan's data_source.dataset_id.")

fuelix_api_key, model_name = sidebar_fuelix(
    "Model (FuelIX)", "Chat models exposed by the FuelIX gateway.")
gen_descriptions = st.sidebar.checkbox(
    "Table/column descriptions (review current, optionally generate via LLM)",
    value=False,
    help="Shows the descriptions currently stored on the BigQuery tables and asks "
         "whether to use them as-is (no LLM call), generate new ones, or fill only "
         "the gaps. Generating samples the last 1000 rows per table; per-column "
         "summaries and 20 rows go to the model (results saved to "
         "descriptions/*.xlsx) and are used alongside profiling stats for rule "
         "generation. When off, only Dataplex profiling statistics are used and no "
         "table data is sent to the LLM. Columns carrying a BigQuery policy tag "
         "are always excluded from every LLM payload.")
use_collibra, collibra_url, collibra_domain = False, "", ""
refresh_collibra = True  # no saved CSV yet -> first retrieval is live and saves it
if gen_descriptions:
    use_collibra = st.sidebar.checkbox(
        "Enrich with Collibra glossary", value=False,
        help="Imports Business Terms (acronym, full name, definition) via the "
             "Collibra API — auth key read from Secret Manager — and injects the "
             "terms matched to table/column name segments into the description "
             "prompts.")
    if use_collibra:
        if os.path.exists(gen.COLLIBRA_CSV):
            refresh_collibra = st.sidebar.checkbox(
                "Refresh glossary from Collibra (replaces the saved CSV)", value=False,
                help="Off: reuse the glossary saved in reference/collibra_glossary.csv "
                     "from the last retrieval — no Collibra calls. On: retrieve live "
                     "and replace the saved copy.")
            st.sidebar.caption(
                f"Saved glossary from {date.fromtimestamp(os.path.getmtime(gen.COLLIBRA_CSV))}"
                " — reused unless refreshed.")
        if refresh_collibra:
            collibra_url = st.sidebar.text_input("Collibra URL",
                                                 value="https://telus.collibra.com")
            collibra_domain = st.sidebar.text_input(
                "Collibra domain id (optional)", value="",
                help="Narrows the import to one glossary domain; blank = all "
                     "Business Terms org-wide.")
scan_cron, export_dataset, catalog_publishing_enabled = sidebar_scan_settings(
    "DQ Scan Settings", "0 7 * * *",
    "Header cron for a NEW dataset file; existing files keep theirs.")

governance_dir = os.path.join(repo_root, "edemm", environment, "governance")
job_prefix = instance
output_filename = f"{instance}_dqs_{source_dataset_id}.yaml"

settings = gen.Settings(
    source_project_id=source_project_id, source_dataset_id=source_dataset_id,
    bq_location=bq_location, instance=instance, governance_dir=governance_dir,
    fuelix_api_key=fuelix_api_key, model_name=model_name, project_id=project_id,
    dataset_id=dataset_id, profile_table_name=profile_table_name,
    gen_descriptions=gen_descriptions, use_collibra=use_collibra,
    collibra_url=collibra_url, collibra_domain=collibra_domain)


# --- Dependency wiring (lazy: clients are resolved inside the calls) ---------------
def call_llm(system, user):
    return dq_core.call_fuelix(system, user, fuelix_api_key, model_name)


def fetch_rows(table, meta, exclude):
    return gen.fetch_last_rows(
        get_bq_client(source_project_id, bq_location), source_project_id,
        source_dataset_id, table, meta, exclude=exclude, notify=st_notify)


@st.cache_data(ttl="15m", show_spinner=False)
def get_column_profiles(project: str, dataset: str, profile_table: str,
                        source_dataset: str, tables: tuple, location: str) -> list:
    """Cached wrapper around dq_generation.get_column_profiles; the client
    never enters the cache key."""
    return gen.get_column_profiles(get_bq_client(project, location), project,
                                   dataset, profile_table, source_dataset, tables)


@st.cache_data(ttl="15m", show_spinner=False)
def fetch_bq_metadata(project: str, dataset: str, location: str, tables: tuple) -> dict:
    """Policy-tag firewall lookup (dq_generation.fetch_bq_metadata): still
    raises on failure so callers fail closed — st.cache_data never caches
    exceptions, so a failed lookup is retried, not remembered."""
    return gen.fetch_bq_metadata(get_bq_client(project, location), project,
                                 dataset, tables)


def get_profiles_for_llm(tables: list) -> tuple[list, dict]:
    return gen.get_profiles_for_llm(tables, settings,
                                    fetch_profiles=get_column_profiles,
                                    fetch_bq_meta=fetch_bq_metadata,
                                    notify=st_notify)


def active_descriptions():
    return gen.active_descriptions(st.session_state.get("descriptions"),
                                   gen_descriptions)


# --- UI flow -------------------------------------------------------------------------
st.write("### Target Selection")
st.caption(f"Env **{environment}** / **{instance}**, source project `{source_project_id}`, "
           f"output `edemm/{environment}/governance/{output_filename}`.")
table_names = select_tables(source_project_id, source_dataset_id, bq_location, "fetched_tables")

if table_names and gen_descriptions:
    st.write("---")
    st.write("### Optional Step: Table & Column Descriptions")
    st.caption("Fetch the descriptions currently stored on the BigQuery tables, "
               "review them, then choose: keep them for the next steps, generate "
               f"new ones with the LLM (per-column summaries and up to {gen._EVIDENCE_ROWS} "
               f"of the last {gen.SAMPLE_ROW_COUNT} rows per table go to the model, held "
               "to the TELUS description standards), or a hybrid that only fills the "
               "gaps. The chosen set is saved to "
               f"`descriptions/{instance}_dq_descriptions_{source_dataset_id}.xlsx` "
               "and fed into the action plan and rule generation. Policy-tagged "
               "columns are excluded from every LLM payload, and descriptions the "
               "model cannot produce are stored as null.")
    if st.button("Fetch Current Descriptions"):
        with st.spinner("Reading current table metadata from BigQuery..."):
            try:
                bq_meta = fetch_bq_metadata(source_project_id, source_dataset_id,
                                            bq_location, tuple(table_names))
                st.session_state["existing_descriptions"] = gen.existing_descriptions_from(bq_meta)
                st.session_state["desc_policy_tags"] = {t: i["tagged"]
                                                        for t, i in bq_meta.items()}
            except dq_core.KNOWN_ERRORS as e:
                logger.exception("fetching current descriptions failed")
                st.error(f"Error fetching current descriptions: {e}")
    if "existing_descriptions" in st.session_state:
        existing = st.session_state["existing_descriptions"]
        st.write("#### Current BigQuery descriptions")
        st.dataframe(gen.flatten_descriptions(existing), width="stretch")
        col_descs = [d for i in existing.values() for d in i["columns"].values()]
        st.caption(f"{sum(1 for i in existing.values() if i['table_description'])} of "
                   f"{len(existing)} tables and {sum(1 for d in col_descs if d)} of "
                   f"{len(col_descs)} columns already have a description.")
        choice = st.radio(
            "How should descriptions for the next steps be produced?",
            ["Generate new descriptions with the LLM",
             "Use the current BigQuery descriptions (no LLM call)",
             "Hybrid: keep current descriptions, generate only the missing ones"])
        use_current = choice.startswith("Use")
        hybrid = choice.startswith("Hybrid")
        action_label = ("Use Current Descriptions" if use_current
                        else "Generate Missing Descriptions" if hybrid
                        else "Generate Descriptions")
        if st.button(action_label):
            try:
                if use_current:
                    st.session_state["descriptions"] = existing
                else:
                    with st.spinner("Sampling recent rows and drafting descriptions..."):
                        profiles, tagged = get_profiles_for_llm(table_names)
                        st.session_state["descriptions"] = gen.generate_descriptions(
                            table_names, profiles, tagged, settings,
                            call_llm=call_llm, fetch_table_meta=fetch_table_metadata,
                            fetch_rows=fetch_rows, notify=st_notify,
                            load_glossary=(
                                (lambda: gen.load_collibra(settings,
                                                           save_path=gen.COLLIBRA_CSV))
                                if use_collibra and refresh_collibra
                                else (lambda: gen.load_collibra_csv(gen.COLLIBRA_CSV))
                                if use_collibra else None),
                            existing=existing, fill_only_missing=hybrid)
                        st.session_state["desc_policy_tags"] = tagged
                st.session_state.pop("action_plan", None)
                st.session_state.pop("rules_by_table", None)
                xlsx_path = gen.descriptions_xlsx_path(settings)
                if gen.save_descriptions_xlsx(xlsx_path, st.session_state["descriptions"],
                                              dataset=source_dataset_id,
                                              model=model_name, notify=st_notify):
                    st.success(f"Descriptions saved to `{xlsx_path}`.")
            except dq_core.KNOWN_ERRORS as e:
                logger.exception("preparing descriptions failed")
                st.error(f"Error preparing descriptions: {e}")
    if "descriptions" in st.session_state:
        st.write("#### Descriptions for the next steps")
        st.dataframe(gen.flatten_descriptions(st.session_state["descriptions"]), width="stretch")
        failed, excluded = gen.null_description_labels(
            st.session_state["descriptions"], st.session_state.get("desc_policy_tags", {}))
        if failed:
            st.warning("No description could be generated (stored as null) for: "
                       + gen._label_list(failed))
        if excluded:
            st.info("Excluded from LLM payloads by policy tags (description left "
                    "null): " + gen._label_list(excluded))

if table_names:
    st.write("---")
    st.write("### Step 1: Formulate Action Plan")
    if st.button("Generate Action Plan"):
        with st.spinner("Retrieving profiling statistics and drafting plan..."):
            try:
                profiles = get_profiles_for_llm(table_names)[0]
                st.session_state["profiles"] = profiles
                st.session_state["table_order"] = table_names
                st.session_state.pop("rules_by_table", None)
                if not profiles:
                    st.warning("No profiling data found for the selected table(s).")
                else:
                    st.session_state["action_plan"] = gen.generate_action_plan(
                        profiles, call_llm=call_llm, notify=st_notify,
                        descriptions=active_descriptions())
            except dq_core.KNOWN_ERRORS as e:
                logger.exception("action plan generation failed")
                st.error(f"Error generating action plan: {e}")

if "action_plan" in st.session_state:
    st.markdown("#### Proposed Action Plan")
    st.markdown(st.session_state["action_plan"])
    st.write("---")
    st.write("### Step 2: Feedback & Rule Generation")
    hitl_feedback = st.text_area(
        "Apply Custom Business Knowledge / Override Rules",
        value="The plan looks good. Please proceed.",
        help="e.g. 'Remove volume check', 'Add Antarctica to geo_country allowed set'.")
    uploaded_file = st.file_uploader("Upload Rules Reference (PDF, CSV, Excel, TXT)",
                                     type=["pdf", "csv", "xlsx", "xls", "txt"])
    if st.button("Generate DQ Rules"):
        with st.spinner("Incorporating feedback and generating rules..."):
            try:
                st.session_state["rules_by_table"] = gen.generate_rules(
                    st.session_state["profiles"], st.session_state["action_plan"],
                    hitl_feedback, call_llm=call_llm, notify=st_notify,
                    uploaded_file=uploaded_file, descriptions=active_descriptions())
            except dq_core.KNOWN_ERRORS as e:
                logger.exception("rule generation failed")
                st.error(f"Error generating rules: {e}")

if "rules_by_table" in st.session_state:
    plan, blocks, ids = gen.build_scan_plan(
        st.session_state.get("table_order", table_names),
        st.session_state["rules_by_table"], settings)
    st.write("---")
    st.write("### Step 3: Validate & Deploy")
    st.caption(f"Scan-id rules: length <= {dq_core.MAX_SCAN_ID_LEN}, `^{job_prefix}_[a-z0-9_]+$`, "
               "no trailing separator, unique across the repo and this run.")
    st.dataframe(plan, width="stretch")
    if blocks:
        skipped = len(plan) - len(blocks)
        st.success(f"{len(blocks)} scan(s) ready." + (f" {skipped} skipped." if skipped else ""))
    else:
        st.warning("No scans passed validation — see the report above.")

    target_path = os.path.join(governance_dir, output_filename)
    file_exists, existing_text = read_target(target_path)
    show_target(environment, output_filename, target_path, file_exists, "dataplex-dq")
    render_preview_and_deploy(
        existing_text,
        dq_core.render_file_header("dataplex-dq", scan_cron,
                                   catalog_publishing_enabled, export_dataset),
        blocks, target_path, governance_dir, output_filename, file_exists,
        scan_ids=ids, top_key="dataplex-dq")
