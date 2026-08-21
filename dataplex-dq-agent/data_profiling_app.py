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
"""Dataplex Data Profiling Scan (dataplex-dp) spec generator — UI shell.

Sources table metadata from BigQuery INFORMATION_SCHEMA (no Sheets access) and
mirrors the CLI's field-resolution cascade. The DPS logic lives in dq_core
(streamlit-free, dependency-injected); the shared UI/caching layer in dq_ui."""
import logging
import os
import re

import pandas as pd
import streamlit as st

import dq_core
from dq_ui import (
    fetch_table_metadata, read_target, render_preview_and_deploy, select_tables,
    setup_page, show_target, sidebar_connection, sidebar_fuelix,
    sidebar_scan_settings,
)

dq_core.configure_logging()
logger = logging.getLogger(__name__)

# `field` is typed by hand and lands inside a SQL row_filter, so it is held to
# the BigQuery column-identifier charset before being rendered.
FIELD_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
NEEDS_FIELD = "needs field — type one below"

# --- Page + sidebar -----------------------------------------------------------
setup_page("Dataplex Data Profiling Scan (DPS) Generator")

environment, instance, source_project_id, repo_root = sidebar_connection()
bq_location = st.sidebar.text_input("BigQuery Data Location", value="northamerica-northeast1")
source_dataset_id = st.sidebar.text_input(
    "Source Dataset (scanned data)", value="ent_cust_cust",
    help="Dataset of the tables to profile; drives the output file name "
         "and each scan's data_source.dataset_id.")

fuelix_api_key, model_name = sidebar_fuelix(
    "Model (FuelIX, id-rename fallback)",
    "Used only when a scan id cannot be abbreviated under the length limit.")
use_llm_rename = st.sidebar.checkbox(
    "LLM rename fallback for oversized ids", value=True,
    help="Every suggestion is re-validated in Python; the model never gets the final word.")

scan_cron, export_dataset, catalog_publishing_enabled = sidebar_scan_settings(
    "DPS Scan Settings", "0 8 * * 0",
    "Cron for a NEW dataset file (header + each scan); an existing file's cron is reused.")

governance_dir = os.path.join(repo_root, "edemm", environment, "governance")
job_prefix = instance
output_filename = f"{instance}_dps_{source_dataset_id}.yaml"


def call_llm(system, user):
    return dq_core.call_fuelix(system, user, fuelix_api_key, model_name)


# --- UI flow ----------------------------------------------------------------------------
st.write("### Target Selection")
st.caption(f"Env **{environment}** / **{instance}**, source project `{source_project_id}`, "
           f"output `edemm/{environment}/governance/{output_filename}`.")
table_names = select_tables(source_project_id, source_dataset_id, bq_location, "dps_fetched_tables")

if table_names:
    st.write("---")
    st.write("### Step 1: Analyze Tables")
    st.caption("Resolves the incremental `field` per table from INFORMATION_SCHEMA "
               "(partition column first, then audit columns — same cascade as the CLI).")
    if st.button("Analyze Tables"):
        try:
            with st.spinner("Fetching INFORMATION_SCHEMA metadata..."):
                meta_by_table = fetch_table_metadata(
                    source_project_id, source_dataset_id, bq_location, tuple(table_names))
            existing_tables = dq_core.load_repo_dps_tables(governance_dir, job_prefix)
            forbidden = dq_core.collect_repo_scan_ids(governance_dir, job_prefix)
            plan, assigned, llm_candidates = [], set(), []
            for table in table_names:
                field, source = dq_core.resolve_field(meta_by_table[table])
                repo_key = (source_project_id, source_dataset_id, table)
                if repo_key in existing_tables:
                    plan.append({"table": table, "scan_id": existing_tables[repo_key],
                                 "field": field, "field_source": source,
                                 "status": "skip: profiling scan already exists",
                                 "locked": True})
                    continue
                scan_id = dq_core.build_scan_id(job_prefix, source_dataset_id, table)
                ok, reason = dq_core.validate_scan_id(scan_id, job_prefix, forbidden | assigned)
                if ok:
                    assigned.add(scan_id)
                else:
                    llm_candidates.append({"table": table, "candidate": scan_id,
                                           "reason": reason})
                plan.append({"table": table, "scan_id": scan_id,
                             "field": field, "field_source": source,
                             "status": (("ok" if field else NEEDS_FIELD)
                                        if ok else f"skip: {reason}"),
                             "locked": False})
            if llm_candidates and use_llm_rename and fuelix_api_key:
                with st.spinner(f"Renaming {len(llm_candidates)} scan id(s) via {model_name}..."):
                    try:
                        renames = dq_core.llm_rename_scan_ids(
                            llm_candidates, forbidden | assigned, prefix=job_prefix,
                            dataset=source_dataset_id, call_llm=call_llm)
                    except dq_core.KNOWN_ERRORS as e:
                        renames = {}
                        logger.warning("LLM rename fallback failed: %s", e)
                        st.warning(f"LLM rename fallback failed ({e}); "
                                   "falling back to deterministic ids.")
                for row in plan:
                    new_id = renames.get(row["table"])
                    if new_id:
                        row["scan_id"], row["status"] = new_id, (
                            "ok (LLM-renamed)" if row["field"] else NEEDS_FIELD)
                        assigned.add(new_id)
            # Deterministic retry: any id still invalid after the optional LLM
            # rename is truncated/suffixed until valid and unique — never skipped.
            unresolved = {c["table"] for c in llm_candidates}
            for row in plan:
                if row["table"] in unresolved and row["status"].startswith("skip:"):
                    new_id = dq_core.resolve_scan_id(job_prefix, source_dataset_id,
                                                     row["table"], forbidden | assigned)
                    row["scan_id"], row["status"] = new_id, (
                        "ok (auto-renamed)" if row["field"] else NEEDS_FIELD)
                    assigned.add(new_id)
            st.session_state["dps_plan"] = plan
        except dq_core.KNOWN_ERRORS as e:
            logger.exception("table analysis failed")
            st.error(f"Error analyzing tables: {e}")

if "dps_plan" in st.session_state:
    plan = st.session_state["dps_plan"]
    st.write("---")
    st.write("### Step 2: Review Plan (edit `field` to override)")
    st.caption(f"Scan-id rules: length <= {dq_core.MAX_SCAN_ID_LEN}, `^{job_prefix}_[a-z0-9_]+$`, "
               "no trailing separator, unique across the repo and this run. `field` drives "
               "execution_spec.field and the daily incremental row_filter.")
    edited = st.data_editor(
        pd.DataFrame(plan)[["table", "scan_id", "field", "field_source", "status"]],
        width="stretch", hide_index=True, key="dps_editor",
        disabled=["table", "scan_id", "field_source", "status"])

    target_path = os.path.join(governance_dir, output_filename)
    file_exists, existing_text = read_target(target_path)

    # Additive-only: an existing file's cron is reused for appended scans.
    effective_cron = (dq_core.read_existing_cron(target_path) if file_exists else "") or scan_cron
    if file_exists and effective_cron != scan_cron:
        st.info(f"Existing file cron `{effective_cron}` reused for appended scans.")

    blocks, scan_ids, skipped = [], [], []
    # pd.DataFrame(...) normalizes data_editor's loosely-typed return (cheap
    # under copy-on-write) so to_dict(orient=...) type-checks.
    for orig, row in zip(plan, pd.DataFrame(edited).to_dict(orient="records")):
        field = str(row.get("field") or "").strip()
        if orig["locked"] or orig["status"].startswith("skip:"):
            skipped.append((orig["table"], orig["status"]))
        elif not field:
            skipped.append((orig["table"], "no field resolved/entered"))
        elif not FIELD_RE.match(field):
            skipped.append((orig["table"], f"field {field!r} is not a valid column name"))
        else:
            blocks.append(dq_core.render_dps_scan_block(
                orig["scan_id"], source_project_id, source_dataset_id,
                orig["table"], field, effective_cron))
            scan_ids.append(orig["scan_id"])

    if blocks:
        st.success(f"{len(blocks)} scan(s) ready."
                   + (f" {len(skipped)} skipped." if skipped else ""))
    else:
        st.warning("No scans ready — resolve the skipped/needs-field rows above.")
    if skipped:
        with st.expander(f"Skipped tables ({len(skipped)})"):
            st.dataframe([{"table": t, "reason": r} for t, r in skipped], width="stretch")

    show_target(environment, output_filename, target_path, file_exists, "dataplex-dp")
    render_preview_and_deploy(
        existing_text,
        dq_core.render_file_header("dataplex-dp", effective_cron,
                                   catalog_publishing_enabled, export_dataset),
        blocks, target_path, governance_dir, output_filename, file_exists,
        scan_ids=scan_ids, top_key="dataplex-dp")
