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
"""Dataplex Data Profiling Scan (dataplex-dp) spec generator.

Sources table metadata from BigQuery INFORMATION_SCHEMA (no Sheets access) and
mirrors the CLI's field-resolution cascade. Shared plumbing and the gated YAML
validation/deploy path live in dq_common."""
import glob
import json
import os
import re

import pandas as pd
import streamlit as st
from google.cloud import bigquery

from dq_common import (
    MAX_SCAN_ID_LEN, build_scan_id, call_fuelix, collect_repo_scan_ids,
    get_bq_client, load_yaml, parse_llm_json, read_scans, read_target,
    render_file_header, render_preview_and_deploy, resolve_scan_id,
    select_tables, setup_page, show_target, sidebar_connection, sidebar_fuelix,
    sidebar_scan_settings, validate_scan_id, yaml_quote,
)

# Audit-column candidates, highest priority first (matched case-insensitively
# against TIMESTAMP/DATETIME/DATE columns).
AUDIT_PRIORITY = [
    "last_updt_ts", "last_update_ts", "src_last_updt_ts", "last_updt_tms",
    "last_upd_ts", "updt_ts", "update_ts", "last_updt_dt",
    "create_ts", "created_ts", "creation_ts", "create_dt", "__source_ts_ms",
]
AUDIT_REGEX = re.compile(r"(updt|update|audit|source_ts)", re.IGNORECASE)
# `field` is typed by hand and lands inside a SQL row_filter, so it is held to
# the BigQuery column-identifier charset before being rendered.
FIELD_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# ---------------------------------------------------------
# Page + sidebar
# ---------------------------------------------------------
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

# ---------------------------------------------------------
# Metadata + field resolution
# ---------------------------------------------------------
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


# ---------------------------------------------------------
# Repo inventory (DPS-specific)
# ---------------------------------------------------------
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
    try:
        document = load_yaml(yaml_path)
        if not isinstance(document, dict):
            return ""
        governance = document.get("governance")
        if not isinstance(governance, dict):
            return ""
        consumer_governance = governance.get("consumer-governance")
        if not isinstance(consumer_governance, dict):
            return ""
        dataplex_dp = consumer_governance.get("dataplex-dp")
        if not isinstance(dataplex_dp, dict):
            return ""
        execution_spec = dataplex_dp.get("execution_spec")
        if not isinstance(execution_spec, dict):
            return ""
        trigger = execution_spec.get("trigger")
        if not isinstance(trigger, dict):
            return ""
        schedule = trigger.get("schedule")
        if not isinstance(schedule, dict):
            return ""
        return str(schedule.get("cron") or "").strip()
    except (KeyError, TypeError):
        return ""


# ---------------------------------------------------------
# YAML block rendering
# ---------------------------------------------------------
def render_scan_block(job_id, project_id_, dataset_id_, table_id_, field_column, cron) -> str:
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


# ---------------------------------------------------------
# LLM rename fallback (validated outside the model)
# ---------------------------------------------------------
def llm_rename_scan_ids(failing: list, forbidden: set) -> dict:
    """{table: new_id}, keeping only suggestions that pass validate_scan_id."""
    system_instruction = (
        "You rename BigQuery Dataplex scan job ids that violate naming rules. "
        "Return ONLY a JSON object mapping table_name to a new job id. Every id must:\n"
        f"1. be <= {MAX_SCAN_ID_LEN} characters;\n"
        f"2. match ^{job_prefix}_[a-z0-9_]+$ (start with the literal prefix '{job_prefix}_');\n"
        "3. not end with an underscore or hyphen;\n"
        "4. not collide with the forbidden ids nor with each other.\n"
        "Abbreviate tokens of the dataset/table name rather than inventing unrelated words.")
    prompt = json.dumps({"dataset": source_dataset_id, "rows": failing,
                         "forbidden_ids": sorted(forbidden)}, separators=(",", ":"))
    data = parse_llm_json(call_fuelix(system_instruction, prompt, fuelix_api_key, model_name))
    if not isinstance(data, dict):
        return {}
    accepted, taken = {}, set(forbidden)
    for row in failing:
        cand = str(data.get(row["table"], "")).strip()
        if validate_scan_id(cand, job_prefix, taken)[0]:
            accepted[row["table"]] = cand
            taken.add(cand)
    return accepted


# ---------------------------------------------------------
# UI flow
# ---------------------------------------------------------
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
            existing_tables = load_repo_dps_tables(governance_dir, job_prefix)
            forbidden = collect_repo_scan_ids(governance_dir, job_prefix)
            plan, assigned, llm_candidates = [], set(), []
            for table in table_names:
                field, source = resolve_field(meta_by_table[table])
                repo_key = (source_project_id, source_dataset_id, table)
                if repo_key in existing_tables:
                    plan.append({"table": table, "scan_id": existing_tables[repo_key],
                                 "field": field, "field_source": source,
                                 "status": "skip: profiling scan already exists",
                                 "locked": True})
                    continue
                scan_id = build_scan_id(job_prefix, source_dataset_id, table)
                ok, reason = validate_scan_id(scan_id, job_prefix, forbidden | assigned)
                if ok:
                    assigned.add(scan_id)
                else:
                    llm_candidates.append({"table": table, "candidate": scan_id,
                                           "reason": reason})
                plan.append({"table": table, "scan_id": scan_id,
                             "field": field, "field_source": source,
                             "status": (("ok" if field else "needs field — type one below")
                                        if ok else f"skip: {reason}"),
                             "locked": False})
            if llm_candidates and use_llm_rename and fuelix_api_key:
                with st.spinner(f"Renaming {len(llm_candidates)} scan id(s) via {model_name}..."):
                    try:
                        renames = llm_rename_scan_ids(llm_candidates, forbidden | assigned)
                    except Exception as e:
                        renames = {}
                        st.warning(f"LLM rename fallback failed ({e}); "
                                   "falling back to deterministic ids.")
                for row in plan:
                    new_id = renames.get(row["table"])
                    if new_id:
                        row["scan_id"], row["status"] = new_id, (
                            "ok (LLM-renamed)" if row["field"] else "needs field — type one below")
                        assigned.add(new_id)
            # Deterministic retry: any id still invalid after the optional LLM
            # rename is truncated/suffixed until valid and unique — never skipped.
            unresolved = {c["table"] for c in llm_candidates}
            for row in plan:
                if row["table"] in unresolved and row["status"].startswith("skip:"):
                    new_id = resolve_scan_id(job_prefix, source_dataset_id,
                                             row["table"], forbidden | assigned)
                    row["scan_id"], row["status"] = new_id, (
                        "ok (auto-renamed)" if row["field"] else "needs field — type one below")
                    assigned.add(new_id)
            st.session_state["dps_plan"] = plan
        except Exception as e:
            st.error(f"Error analyzing tables: {e}")

if "dps_plan" in st.session_state:
    plan = st.session_state["dps_plan"]
    st.write("---")
    st.write("### Step 2: Review Plan (edit `field` to override)")
    st.caption(f"Scan-id rules: length <= {MAX_SCAN_ID_LEN}, `^{job_prefix}_[a-z0-9_]+$`, "
               "no trailing separator, unique across the repo and this run. `field` drives "
               "execution_spec.field and the daily incremental row_filter.")
    edited = st.data_editor(
        pd.DataFrame(plan)[["table", "scan_id", "field", "field_source", "status"]],
        width="stretch", hide_index=True, key="dps_editor",
        disabled=["table", "scan_id", "field_source", "status"])

    target_path = os.path.join(governance_dir, output_filename)
    file_exists, existing_text = read_target(target_path)

    # Additive-only: an existing file's cron is reused for appended scans.
    effective_cron = scan_cron
    if file_exists:
        existing_cron = read_existing_cron(target_path)
        if existing_cron:
            effective_cron = existing_cron
            if existing_cron != scan_cron:
                st.info(f"Existing file cron `{existing_cron}` reused for appended scans.")

    blocks, scan_ids, skipped = [], [], []
    for orig, row in zip(plan, edited.to_dict("records")):
        field = str(row.get("field") or "").strip()
        if orig["locked"] or orig["status"].startswith("skip:"):
            skipped.append((orig["table"], orig["status"]))
        elif not field:
            skipped.append((orig["table"], "no field resolved/entered"))
        elif not FIELD_RE.match(field):
            skipped.append((orig["table"], f"field {field!r} is not a valid column name"))
        else:
            blocks.append(render_scan_block(orig["scan_id"], source_project_id,
                                            source_dataset_id, orig["table"],
                                            field, effective_cron))
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
        render_file_header("dataplex-dp", effective_cron, catalog_publishing_enabled, export_dataset),
        blocks, target_path, governance_dir, output_filename, file_exists,
        scan_ids=scan_ids, top_key="dataplex-dp")