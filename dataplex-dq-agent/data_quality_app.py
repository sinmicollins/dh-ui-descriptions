
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
"""Dataplex Auto-DQ (dataplex-dq) spec generator.

The LLM drafts an action plan and per-column rules from BigQuery profiling
results; scan ids, data_source blocks and file placement are deterministic.
Shared plumbing and the gated YAML validation/deploy path live in dq_common."""
import json
import os
import re

import streamlit as st
from google.cloud import bigquery

from dq_common import (
    MAX_SCAN_ID_LEN, call_fuelix, collect_repo_scan_ids,
    finite_float, get_bq_client, parse_llm_json, read_target,
    render_file_header, render_preview_and_deploy, resolve_scan_id,
    select_tables, setup_page, show_target, sidebar_connection, sidebar_fuelix,
    sidebar_scan_settings, validate_scan_id, yaml_quote,
)

# ---------------------------------------------------------
# Page + sidebar
# ---------------------------------------------------------
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
scan_cron, export_dataset, catalog_publishing_enabled = sidebar_scan_settings(
    "DQ Scan Settings", "0 7 * * *",
    "Header cron for a NEW dataset file; existing files keep theirs.")

governance_dir = os.path.join(repo_root, "edemm", environment, "governance")
job_prefix = instance
output_filename = f"{instance}_dqs_{source_dataset_id}.yaml"

# ---------------------------------------------------------
# Profiling
# ---------------------------------------------------------
@st.cache_data(ttl="15m", show_spinner=False)
def get_column_profiles(project: str, dataset: str, profile_table: str,
                        source_dataset: str, tables: tuple, location: str) -> list:
    """Latest profiling metrics per column for the target tables."""
    query = f"""
    WITH RankedProfiles AS (
      SELECT data_source.table_id AS table_name, column_name, column_type,
             column_mode, percent_null, percent_unique, min_value, max_value,
             average_value, standard_deviation, top_n,
             ROW_NUMBER() OVER (PARTITION BY data_source.table_id, column_name
                                ORDER BY job_start_time DESC) AS rank
      FROM `{project}.{dataset}.{profile_table}`
      WHERE data_source.dataset_id = @source_dataset_id
        AND data_source.table_id IN UNNEST(@table_names)
    )
    SELECT * FROM RankedProfiles WHERE rank = 1
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("source_dataset_id", "STRING", source_dataset),
        bigquery.ArrayQueryParameter("table_names", "STRING", list(tables)),
    ])
    results = []
    for row in get_bq_client(project, location).query(query, job_config=job_config):
        rec: dict[str, object] = {k: row[k] for k in (
            "table_name", "column_name", "column_type", "column_mode",
            "percent_null", "percent_unique", "min_value", "max_value",
            "average_value", "standard_deviation")}
        top_n = row["top_n"] if isinstance(row["top_n"], list) else []
        rec["top_n"] = [{"value": i.get("value"), "count": i.get("count"),
                         "percent": i.get("percent")} for i in top_n]
        results.append(rec)
    return results


# ---------------------------------------------------------
# YAML rendering (dataplex-dq scan format)
# ---------------------------------------------------------
_I_DASH, _I_KEY, _I_SUB = " " * 14, " " * 16, " " * 18
_EXPECTATIONS = {
    "non_null_expectation", "uniqueness_expectation", "range_expectation",
    "set_expectation", "regex_expectation", "table_condition_expectation",
}


def _num(value) -> str:
    """Bare number when finite (whole floats without decimal), else a quoted
    scalar — so string bounds like '2020-01-01' can't corrupt the document."""
    n = finite_float(value)
    if n is None:
        return yaml_quote(value)
    return str(int(n)) if n == int(n) else str(n)


def rule_is_valid(rule) -> bool:
    """Renderable = name + dimension + a KNOWN expectation type; unknown types
    would otherwise render as an expectation-less rule Dataplex rejects."""
    return (isinstance(rule, dict) and bool(rule.get("name"))
            and bool(rule.get("dimension"))
            and (rule.get("expectation") or {}).get("type") in _EXPECTATIONS)


def _sanitize_rule(rule):
    """Coerce one LLM rule toward validity: Dataplex name charset, float
    threshold (dropped if not coercible), stripped column/dimension, list-typed
    set values."""
    if not isinstance(rule, dict):
        return rule
    if rule.get("name"):
        rule["name"] = re.sub(r"[^a-z0-9-]+", "-", str(rule["name"]).lower()).strip("-")
    if rule.get("column") is not None:
        rule["column"] = str(rule["column"]).strip()
    if rule.get("dimension") is not None:
        rule["dimension"] = str(rule["dimension"]).strip().upper()
    if rule.get("threshold") is not None:
        rule["threshold"] = finite_float(str(rule["threshold"]).strip().rstrip("%"))
        if rule["threshold"] is None:
            del rule["threshold"]
    exp = rule.get("expectation")
    if isinstance(exp, dict) and exp.get("type") == "set_expectation":
        values = exp.get("values")
        exp["values"] = values if isinstance(values, list) else ([] if values is None else [values])
    return rule


def render_rule(rule: dict) -> list:
    exp = rule.get("expectation") or {}
    etype, column = exp.get("type"), rule.get("column")
    lines = ([_I_DASH + f"- column: {yaml_quote(column)}",
              _I_KEY + f"name: {yaml_quote(rule['name'])}"]
             if column else [_I_DASH + f"- name: {yaml_quote(rule['name'])}"])
    lines.append(_I_KEY + f"dimension: {yaml_quote(rule['dimension'])}")
    threshold = finite_float(rule.get("threshold"))
    if etype != "table_condition_expectation" and threshold is not None:
        lines.append(_I_KEY + f"threshold: {threshold}")
    if etype in ("non_null_expectation", "uniqueness_expectation"):
        lines.append(_I_KEY + f"{etype}: true")
    elif etype == "range_expectation":
        lines.append(_I_KEY + "range_expectation:")
        for key in ("min_value", "max_value"):
            if exp.get(key) is not None:
                lines.append(_I_SUB + f"{key}: {_num(exp[key])}")
    elif etype == "set_expectation":
        lines.append(_I_KEY + "set_expectation:")
        lines.append(_I_SUB + "values: ["
                     + ", ".join(yaml_quote(v) for v in exp.get("values") or []) + "]")
        if exp.get("ignore_null"):
            lines.append(_I_SUB + "ignore_null: true")
    elif etype == "regex_expectation":
        lines.append(_I_KEY + "regex_expectation:")
        lines.append(_I_SUB + f"regex: {yaml_quote(exp.get('regex', ''))}")
        if exp.get("ignore_null"):
            lines.append(_I_SUB + "ignore_null: true")
    else:  # table_condition_expectation
        lines.append(_I_KEY + "table_condition_expectation:")
        lines.append(_I_SUB + f"sql_expression: {yaml_quote(exp.get('sql_expression', ''))}")
    return lines


def render_scan_block(scan_id, project_id_, dataset_id_, table_id_, rules) -> str:
    lines = [
        f"        {scan_id}:",
        "          data_source:",
        f"            project_id: {yaml_quote(project_id_)}",
        f"            dataset_id: {yaml_quote(dataset_id_)}",
        f"            table_id: {yaml_quote(table_id_)}",
        "          data_quality_spec:",
        "            rules:",
    ]
    for rule in rules:
        lines.extend(render_rule(rule))
    return "\n".join(lines)


# ---------------------------------------------------------
# LLM steps
# ---------------------------------------------------------
_SYS_PLAN = """You are an expert Google Cloud Dataplex data quality engineer.
From the provided BigQuery column profiling data, draft a step-by-step Action Plan for a Dataplex DQ scan:
1. Identify columns suited to non_null_expectation (COMPLETENESS), uniqueness_expectation (UNIQUENESS), range/set/regex_expectation (VALIDITY), or table-level table_condition_expectation (VOLUME/FRESHNESS).
2. Justify each suggestion from the statistics (e.g. "range_expectation for antenna_face: values span 1-3 with no outliers"; "uniqueness_expectation for acct_id: percent_unique is 100%").
3. Do NOT write YAML/JSON yet — analysis and justification only, in clear markdown."""

_SYS_RULES = """You are an expert Google Cloud Dataplex data quality engineer.
From column profiling data, an action plan and user feedback, return ONLY one JSON object (no markdown, no prose) of this shape:
{"tables": {"<table_name>": {"rules": [{
  "column": "<column_name>",   // omit for table-level rules
  "name": "<rule-name>",       // lowercase letters, digits and hyphens ONLY
  "dimension": "COMPLETENESS|UNIQUENESS|VALIDITY|VOLUME|FRESHNESS",
  "threshold": 0.99,           // float; omit for table-level rules
  "expectation": {...}}]}}}    // exactly one of:
- {"type":"non_null_expectation"}                                        COMPLETENESS, threshold 0.99
- {"type":"uniqueness_expectation"}                                      UNIQUENESS, threshold 1.0
- {"type":"range_expectation","min_value":0,"max_value":9}               VALIDITY, threshold 0.99 (min and/or max)
- {"type":"set_expectation","values":["A","B"],"ignore_null":true}       VALIDITY, threshold 0.99
- {"type":"regex_expectation","regex":"^.{9}$","ignore_null":true}       VALIDITY, threshold 0.99
- {"type":"table_condition_expectation","sql_expression":"COUNT(*) > 0"} VOLUME/FRESHNESS; NO column, NO threshold
Use table_name keys exactly as given. Only reference columns present in the profiling data. Incorporate all user feedback."""


def generate_action_plan(profile_json):
    prompt = ("Analyze this column profiling data and draft a data quality action plan:\n"
              + json.dumps(profile_json, separators=(",", ":"), default=str))
    return call_fuelix(_SYS_PLAN, prompt, fuelix_api_key, model_name)


def _read_upload(uploaded_file) -> str:
    """Best-effort text of an uploaded reference file; '' if binary."""
    if uploaded_file is None:
        return ""
    raw = uploaded_file.read()
    for enc in ("utf-8", "latin-1"):
        try:
            text = raw.decode(enc)
            if text.strip():
                return text
        except (UnicodeDecodeError, AttributeError):
            pass
    st.warning(f"'{getattr(uploaded_file, 'name', 'file')}' is not readable as text "
               "(binary formats are unsupported) — paste key requirements into the feedback box.")
    return ""


def generate_rules(profile_json, action_plan, hitl_feedback, uploaded_file=None):
    """{table_name: [rule_dict, ...]} from the model; sanitized per rule."""
    uploaded_text = _read_upload(uploaded_file)
    prompt = (f"Profiling Data:\n{json.dumps(profile_json, separators=(',', ':'), default=str)}\n\n"
              f"Proposed Action Plan:\n{action_plan}\n\n"
              f"User Adjustments/HITL Feedback:\n{hitl_feedback}\n\n"
              f"Reference document contents (if any):\n{uploaded_text or '(none provided)'}\n\n"
              "Return the JSON object of rules per table.")
    raw_text = call_fuelix(_SYS_RULES, prompt, fuelix_api_key, model_name)
    data = parse_llm_json(raw_text)
    if data is None:
        raise ValueError("Model did not return JSON. Raw response:\n" + raw_text)
    tables = data.get("tables", data) if isinstance(data, dict) else {}
    out = {}
    for tbl, spec in tables.items():
        rules = (spec.get("rules", []) if isinstance(spec, dict)
                 else spec if isinstance(spec, list) else [])
        out[tbl] = [_sanitize_rule(r) for r in rules]
    return out


def build_scan_plan(table_order, rules_by_table):
    """(plan, blocks, scan_ids): report rows plus rendered blocks and their ids
    for tables that passed scan-id validation and have valid rules."""
    forbidden = collect_repo_scan_ids(governance_dir, job_prefix)
    plan, blocks, ids = [], [], []
    for table in table_order:
        rules = [r for r in (rules_by_table.get(table) or []) if rule_is_valid(r)]
        scan_id = resolve_scan_id(job_prefix, source_dataset_id, table,
                                  forbidden | set(ids))
        ok, reason = validate_scan_id(scan_id, job_prefix, forbidden | set(ids))
        if ok and not rules:
            ok, reason = False, "no valid rules generated"
        if ok:
            blocks.append(render_scan_block(scan_id, source_project_id,
                                            source_dataset_id, table, rules))
            ids.append(scan_id)
        plan.append({"table": table, "scan_id": scan_id, "rules": len(rules),
                     "status": "ok" if ok else f"skip: {reason}"})
    return plan, blocks, ids


# ---------------------------------------------------------
# UI flow
# ---------------------------------------------------------
st.write("### Target Selection")
st.caption(f"Env **{environment}** / **{instance}**, source project `{source_project_id}`, "
           f"output `edemm/{environment}/governance/{output_filename}`.")
table_names = select_tables(source_project_id, source_dataset_id, bq_location, "fetched_tables")

if table_names:
    st.write("---")
    st.write("### Step 1: Formulate Action Plan")
    if st.button("Generate Action Plan"):
        with st.spinner("Retrieving profiling statistics and drafting plan..."):
            try:
                profiles = get_column_profiles(project_id, dataset_id, profile_table_name,
                                               source_dataset_id, tuple(table_names), bq_location)
                st.session_state["profiles"] = profiles
                st.session_state["table_order"] = table_names
                st.session_state.pop("rules_by_table", None)
                if not profiles:
                    st.warning("No profiling data found for the selected table(s).")
                else:
                    st.session_state["action_plan"] = generate_action_plan(profiles)
            except Exception as e:
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
                st.session_state["rules_by_table"] = generate_rules(
                    st.session_state["profiles"], st.session_state["action_plan"],
                    hitl_feedback, uploaded_file)
            except Exception as e:
                st.error(f"Error generating rules: {e}")

if "rules_by_table" in st.session_state:
    plan, blocks, ids = build_scan_plan(
        st.session_state.get("table_order", table_names), st.session_state["rules_by_table"])
    st.write("---")
    st.write("### Step 3: Validate & Deploy")
    st.caption(f"Scan-id rules: length <= {MAX_SCAN_ID_LEN}, `^{job_prefix}_[a-z0-9_]+$`, "
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
        render_file_header("dataplex-dq", scan_cron, catalog_publishing_enabled, export_dataset),
        blocks, target_path, governance_dir, output_filename, file_exists,
        scan_ids=ids, top_key="dataplex-dq")