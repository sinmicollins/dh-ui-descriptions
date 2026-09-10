"""Table & Column Description POC — a small standalone app for testing the
description-generation pipeline in isolation.

Point it at one table, pick a sample size or model(s), and read the result.
This is intentionally minimal. The policy-tag firewall still applies. Tagged
columns are never sent to the LLM.

Extra context (description_poc_context.py) is best-effort: PII category names, data
domain, related tables, and owning team all include context.
"""
import datetime
import logging

import streamlit as st

import dq_core
import dq_generation as gen
import description_poc_context as ctx
from dq_ui import fetch_table_metadata, get_bq_client, setup_page, sidebar_fuelix, st_notify

dq_core.configure_logging()
logger = logging.getLogger(__name__)

setup_page("Table & Column Description POC")
st.caption(
    "Quick loop for testing description quality: pick a table and model(s), "
    "then generate. Results are shown here only — nothing is saved to the "
    "governance repo or the descriptions workbook.")

# --- Sidebar: connection + model ---------------------------------------------
st.sidebar.header("Connection Settings")
PROJECT_PRESETS = {
    "prd — cio-datahub-enterprise-pr-183a": "cio-datahub-enterprise-pr-183a",
    "dev — cio-datahub-enterprise-dv-e8ff": "cio-datahub-enterprise-dv-e8ff",
    "Other (type below)": "",
}
preset_choice = st.sidebar.selectbox("Environment", list(PROJECT_PRESETS))
source_project_id = st.sidebar.text_input(
    "GCP Project ID", value=PROJECT_PRESETS[preset_choice],
    help="Pick a preset above, or type any project ID you have BigQuery "
         "access to.").strip()
instance = "poc"  # only used as a scan-id/file-name prefix elsewhere; unused here
bq_location = st.sidebar.text_input("BigQuery Data Location",
                                    value="northamerica-northeast1")
source_dataset_id = st.sidebar.text_input(
    "Source Dataset (scanned data)", value="ent_actvn",
    help="Dataset containing the table to describe.")

fuelix_api_key, model_name = sidebar_fuelix(
    "Model (FuelIX)", "Chat models exposed by the FuelIX gateway.")

st.sidebar.header("Extra Context")
st.sidebar.caption("PII detection always runs — it's a safety signal, not "
                   "an optional quality experiment.")
use_domain = st.sidebar.checkbox("Look up data domain", value=True)
use_related = st.sidebar.checkbox("Detect table relationships (joins)", value=True)
use_team = st.sidebar.checkbox(
    "Look up owning team", value=False,
    help="Runs a heavy org-wide query (30 days of BigQuery job history + "
         "the HR team-member dimension) the first time it's needed, then "
         "caches it for the rest of this session. Needs access to "
         "cio-datahub-work-pr-0be526 — off by default since most people "
         "testing against their own project won't have that.")
st.sidebar.caption("The abbreviation table (bq_abbr_list) always runs too, "
                   "merged with the local abbreviations.csv.")
ABBREV_TABLE_REF = "cio-datahub-work-pr-0be526.datahub_operations.bq_abbr_list"

settings = gen.Settings(
    source_project_id=source_project_id, source_dataset_id=source_dataset_id,
    bq_location=bq_location, instance=instance)


# --- Dependency wiring --------------------------------------------------------------
def make_call_llm(model: str):
    def call_llm(system, user):
        return dq_core.call_fuelix(system, user, fuelix_api_key, model)
    return call_llm


def make_fetch_rows(n: int):
    """fetch_rows callable pinned to a specific sample size `n`."""
    def fetch_rows(table, meta, exclude):
        return gen.fetch_last_rows(
            get_bq_client(source_project_id, bq_location), source_project_id,
            source_dataset_id, table, meta, n=n, exclude=exclude,
            notify=st_notify)
    return fetch_rows


def fetch_bq_metadata(tables: tuple) -> dict:
    return gen.fetch_bq_metadata(get_bq_client(source_project_id, bq_location),
                                 source_project_id, source_dataset_id, tables)


@st.cache_data(ttl=600, show_spinner=False)
def fetch_profiling_stats(project: str, location: str, dataset: str, table: str) -> list:
    """Real profiling stats (row count, null %, approx-distinct count, top-5
    values) per simple-typed column, via one aggregate query — this is what
    stands in for a real DQ profiling scan in the full app. Grounding
    descriptions in actual cardinality is what stops the LLM from calling a
    3-value column "status" when it's actually something with 30 distinct
    values. STRUCT/ARRAY columns are skipped (aggregate functions below
    don't apply cleanly to nested types); very wide tables are capped at
    the first 150 simple columns to keep the query size sane — the FuelIX
    comparison spreadsheet already flags 100+ column tables as unreliable
    for the non-Gemini models anyway (see WIDE_TABLE_COLUMN_WARNING)."""
    from google.cloud import bigquery
    client = get_bq_client(project, location)
    p, d, t = (dq_core.bq_ident(project, "project"), dq_core.bq_ident(dataset, "dataset"),
              dq_core.bq_ident(table, "table"))
    schema_query = f"""
    SELECT column_name, data_type FROM `{p}.{d}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = @table ORDER BY ordinal_position
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("table", "STRING", t)])
    schema = [(row["column_name"], row["data_type"])
             for row in client.query(schema_query, job_config=job_config)]
    simple = [(c, dt) for c, dt in schema
             if not dt.startswith("STRUCT") and not dt.startswith("ARRAY")][:150]
    if not simple:
        return [{"table_name": table, "column_name": c, "column_type": dt} for c, dt in schema]
    aggs = ["COUNT(*) AS total_rows"]
    for c, _dt in simple:
        col = dq_core.bq_ident(c, "column")
        aggs += [f"COUNTIF(`{col}` IS NULL) AS `{col}__nulls`",
                f"APPROX_COUNT_DISTINCT(`{col}`) AS `{col}__distinct`",
                f"APPROX_TOP_COUNT(`{col}`, 5) AS `{col}__top5`"]
    stats_query = f"SELECT {', '.join(aggs)} FROM `{p}.{d}.{t}`"
    try:
        row = next(iter(client.query(stats_query)))
    except Exception as e:
        logger.info("profiling stats query failed for %s (%s) — falling back "
                   "to schema only", table, type(e).__name__)
        return [{"table_name": table, "column_name": c, "column_type": dt} for c, dt in schema]
    total = row["total_rows"] or 0
    out = []
    for c, dt in schema:
        entry = {"table_name": table, "column_name": c, "column_type": dt}
        if (c, dt) in simple:
            nulls, distinct, top5 = row[f"{c}__nulls"], row[f"{c}__distinct"], row[f"{c}__top5"]
            entry["percent_null"] = round(100 * nulls / total, 2) if total else None
            entry["distinct_count"] = distinct
            entry["top_values"] = [{"value": tv["value"], "count": tv["count"]} for tv in top5]
        out.append(entry)
    return out


@st.cache_data(ttl=86_400, show_spinner=False)
def _cached_team_ownership_raw(project: str, location: str) -> list:
    """Runs the heavy org-wide team-ownership query once per session (per
    the earlier decision) rather than per table/model. Cached across the
    whole app run, not just one table."""
    return ctx.fetch_team_ownership_raw(get_bq_client(project, location))


def gather_extra_context(table: str) -> dict:
    """{table: extra_context_str} for generate_descriptions(), built from
    whichever of the sidebar's Extra Context toggles are on (PII detection
    is not a toggle — it always runs, since it's a safety signal, not an
    optional quality experiment). Each source is independently best-effort
    (see description_poc_context.py) so one failing doesn't drop the others."""
    client = get_bq_client(source_project_id, bq_location)
    domain = ctx.resolve_domain(client, source_dataset_id) if use_domain else None
    pii = ctx.fetch_pii_categories(client, source_project_id, source_dataset_id, table)
    related = (ctx.fetch_related_tables(client, source_project_id, bq_location,
                                        source_dataset_id, table)
              if use_related else [])
    team = None
    if use_team:
        raw = _cached_team_ownership_raw(source_project_id, bq_location)
        team = ctx.team_for_table(raw, source_dataset_id, table)
    text = ctx.build_extra_context(domain=domain, pii=pii, related=related, team=team)
    return {table: text}


@st.cache_data(ttl=3_600, show_spinner=False)
def gather_abbreviations(table_ref: str) -> dict:
    """Merges the local abbreviations.csv with the BigQuery abbreviation
    table (if one is set in the sidebar) — BQ entries win on conflicts
    since they're the more likely to be current. Cached for an hour since
    this doesn't vary per table/model."""
    local = dq_core.load_abbreviations()
    if not table_ref:
        return local
    client = get_bq_client(source_project_id, bq_location)
    from_bq = ctx.fetch_abbreviations_from_bq(client, table_ref)
    return {**local, **from_bq}


_JUNK_PHRASES = ("column of the table", "the column name", "column name and",
                "this is a column", "this is the column", "this column is the",
                "the table name", "name of the table")


def is_useful_description(text: str, name: str) -> bool:
    """Rejects placeholder/junk existing descriptions before they're ever
    used. A description that's just "column", "field", or a restatement
    of the name itself provides no real information and shouldn't be treated as usable context, 
    let alone a fallback value. Checks both exact generic phrases and, separately,
    boilerplate phrase fragments — the "column of the table" style filler
    never mentions the actual column name, so a check that only strips
    the name and measures what's left (as an earlier version of this
    function did) misses it entirely; it needs an explicit phrase check."""
    if not text:
        return False
    t = text.strip().lower()
    if len(t) < 15:
        return False
    n = name.strip().lower().replace("_", " ")
    generic = {"column", "field", "this is a column", "this column", n,
              f"this is the {n} column", f"this is a column of the table"}
    if t in generic:
        return False
    if any(phrase in t for phrase in _JUNK_PHRASES):
        return False
    stripped = t.replace(n, "").strip(" .:-")
    return len(stripped) >= 10  # must say something beyond just the name


_REFORMULATE_SYSTEM = (
    "You write concise BigQuery metadata descriptions in the TELUS "
    "documentation standard: start with 'This column contains...' (or "
    "'This table contains...' for a table), present tense, no jargon, "
    "no restating specific sample values, spell out abbreviations. You "
    "are given an existing human-written description as your primary "
    "source of truth for this field — rewrite it into the standard "
    "format without inventing facts that aren't in it. If the existing "
    "text genuinely contains no usable information, respond with "
    "exactly: Unable to Generate Description")


def reformulate_from_existing(call_llm, name: str, kind: str, existing_text: str) -> str | None:
    """Turns a *good* existing description (already passed through
    is_useful_description) into an AI-written one in the standard format,
    grounded in that existing text rather than in sampled data — used
    when the AI couldn't produce anything from the data itself but a
    real existing description was available. This is a genuine LLM call,
    not a verbatim copy: the AI still writes the final text."""
    label = "TABLE" if kind == "table" else "COLUMN"
    user = f"{label} NAME: {name}\nEXISTING DESCRIPTION:\n{existing_text}\n\nRewrite this into the standard format."
    return gen._null_if_unable(call_llm(_REFORMULATE_SYSTEM, user))


def apply_existing_fallback(descriptions: dict, existing: dict, call_llm) -> set:
    """Where the AI came back null for a column (or the whole table),
    checks whether BigQuery already had a *useful* description for it
    (is_useful_description filters out placeholder junk like literally
    "column"). If so, makes one more LLM call (reformulate_from_existing)
    to have the AI write the final description grounded in that existing
    text, rather than pasting the old text in verbatim — the output is
    still AI-written, just from a different source of grounding than
    sampled data. If nothing useful exists either, the result stays
    null. The AI still attempts every column from data first regardless
    of what already exists — this only kicks in *after* that attempt
    fails; existing descriptions are never assumed good enough to skip
    generation outright (that's fill_only_missing's job elsewhere, and
    deliberately unused here since existing quality varies). Mutates
    `descriptions` in place. Returns the set of (table, column_or_None)
    pairs that got filled this way, so callers can report AI-from-data
    vs. AI-from-existing-description honestly instead of conflating
    them."""
    filled = set()
    for table, info in descriptions.items():
        cur = existing.get(table, {})
        if not info.get("table_description"):
            existing_td = cur.get("table_description")
            if is_useful_description(existing_td, table):
                result = reformulate_from_existing(call_llm, table, "table", existing_td)
                if result:
                    info["table_description"] = result
                    filled.add((table, None))
        cur_cols = cur.get("columns", {})
        for col, desc in info.get("columns", {}).items():
            if not desc:
                existing_cd = cur_cols.get(col)
                if is_useful_description(existing_cd, col):
                    result = reformulate_from_existing(call_llm, col, "column", existing_cd)
                    if result:
                        info["columns"][col] = result
                        filled.add((table, col))
    return filled


def run_generation(table: str, sample_size: int, model: str, tagged: dict,
                   existing: dict, extra_context: dict) -> dict:
    """One generate_descriptions call for `table` at a fixed sample size
    and model. Every call is recorded to st.session_state['run_log'] with
    its exact settings and output — this is the single choke point all 3
    modes call through, so logging lives here once rather than being
    duplicated per mode."""
    profiles = fetch_profiling_stats(source_project_id, bq_location,
                                     source_dataset_id, table)
    call_llm = make_call_llm(model)
    descriptions = gen.generate_descriptions(
        [table], profiles, tagged, settings, call_llm=call_llm,
        fetch_table_meta=fetch_table_metadata, fetch_rows=make_fetch_rows(sample_size),
        notify=st_notify, existing=existing, extra_context=extra_context,
        abbrev=gather_abbreviations(ABBREV_TABLE_REF))
    filled_from_existing = apply_existing_fallback(descriptions, existing, call_llm)
    log_run(table, sample_size, model, extra_context.get(table, ""), descriptions,
           existing, filled_from_existing)
    return descriptions


def log_run(table: str, sample_size: int, model: str, extra_context_text: str,
           descriptions: dict, existing: dict, filled_from_existing: set) -> None:
    """Appends one row to the in-session run log: every input setting that
    could plausibly affect the output (sample size, model, which context
    toggles were on, the exact assembled extra-context text) alongside the
    resulting table/column descriptions. Also captures whatever
    descriptions already existed in BigQuery for the same table/columns
    (already fed to the model as reference context in every run — this
    just makes that comparison visible instead of only implicit) so you
    can judge whether the AI matched, improved on, or diverged from
    descriptions a human already wrote. `descriptions` has already had
    apply_existing_fallback() applied by the time this runs, so
    num_columns_described/null reflect the *final* result including any
    AI-from-existing-description fills — num_columns_filled_from_existing
    tells you how many of those "described" columns were grounded in an
    existing BigQuery description rather than sampled data (still
    AI-written either way, just a different source), so the two don't
    get conflated. This is the raw
    data for the parameter-effect documentation — export it once you've
    run enough combinations and diff across rows."""
    rows = gen.flatten_descriptions(descriptions)
    table_desc = next((r["description"] for r in rows if r["column"] == "(table)"), None)
    col_descs = {r["column"]: r["description"] for r in rows if r["column"] != "(table)"}

    existing_info = existing.get(table, {})
    existing_table_desc = existing_info.get("table_description")
    existing_col_descs = existing_info.get("columns", {})
    had_existing = sum(1 for v in existing_col_descs.values() if v)
    filled_cols = sorted(c for t, c in filled_from_existing if t == table and c is not None)
    table_desc_filled = (table, None) in filled_from_existing

    entry = {
        "run_id": len(st.session_state.get("run_log", [])) + 1,
        "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
        "table": table,
        "sample_size": sample_size,
        "model": model,
        "pii_context": True,  # always on now, not a toggle — kept in the log for consistency
        "domain_context": use_domain,
        "related_tables_context": use_related,
        "team_context": use_team,
        "extra_context_text": extra_context_text,
        "table_description": table_desc,
        "table_description_filled_from_existing": table_desc_filled,
        "column_descriptions": col_descs,
        "num_columns_described": sum(1 for v in col_descs.values() if v),
        "num_columns_null": sum(1 for v in col_descs.values() if not v),
        "num_columns_filled_from_existing": len(filled_cols),
        "columns_filled_from_existing": filled_cols,
        "existing_table_description": existing_table_desc,
        "existing_column_descriptions": existing_col_descs,
        "num_columns_with_existing_desc": had_existing,
    }
    st.session_state.setdefault("run_log", []).append(entry)


def show_result(descriptions: dict, tagged: dict, sample_size: int, model: str) -> None:
    st.caption(f"Generated with `{model}`, sample size {sample_size} rows.")
    st.dataframe(gen.flatten_descriptions(descriptions), width="stretch")
    failed, excluded = gen.null_description_labels(descriptions, tagged)
    if failed:
        st.warning("No description could be generated (stored as null) for: "
                   + gen._label_list(failed))
    if excluded:
        st.info("Excluded from LLM payloads by policy tags (description left "
                "null): " + gen._label_list(excluded))


def wide_table_warning(table: str, models: list) -> None:
    """Per the FuelIX comparison spreadsheet: mistral-small-3.2-24b and
    gpt-4o-mini-ca-east both degrade badly on wide tables."""
    schema = fetch_profiling_stats(source_project_id, bq_location, source_dataset_id, table)
    n_cols = len(schema)
    risky = [m for m in models if m in ctx.WIDE_TABLE_UNRELIABLE_MODELS]
    if n_cols >= ctx.WIDE_TABLE_COLUMN_WARNING and risky:
        st.warning(f"`{table}` has {n_cols} columns. Per prior FuelIX model "
                  f"testing, {', '.join(risky)} became unreliable (missing "
                  "columns, or breaking down entirely) on tables this wide — "
                  "gemini-2.5-pro-ca held up better at this scale.")


# --- UI flow -------------------------------------------------------------------------
st.write("### Table")
table = st.text_input("Table name", value="bq_actvn_servreq_transaction").strip()

mode = st.radio("Mode", ["Single sample size", "Compare 10 vs 1000 rows",
                        "Compare models"])

if mode == "Single sample size":
    sample_size = st.radio("Rows sampled from the table", [10, 1000], index=1,
                           help="How many of the most recent rows to pull from "
                                "BigQuery and forward as evidence to the model.")
    if table:
        wide_table_warning(table, [model_name])
    if table and st.button("Generate Descriptions"):
        with st.spinner(f"Sampling {sample_size} rows and drafting descriptions..."):
            try:
                bq_meta = fetch_bq_metadata((table,))
                tagged = {t: info["tagged"] for t, info in bq_meta.items()}
                existing = gen.existing_descriptions_from(bq_meta)
                extra_context = gather_extra_context(table)
                st.session_state["poc_single"] = (
                    run_generation(table, sample_size, model_name, tagged,
                                  existing, extra_context),
                    tagged, sample_size, model_name)
            except dq_core.KNOWN_ERRORS as e:
                logger.exception("description POC generation failed for %s", table)
                st.error(f"Error generating descriptions: {e}")

    if "poc_single" in st.session_state:
        st.write("### Result")
        show_result(*st.session_state["poc_single"])

elif mode == "Compare 10 vs 1000 rows":
    st.caption("Runs the same table twice — once sampling 10 rows, once "
              "sampling 1000 — so you can compare description quality "
              "against sample size directly.")
    if table:
        wide_table_warning(table, [model_name])
    if table and st.button("Compare 10 vs 1000 Rows"):
        try:
            bq_meta = fetch_bq_metadata((table,))
            tagged = {t: info["tagged"] for t, info in bq_meta.items()}
            existing = gen.existing_descriptions_from(bq_meta)
            extra_context = gather_extra_context(table)
            with st.spinner("Sampling 10 rows and drafting descriptions..."):
                small = run_generation(table, 10, model_name, tagged, existing, extra_context)
            with st.spinner("Sampling 1000 rows and drafting descriptions..."):
                large = run_generation(table, 1000, model_name, tagged, existing, extra_context)
            st.session_state["poc_compare"] = (small, large, tagged, model_name)
        except dq_core.KNOWN_ERRORS as e:
            logger.exception("description POC comparison failed for %s", table)
            st.error(f"Error generating descriptions: {e}")

    if "poc_compare" in st.session_state:
        small, large, tagged, used_model = st.session_state["poc_compare"]
        st.write("### Result")
        col_10, col_1000 = st.columns(2)
        with col_10:
            st.write("#### 10 rows")
            show_result(small, tagged, 10, used_model)
        with col_1000:
            st.write("#### 1000 rows")
            show_result(large, tagged, 1000, used_model)

        st.write("#### Where the two runs disagree")
        rows_10 = {(r["table"], r["column"]): r["description"]
                  for r in gen.flatten_descriptions(small)}
        rows_1000 = {(r["table"], r["column"]): r["description"]
                    for r in gen.flatten_descriptions(large)}
        diffs = [{"table": t, "column": c, "10 rows": rows_10.get((t, c)),
                  "1000 rows": rows_1000.get((t, c))}
                 for t, c in rows_10.keys() | rows_1000.keys()
                 if rows_10.get((t, c)) != rows_1000.get((t, c))]
        if diffs:
            st.dataframe(diffs, width="stretch")
        else:
            st.success("Identical descriptions at both sample sizes for "
                      "every table/column.")

else:  # Compare models
    COMPARE_MODELS = ["gemini-2.5-pro-ca", "mistral-small-3.2-24b", "gpt-4o-mini-ca-east"]
    st.caption("Runs the same table through all 3 models FuelIX testing "
              "already covers: " + ", ".join(f"`{m}`" for m in COMPARE_MODELS))
    sample_size = st.radio("Rows sampled from the table", [10, 1000], index=1)
    if table:
        wide_table_warning(table, COMPARE_MODELS)
    if table and st.button("Compare Models"):
        try:
            bq_meta = fetch_bq_metadata((table,))
            tagged = {t: info["tagged"] for t, info in bq_meta.items()}
            existing = gen.existing_descriptions_from(bq_meta)
            extra_context = gather_extra_context(table)
            results = {}
            for m in COMPARE_MODELS:
                with st.spinner(f"Generating with {m}..."):
                    try:
                        results[m] = run_generation(table, sample_size, m, tagged,
                                                    existing, extra_context)
                    except dq_core.KNOWN_ERRORS as e:
                        logger.exception("model comparison failed for %s on %s", m, table)
                        st.error(f"{m} failed: {e}")
                        results[m] = None
            st.session_state["poc_model_compare"] = (results, tagged, sample_size)
        except dq_core.KNOWN_ERRORS as e:
            logger.exception("description POC model comparison failed for %s", table)
            st.error(f"Error generating descriptions: {e}")

    if "poc_model_compare" in st.session_state:
        results, tagged, used_size = st.session_state["poc_model_compare"]
        st.write("### Result")
        cols = st.columns(len([r for r in results if results[r] is not None]) or 1)
        i = 0
        for m, descriptions in results.items():
            if descriptions is None:
                continue
            with cols[i]:
                st.write(f"#### {m}")
                show_result(descriptions, tagged, used_size, m)
            i += 1


# --- Run log ------------------------------------------------------------------------
# Every generate_descriptions call (from any of the 3 modes) lands here via
# log_run(), so this fills up as you test different sample sizes, models,
# and context toggles — the raw data for documenting each parameter's
# effect. It's session-only (cleared on page reload), so export it before
# closing the tab if you want to keep it.
st.divider()
st.write("### Run Log")
run_log = st.session_state.get("run_log", [])
if not run_log:
    st.caption("Empty — each generation run (any mode above) gets logged "
              "here with its exact settings and output.")
else:
    summary = [{"run": r["run_id"], "table": r["table"], "sample_size": r["sample_size"],
               "model": r["model"], "pii": r["pii_context"], "domain": r["domain_context"],
               "related_tables": r["related_tables_context"], "team": r["team_context"],
               "cols_described": r["num_columns_described"], "cols_null": r["num_columns_null"],
               "cols_filled_from_existing": r.get("num_columns_filled_from_existing", 0),
               "cols_with_existing_desc": r.get("num_columns_with_existing_desc", 0),
               "table_description": (r["table_description"] or "")[:80]}
              for r in run_log]
    st.dataframe(summary, width="stretch")
    st.caption("cols_described includes cols_filled_from_existing — subtract "
              "the two to see how many were grounded in sampled data versus "
              "an existing description (both are AI-written either way).")

    with st.expander(f"Full detail for all {len(run_log)} runs (table + column "
                     "descriptions, exact extra-context text sent to the LLM)"):
        for r in run_log:
            st.write(f"**Run {r['run_id']}** — `{r['table']}`, {r['sample_size']} rows, "
                    f"`{r['model']}`, pii={r['pii_context']}, domain={r['domain_context']}, "
                    f"related={r['related_tables_context']}, team={r['team_context']}")
            table_desc_label = (" (AI-written from the existing BigQuery description — "
                                "sampled data alone wasn't enough)"
                                if r.get("table_description_filled_from_existing")
                                else " (AI-written from sampled data)")
            st.write(f"Table description{table_desc_label}: {r['table_description']}")
            existing_td = r.get("existing_table_description")
            if existing_td and not r.get("table_description_filled_from_existing"):
                st.write(f"Table description (already in BigQuery, for reference): {existing_td}")
            filled_cols = set(r.get("columns_filled_from_existing", []))
            if filled_cols:
                st.caption(f"AI-written from the existing BigQuery description "
                          f"(sampled data alone wasn't enough) for: "
                          f"{', '.join(sorted(filled_cols))}")
            st.write("Column descriptions (final — AI-written, from either "
                    "sampled data or an existing description as noted above):")
            st.json(r["column_descriptions"])
            existing_cols = {c: d for c, d in r.get("existing_column_descriptions", {}).items() if d}
            if existing_cols:
                st.write(f"Column descriptions already in BigQuery "
                        f"({len(existing_cols)} of {len(r['column_descriptions'])} columns):")
                st.json(existing_cols)
            if r["extra_context_text"].strip():
                st.caption("Extra context sent to the LLM:")
                st.text(r["extra_context_text"])
            st.divider()

    col_dl, col_clear = st.columns(2)
    with col_dl:
        import json as _json
        st.download_button("Download Run Log (JSON)",
                           data=_json.dumps(run_log, indent=2, default=str),
                           file_name="description_poc_run_log.json",
                           mime="application/json")
    with col_clear:
        if st.button("Clear Run Log"):
            st.session_state["run_log"] = []
            st.rerun()