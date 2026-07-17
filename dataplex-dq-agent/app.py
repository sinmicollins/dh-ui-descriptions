
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
import yaml
import streamlit as st
from google import genai
from google.genai import types
from google.cloud import bigquery
from google.auth import default

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
project_id = st.sidebar.text_input("GCP Project ID", value="demo1-311322")
location = st.sidebar.text_input("Dataplex Location", value="us-central1")
dataset_id = st.sidebar.text_input("BigQuery Dataset ID", value="mobile_data")
profile_table_name = st.sidebar.text_input("Profiling Results Table", value="data_profile_results")
model_name = st.sidebar.selectbox("Gemini Model", ["gemini-3.5-flash", "gemini-3.1-pro-preview"])

model_location = st.sidebar.text_input("Model Location", value="global")
output_filename = st.sidebar.text_input("Output Spec Filename", value="data-rules-aspects.yaml")


# Initialize BQ Client
try:
    bq_client = bigquery.Client(project=project_id)
except Exception as e:
    st.sidebar.error(f"BQ Client Init Failed: {e}")

# Initialize GenAI Client
try:
    genai_client = genai.Client(
        vertexai=True,
        project=project_id,
        location=model_location
    )
except Exception as e:
    st.sidebar.error(f"Gemini Client Init Failed: {e}")


# ---------------------------------------------------------
# Input Panel
# ---------------------------------------------------------
st.write("### Target Selection")
selection_mode = st.radio("Input Type", ["Single Table", "List of Tables", "Entire Dataset"])

table_names = []
if selection_mode == "Single Table":
    single_table = st.text_input("Table Name", value="work_orders")
    if single_table:
        table_names = [single_table.strip()]
elif selection_mode == "List of Tables":
    table_list_str = st.text_input("Table Names (comma separated)", value="work_orders")
    if table_list_str:
        table_names = [t.strip() for t in table_list_str.split(",") if t.strip()]
else:
    st.info(f"Rules aspects will be generated for ALL tables found in profiling results dataset '{dataset_id}'.")
    # Query distinct tables in dataset
    if st.button("Fetch Tables in Profile Results"):
        try:
            query = f"""
            SELECT DISTINCT data_source.table_id as table_name
            FROM `{project_id}.{dataset_id}.{profile_table_name}`
            WHERE data_source.dataset_id = @dataset_id
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("dataset_id", "STRING", dataset_id)
                ]
            )
            query_job = bq_client.query(query, job_config=job_config)
            table_names = [row["table_name"] for row in query_job]
            st.success(f"Found {len(table_names)} tables: {', '.join(table_names)}")
        except Exception as e:
            st.error(f"Error retrieving tables: {e}")

# ---------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------
def get_column_profiles(tables):
    """Retrieve latest profiling metrics for target tables from BigQuery"""
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
      WHERE data_source.dataset_id = @dataset_id
        AND data_source.table_id IN UNNEST(@table_names)
    )
    SELECT * FROM RankedProfiles WHERE rank = 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("dataset_id", "STRING", dataset_id),
            bigquery.ArrayQueryParameter("table_names", "STRING", tables),
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)
    results = []
    for row in query_job:
        # Format top_n records to list of dicts
        top_n_val = []
        if row["top_n"]:
            for item in row["top_n"]:
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

def generate_action_plan_via_gemini(profile_json):
    """Step 1: Ask Gemini to generate an Action Plan with justifications"""
    system_instruction = """
    You are an expert Google Cloud Knowledge Catalog engineer and data quality analyst.
    Your task is to analyze the provided BigQuery column profiling data and propose a step-by-step data quality rules Action Plan.
    
    In your plan:
    1. Identify which specific columns are good candidates for rules like nonNullExpectation, setExpectation, rangeExpectation, or rowConditionExpectation.
    2. Provide a clear justification for each rule suggestion based on the statistical metrics (e.g., "Recommend rangeExpectation for Antenna_Face because values range between 1 and 3 without outliers").
    3. Do NOT write any YAML code yet. Focus only on analysis and justification in clear markdown formatting.
    """
    
    prompt = f"""
    Analyze this column profiling data and draft a data quality action plan:
    {json.dumps(profile_json, indent=2)}
    """
    
    response = genai_client.models.generate_content(
        model=model_name,
        contents=prompt,
        config=types.GenerateContentConfig(
            system_instruction=system_instruction,
            temperature=0.1
        )
    )
    return response.text

def generate_yaml_spec_via_gemini(profile_json, action_plan, hitl_feedback, uploaded_file=None):
    """Step 2: Take action plan, profiling stats, user feedback, and optional multimodal reference file to output strict YAML spec"""
    system_instruction = """
    You are an expert Google Cloud Knowledge Catalog engineer.
    Your task is to generate Dataplex Catalog Data Rules Aspects configurations in YAML format.
    
    You are given:
    1. The raw column profiling data.
    2. The proposed Action Plan.
    3. Human-in-the-loop feedback/adjustments from the user. You MUST incorporate all user requests and feedback.
    
    You MUST output valid YAML matching the 'dataplex-types.global.data-rules' aspect schema.
    
    Rule Naming Constraints:
    - Rule names ('name') MUST contain ONLY alphabets, numbers, and/or hyphens (no underscores!). E.g., 'range-antenna-face'.
    
    Aspect key format:
    dataplex-types.global.data-rules@Schema.<COLUMN_NAME>:
      data:
        rules:
          - name: "<RULE_NAME>"
            type: "TEMPLATE_REFERENCE"
            templateReference:
              name: "projects/dataplex-templates/locations/global/entryGroups/rule-library/entries/<TEMPLATE_NAME>"
              values:
                <PARAMETER_NAME>:
                  value: "<PARAMETER_VALUE>"
            dimension: "<DIMENSION_NAME>"
            threshold: <THRESHOLD>
            
    For template target names, use:
    - Null Check: projects/dataplex-templates/locations/global/entryGroups/rule-library/entries/non_null_expectation
    - Range Check: projects/dataplex-templates/locations/global/entryGroups/rule-library/entries/range_expectation
    - Set/Whitelist: projects/dataplex-templates/locations/global/entryGroups/rule-library/entries/set_expectation
    - Custom row check: projects/dataplex-templates/locations/global/entryGroups/rule-library/entries/row_condition_expectation
    
    Add a comment (#) above each rule explaining the justification.
    Return ONLY the raw YAML configurations. Do not include any explanations, markdown code blocks (like ```yaml or ```), or introductory text.
    """
    
    contents = []
    
    # If the user uploaded a rules description file, pass it directly to Gemini as a Part!
    if uploaded_file is not None:
        file_bytes = uploaded_file.read()
        mime_type = uploaded_file.type
        contents.append(
            types.Part.from_bytes(
                data=file_bytes,
                mime_type=mime_type
            )
        )
        
    prompt = f"""
    Profiling Data:
    {json.dumps(profile_json, indent=2)}
    
    Proposed Action Plan:
    {action_plan}
    
    User Adjustments/HITL Feedback:
    {hitl_feedback}
    
    Refer to the uploaded document (if present) for additional rule requirements and logic instructions.
    
    Generate the final data quality aspects YAML configuration.
    """
    contents.append(prompt)
    
    response = genai_client.models.generate_content(
        model=model_name,
        contents=contents,
        config=types.GenerateContentConfig(
            system_instruction=system_instruction,
            temperature=0.1
        )
    )
    
    # Remove markdown code fences if generated
    cleaned = re.sub(r'```(yaml)?', '', response.text, flags=re.IGNORECASE).strip()
    return cleaned

# ---------------------------------------------------------
# Action / Execution Trigger
# ---------------------------------------------------------
if len(table_names) > 0:
    st.write("---")
    st.write("### Step 1: Formulate Action Plan")
    
    if st.button("Generate Action Plan"):
        with st.spinner("Retrieving profiling statistics and drafting plan..."):
            try:
                profiles = get_column_profiles(table_names)
                st.session_state["profiles"] = profiles
                
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
    st.write("### Step 2: Human-in-the-Loop Feedback & Specification Generation")
    
    # HITL Text Box
    hitl_feedback = st.text_area(
        "Apply Custom Business Knowledge / Override Rules",
        value="The plan looks good. Please proceed.",
        help="Provide directions to override rules (e.g. 'Remove rowCount check', 'Add Antarctica to geo_country allowed set')"
    )
    
    # File Uploader for rules logic
    uploaded_file = st.file_uploader(
        "Upload Rules Reference/Instructions (PDF, CSV, Excel, TXT)", 
        type=["pdf", "csv", "xlsx", "xls", "txt"]
    )
    
    if st.button("Generate Final Spec YAML"):
        with st.spinner("Incorporating feedback and writing YAML specification..."):
            try:
                final_yaml = generate_yaml_spec_via_gemini(
                    st.session_state["profiles"],
                    st.session_state["action_plan"],
                    hitl_feedback,
                    uploaded_file
                )
                st.session_state["generated_yaml"] = final_yaml
                st.success("Final YAML Spec generated successfully!")
            except Exception as e:
                st.error(f"Error generating final spec: {e}")

if "generated_yaml" in st.session_state:
    st.write("---")
    st.write(f"### Generated `{output_filename}` Output")
    st.text_area("YAML Code Preview", value=st.session_state["generated_yaml"], height=400)
    
    # Download Button
    st.download_button(
        label=f"Download {output_filename}",
        data=st.session_state["generated_yaml"],
        file_name=output_filename,
        mime="text/yaml"
    )
    
    # Deploy Actions
    st.write("### Deployment Options")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Save locally to workspace root"):
            try:
                # Portable path relative to this script directory
                workspace_file_path = os.path.join(os.path.dirname(__file__), f"../{output_filename}")
                with open(workspace_file_path, "w") as f:
                    f.write(st.session_state["generated_yaml"])
                st.success(f"File saved successfully to local path: {output_filename}")
            except Exception as e:
                st.error(f"Failed to write file: {e}")
                
    with col2:
        if st.button("Publish directly to Dataplex Catalog"):
            with st.spinner("Publishing aspects to Dataplex Catalog entries..."):
                try:
                    # Write temporarily to apply
                    temp_path = "temp-aspects-publish.yaml"
                    with open(temp_path, "w") as f:
                        f.write(st.session_state["generated_yaml"])
                    
                    # For each table, run update-aspects
                    for table in table_names:
                        entry_id = f"bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table}"
                        
                        import subprocess
                        cmd = [
                            "gcloud", "dataplex", "entries", "update-aspects",
                            entry_id,
                            f"--project={project_id}",
                            f"--location={location}",
                            "--entry-group=@bigquery",
                            f"--aspects={temp_path}"
                        ]
                        res = subprocess.run(cmd, capture_output=True, text=True)
                        if res.returncode == 0:
                            st.success(f"Published rules to entry: `{entry_id}`")
                        else:
                            st.error(f"Failed to publish to `{entry_id}`: {res.stderr}")
                            
                    # Clean up temp file
                    os.remove(temp_path)
                except Exception as e:
                    st.error(f"Publish failed: {e}")
