# Enterprise Data Quality & Governance with GenAI and Knowledge Catalog

This repository provides a GitOps-driven architecture for managing and deploying data quality rules at scale on Google Cloud Platform (GCP) using **Knowledge Catalog (formerly Dataplex)**, or **KC(f.k.a. Dataplex)**.

By leveraging metadata aspects bound directly to Catalog entry schemas instead of maintaining thousands of static rule files, this project delivers a highly scalable "Policy-as-Code" governance model.

---

## 📂 Repository Structure

```text
├── .github/
│   └── workflows/
│       ├── dataplex-dq.yml      # CI/CD: Deploys aspects & schedules daily quality scans
│       └── dataplex-profile.yml # CI/CD: Schedules daily data profiling scans
├── dataplex-dq-agent/           # Streamlit-based GenAI scan generator apps
│   ├── data_quality_app.py      # DQ scan (dataplex-dq) generator UI with Human-in-the-loop validation
│   ├── data_profiling_app.py    # Data profiling scan (dataplex-dp) generator UI
│   ├── dq_generation.py         # DQ pipeline: profiling, policy-tag firewall, descriptions, rule YAML (streamlit-free)
│   ├── dq_core.py               # Core: FuelIX client, BigQuery metadata, scan ids, YAML engine, DPS logic (streamlit-free)
│   ├── dq_ui.py                 # Streamlit layer: cached wrappers + shared page/sidebar/deploy blocks
│   ├── tests/                   # Pytest behavior suite (dependency-injected fakes, no cloud access needed)
│   ├── descriptions/            # Generated table/column description workbooks (data — do not delete)
│   ├── reference/               # Shared reference data
│   │   ├── abbreviations.csv        # Token abbreviations used to fit scan ids in 36 chars
│   │   └── collibra_glossary.csv    # Saved Collibra Business Terms (written on retrieval, reused until refreshed)
│   └── requirements.txt         # Pinned app dependencies (requirements-dev.txt adds pytest)
├── pulumi-approach/             # Infrastructure-as-Code deployment project
│   ├── __main__.py              # Main Pulumi script (loads rules spec dynamically)
│   ├── Pulumi.yaml
│   └── requirements.txt
├── data-profile-spec.yaml       # Knowledge Catalog data profiling configuration
├── data-quality-spec.yaml       # Knowledge Catalog data quality configuration
├── data-rules-aspects.yaml      # Central rules metadata spec (Single Source of Truth)
├── .gitignore
└── README.md
```

---

## 🚀 Workflows & Getting Started

### Workflow 1: Generate Rules Specs via the GenAI Agent
The **Dataplex Auto-DQ Spec Generator Agent** is a Streamlit app that reads historical column profiling metrics directly from BigQuery, uses an LLM via the **FuelIX gateway** (model picker in the sidebar; default `gemma-4-saif`) to propose rule configurations, accepts human feedback/spreadsheets, and generates your scan YAML.

#### Setup & Launch:
```bash
python3 -m venv .venv                        # at the repository root
source .venv/bin/activate                    # Windows: .venv\Scripts\activate
pip install -r dataplex-dq-agent/requirements.txt
cd dataplex-dq-agent
streamlit run data_quality_app.py     # DQ rules/scans
streamlit run data_profiling_app.py   # data profiling scans
```
* **Step 1: Generate Action Plan**: Queries BigQuery profiling tables and outputs suggested rules with clear statistical justifications.
* **Step 2: Human-in-the-Loop Feedback**: Review suggestions, upload logic sheets (PDF/CSV/Excel/TXT), type custom overrides, and generate the final scan YAML (`<instance>_dqs_<dataset>.yaml` / `<instance>_dps_<dataset>.yaml`), written into the governance folder of your **orchestrator repo** checkout (`<repo>/edemm/<env>/governance/` — set the repo root in the sidebar, or via the `DATAPLEX_REPO_ROOT` environment variable).

Diagnostics log to stderr; set `DQ_LOG_LEVEL=DEBUG` for verbose output.

#### Run the test suite (no GCP/FuelIX access required):
```bash
pip install -r dataplex-dq-agent/requirements-dev.txt
pytest dataplex-dq-agent/tests
```

---

### Workflow 2: Deploy Specs via Infrastructure-as-Code (Pulumi)
If you manage your GCP resources declaratively using Pulumi, you can deploy the generated rules aspects programmatically. The Pulumi program dynamically loads the central rules spec from the parent directory `data-rules-aspects.yaml` to ensure zero code duplication.

#### Setup & Deploy:
```bash
cd pulumi-approach
# Ensure virtualenv is active
source venv/bin/activate
pip install -r requirements.txt

# Initialize your stack under your personal namespace
pulumi stack init benjaminliyu-gmail-com/dev

# Set GCP context
pulumi config set gcp:project demo1-311322
pulumi config set gcp:region us-central1

# Preview and Deploy
pulumi preview
pulumi up
```

---

### Workflow 3: Deploy & Schedule Specs via CI/CD (GitHub Actions)
If you deploy via GitHub Actions, pushing changes to `data-rules-aspects.yaml` or `data-quality-spec.yaml` will trigger the automated pipeline:

1. **Deploy aspects**: Runs `gcloud dataplex entries update-aspects` to publish rule metadata directly to the BigQuery table's Catalog entry in KC(f.k.a. Dataplex).
2. **Schedule scans**: Creates or updates background data quality checks targeting the Catalog-based rules, configured to run on a daily recurring schedule using your authorized GCP service account.

Workflows can be found under [.github/workflows/](file:///.github/workflows/).

---

## 📄 License

Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
