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
├── dataplex-dq-agent/           # Streamlit-based GenAI rule generator app
│   ├── app.py                   # Main agent UI with Human-in-the-loop validation
│   └── requirements.txt         # App package dependencies
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
The **Dataplex Auto-DQ Spec Generator Agent** is a Streamlit app that reads historical column profiling metrics directly from BigQuery, uses **Gemini 3.5 Flash** (via the `google-genai` SDK) to propose rule configurations, accepts human feedback/spreadsheets, and generates your YAML aspects.

#### Setup & Launch:
```bash
cd dataplex-dq-agent
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```
* **Step 1: Generate Action Plan**: Queries BigQuery profiling tables and outputs suggested rules with clear statistical justifications.
* **Step 2: Human-in-the-Loop Feedback**: Review suggestions, upload logic sheets (PDF/CSV/Excel/TXT), type custom overrides, and output the final `data-rules-aspects.yaml` file.

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
