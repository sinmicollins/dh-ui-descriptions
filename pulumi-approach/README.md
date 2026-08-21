# Pulumi Deployment for Dataplex Rule Aspects

Deploys the central rules spec (`../data-rules-aspects.yaml`) as a **Knowledge
Catalog (f.k.a. Dataplex) entry aspect** using Pulumi.

`__main__.py`:
1. loads `data-rules-aspects.yaml` from the repository root,
2. rewrites the `dataplex-types.global.data-rules@Schema.*` aspect keys into
   their fully-qualified `projects/dataplex-types/locations/global/...` form
   and JSON-serializes each aspect's `data` payload,
3. declares one `gcp.dataplex.Entry` that binds those aspects to the demo
   BigQuery table (`demo1-311322.mobile_data.work_orders` — edit the
   `entry_id` in `__main__.py` to target another table).

## Prerequisites

- Pulumi CLI, logged in.
- GCP credentials (`gcloud auth application-default login` or
  `GOOGLE_APPLICATION_CREDENTIALS`).
- Python 3.12+.

## Deploy

```bash
cd pulumi-approach
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

pulumi stack init <your-org-or-user>/dev
pulumi config set gcp:project demo1-311322
pulumi config set gcp:region us-central1

pulumi preview
pulumi up
```

See the repository root README (Workflow 2) for how this fits the overall
GitOps flow.
