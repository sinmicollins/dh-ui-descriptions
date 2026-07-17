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

import os
import json
import yaml
import pulumi
import pulumi_gcp as gcp

# Load aspects configuration from the shared YAML file in the parent directory
yaml_path = os.path.join(os.path.dirname(__file__), "../data-rules-aspects.yaml")
with open(yaml_path, "r") as f:
    aspects_raw = yaml.safe_load(f)

# Convert the raw YAML aspects into the format expected by the Pulumi GCP Provider
aspects = {}
for aspect_key, aspect_val in aspects_raw.items():
    # Translate the short alias to the fully qualified aspect type path required by the GCP API
    if aspect_key.startswith("dataplex-types.global.data-rules@"):
        fq_key = aspect_key.replace(
            "dataplex-types.global.data-rules@", 
            "projects/dataplex-types/locations/global/aspectTypes/data-rules@"
        )
    else:
        fq_key = aspect_key

    # The "data" field in Pulumi gcp.dataplex.Entry aspects expects a serialized JSON string
    aspects[fq_key] = {
        "aspectType": "projects/dataplex-types/locations/global/aspectTypes/data-rules",
        "data": json.dumps(aspect_val["data"])
    }

# Define the Dataplex Catalog Entry resource to manage the aspects on the BigQuery table
work_orders_rules = gcp.dataplex.Entry(
    "workOrdersRules",
    entry_id="bigquery.googleapis.com/projects/demo1-311322/datasets/mobile_data/tables/work_orders",
    entry_group_id="@bigquery",
    location="us-central1",
    project="demo1-311322",
    aspects=aspects
)
