"""Shared fixtures/fakes for the dataplex-dq-agent suite.

Everything is injected through the pipeline's dependency seams (call_llm,
fetch_*, notify, http, sleep) — no monkey-patching, no sys.modules stubs.
dq_core and dq_generation import without streamlit; only test_ui_deploy
touches the real streamlit via its official AppTest harness."""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dq_generation as gen  # noqa: E402


class Notes:
    """Recorder standing in for the injected notify(level, text) callable."""

    def __init__(self):
        self.calls = []

    def __call__(self, level, text):
        self.calls.append((level, text))

    def texts(self, level):
        return [t for lvl, t in self.calls if lvl == level]

    @property
    def warnings(self):
        return self.texts("warning")

    @property
    def errors(self):
        return self.texts("error")

    @property
    def infos(self):
        return self.texts("info")


@pytest.fixture
def notes():
    return Notes()


class FakeResp:
    """Minimal requests.Response stand-in."""

    def __init__(self, payload, status=200, text=""):
        self._p = payload
        self.status_code = status
        self.ok = status < 400
        self.text = text

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError(f"{self.status_code} Client Error")

    def json(self):
        return self._p


META0 = {"partition_column": "", "partition_column_type": "",
         "require_partition_filter": False, "temporal_columns": []}


def blank_meta(*tables):
    return {t: {**META0, "temporal_columns": []} for t in tables}


def make_settings(**over):
    base = dict(source_project_id="src-proj", source_dataset_id="src_ds",
                bq_location="loc", instance="dh1", governance_dir="",
                fuelix_api_key="KEY", model_name="model")
    base.update(over)
    return gen.Settings(**base)
