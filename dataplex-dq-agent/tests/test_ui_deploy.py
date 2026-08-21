"""dq_ui deploy flow via Streamlit's official AppTest harness (real streamlit,
no module stubs): the repo-write button writes valid, parseable YAML and the
workspace copy is only written by its own button."""
import os

import dq_core


def test_deploy_write_button(tmp_path):
    from streamlit.testing.v1 import AppTest

    gov = tmp_path / "gov"
    gov.mkdir()
    target = gov / "dh1_dps_test.yaml"
    script = f"""
import dq_core
from dq_ui import render_preview_and_deploy

block = dq_core.render_dps_scan_block("dh1_test_scan", "proj", "ds", "tbl",
                                      "updt_ts", "0 8 * * 0")
header = dq_core.render_file_header("dataplex-dp", "0 8 * * 0", True, "default")
render_preview_and_deploy(None, header, [block], {str(target)!r}, {str(gov)!r},
                          "dh1_dps_test.yaml", False,
                          scan_ids=["dh1_test_scan"], top_key="dataplex-dp")
"""
    at = AppTest.from_string(script)
    at.run()
    assert not at.exception, at.exception

    write_btn = next(b for b in at.button if b.label == "Write to Dataplex repo")
    write_btn.click()
    at.run()
    assert not at.exception, at.exception

    assert target.exists(), "repo write button did not write the file"
    text = target.read_text(encoding="utf-8")
    ok, err = dq_core.check_yaml_text(text, ["dh1_test_scan"], "dataplex-dp")
    assert ok, err
    assert dq_core.read_scans(str(target), "dataplex-dp").keys() == {"dh1_test_scan"}

    workspace_copy = os.path.abspath(os.path.join(
        os.path.dirname(dq_core.__file__), "..", "dh1_dps_test.yaml"))
    assert not os.path.exists(workspace_copy), "workspace copy written without its button"
