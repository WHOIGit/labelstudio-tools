from argparse import Namespace

import questionary

from labelstudio_tools import config_wizard as cw


def _state(tmp_path):
    state = cw.State(
        args=Namespace(verbose=False, dir=None, auth=None, outfile=None, default=False)
    )
    state.config_dir = tmp_path
    state.outfile = tmp_path / "ls_project.new.toml"
    return state


def test_collect_other_storages_skips_auth_only_and_incomplete_entries(tmp_path):
    (tmp_path / "ls_auth.ichthyolith.toml").write_text(
        """
[[storage]]
type = "s3"
bucket = "ichthyolith"
endpoint_url = "https://vast.whoi.edu"
aws_access_key_id = "key"
aws_secret_access_key = "secret"
"""
    )
    (tmp_path / "ls_project.stub.toml").write_text(
        """
[[storage]]
type = "s3"
mode = "source"
bucket = ""
endpoint_url = ""
"""
    )
    (tmp_path / "ls_project.good.toml").write_text(
        """
[[storage]]
type = "s3"
mode = "source"
bucket = "ichthyolith"
endpoint_url = "https://vast.whoi.edu"
bucket_prefix = "temp/train2"
"""
    )

    storages = cw.collect_other_storages(_state(tmp_path))

    assert len(storages) == 1
    assert storages[0]["source"] == "ls_project.good.toml"
    assert storages[0]["data"]["mode"] == "source"
    assert storages[0]["data"]["bucket_prefix"] == "temp/train2"


def test_format_storage_choices_has_headers_separate_prefix_and_auth_check(tmp_path):
    state = _state(tmp_path)
    state.auth_data["storage"].append({
        "type": "s3",
        "bucket": "ichthyolith",
        "endpoint_url": "https://vast.whoi.edu",
        "aws_access_key_id": "key",
        "aws_secret_access_key": "secret",
    })
    items = [{
        "data": {
            "type": "s3",
            "mode": "source",
            "bucket": "ichthyolith",
            "endpoint_url": "https://vast.whoi.edu",
            "bucket_prefix": "temp/train2",
        },
        "sources": ["ls_project.good.toml"],
    }]

    choices = cw._format_storage_choices(state, items)
    row = next(c for c in choices if isinstance(c, questionary.Choice) and not c.disabled)
    row_text = "".join(part[1] for part in row.title)

    assert "Mode" in choices[0].line
    assert "Auth" in choices[0].line
    assert "Bucket" in choices[0].line
    assert "Prefix" in choices[0].line
    assert "ichthyolith/temp/train2" not in row_text
    assert "ichthyolith" in row_text
    assert "temp/train2" in row_text
    assert any(part == ("fg:#00aa00 bold", "✓") for part in row.title)
    assert row.value == 0


def test_prompt_storage_import_method_labels_store_config_value(monkeypatch, tmp_path):
    state = _state(tmp_path)
    state.auth_mode = "defer"
    text_answers = iter(["s3", "https://vast.whoi.edu", "ichthyolith", "temp/train2",
                         "source", "ichthyolith temp", "15", "(?!)"])
    yn_answers = iter([True, True])

    monkeypatch.setattr(cw, "ask_text", lambda *args, **kwargs: next(text_answers))
    monkeypatch.setattr(cw, "ask_yn", lambda *args, **kwargs: next(yn_answers))
    monkeypatch.setattr(
        cw,
        "ask_select",
        lambda *args, **kwargs: "blobs (tasks from media-files)",
    )

    storage = cw.prompt_storage(state)

    assert storage["import_method"] == "blobs"


def test_collect_other_ml_skips_blank_stub_entries(tmp_path):
    (tmp_path / "ls_auth.toml").write_text(
        """
[[ml_backend]]
name = ""
backend_url = ""
"""
    )
    (tmp_path / "ls_project.good.toml").write_text(
        """
[ml_backend]
name = "SAM2"
backend_url = "https://ml.example/SAM2/"
"""
    )

    backends = cw.collect_other_ml(_state(tmp_path))

    assert len(backends) == 1
    assert backends[0]["source"] == "ls_project.good.toml"
    assert backends[0]["data"]["backend_url"] == "https://ml.example/SAM2/"


def test_format_ml_choices_has_headers_and_auth_check(tmp_path):
    state = _state(tmp_path)
    state.auth_data["ml_backend"].append({
        "backend_url": "https://ml.example/SAM2/",
        "user": "user",
        "pass": "pass",
    })
    items = [{
        "data": {
            "name": "SAM2",
            "backend_url": "https://ml.example/SAM2/",
        },
        "sources": ["ls_project.good.toml"],
    }]

    choices = cw._format_ml_choices(state, items)
    row = next(c for c in choices if isinstance(c, questionary.Choice) and not c.disabled)
    row_text = "".join(part[1] for part in row.title)

    assert "Name" in choices[0].line
    assert "Backend URL" in choices[0].line
    assert "Auth" in choices[0].line
    assert "SAM2" in row_text
    assert "https://ml.example/SAM2/" in row_text
    assert any(part == ("fg:#00aa00 bold", "✓") for part in row.title)
    assert row.value == 0


def test_write_project_config_writes_at_most_one_ml_backend(tmp_path):
    state = _state(tmp_path)
    state.host = "https://labelstudio.example"
    state.project_name = "demo"
    state.label_config = "label_ui.xml"
    state.auth_mode = "defer"
    state.pk = "image"
    state.ml_backends = [
        {"name": "first", "backend_url": "https://ml.example/first"},
        {"name": "second", "backend_url": "https://ml.example/second"},
    ]

    cw.write_project_config(state)
    rendered = state.outfile.read_text()

    assert rendered.count("[ml_backend]") == 1
    assert "https://ml.example/first" in rendered
    assert "https://ml.example/second" not in rendered


def test_step_annotations_uses_clear_instruction_prompt(monkeypatch, tmp_path):
    state = _state(tmp_path)
    prompts = []

    def fake_ask_text(prompt, default=""):
        prompts.append(prompt)
        return "instructions"

    monkeypatch.setattr(cw, "ask_text", fake_ask_text)
    monkeypatch.setattr(cw, "ask_yn", lambda *args, **kwargs: False)

    cw.step_annotations(state)

    assert prompts == ["annotation instructions text:"]
    assert state.annotations["instructions"] == "instructions"
