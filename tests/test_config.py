import pytest

from labelstudio_tools.config import load_config


def test_load_config_uses_lstool_config_auth_fallback(monkeypatch, tmp_path):
    project = tmp_path / "ls_project.toml"
    auth = tmp_path / "ls_auth.toml"
    project.write_text(
        'host = "https://labelstudio.example"\n'
        'project = "demo"\n'
    )
    auth.write_text(
        '[[labelstudio]]\n'
        'host = "https://labelstudio.example"\n'
        'token = "secret"\n'
    )

    monkeypatch.setenv("LSTOOL_CONFIG_AUTH", "ls_auth.toml")
    monkeypatch.setenv("LSTOOL_CONFIG_DIR", str(tmp_path))

    loaded = load_config(str(project))

    assert loaded["token"] == "secret"


def test_load_config_auth_fallback_errors_on_config_dir_ambiguity(monkeypatch, tmp_path):
    cwd = tmp_path / "cwd"
    config_dir = tmp_path / "configs"
    cwd.mkdir()
    config_dir.mkdir()
    project = cwd / "ls_project.toml"
    project.write_text(
        'host = "https://labelstudio.example"\n'
        'project = "demo"\n'
    )
    for directory in (cwd, config_dir):
        (directory / "ls_auth.toml").write_text(
            '[[labelstudio]]\n'
            'host = "https://labelstudio.example"\n'
            'token = "secret"\n'
        )

    monkeypatch.chdir(cwd)
    monkeypatch.setenv("LSTOOL_CONFIG_AUTH", "ls_auth.toml")
    monkeypatch.setenv("LSTOOL_CONFIG_DIR", str(config_dir))

    with pytest.raises(ValueError, match="ambiguous LSTOOL_CONFIG_AUTH"):
        load_config(str(project))
