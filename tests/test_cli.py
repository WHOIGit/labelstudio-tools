from argparse import Namespace

import pytest

from labelstudio_tools.cli import build_parser
from labelstudio_tools.cli_utils import (
    CliError,
    auth_sources_from_args,
    project_auth_override,
    resolve_existing_path,
    run_auth_test,
)


def test_parser_accepts_top_level_sections():
    parser = build_parser()

    args = parser.parse_args(["tasks", "view", "123"])

    assert args.section == "tasks"
    assert args.tasks_command == "view"
    assert args.task == ["123"]


def test_project_list_accepts_env_backed_config_without_explicit_source():
    parser = build_parser()

    args = parser.parse_args(["project", "list", "--count"])

    assert args.section == "project"
    assert args.project_command == "list"
    assert args.config is None
    assert args.counts is True


def test_resolve_existing_path_uses_config_dir(monkeypatch, tmp_path):
    cwd = tmp_path / "cwd"
    config_dir = tmp_path / "configs"
    cwd.mkdir()
    config_dir.mkdir()
    target = config_dir / "ls_project.toml"
    target.write_text("")

    monkeypatch.chdir(cwd)
    monkeypatch.setenv("LSTOOL_CONFIG_DIR", str(config_dir))

    assert resolve_existing_path("ls_project.toml") == target.resolve()


def test_resolve_existing_path_errors_on_ambiguity(monkeypatch, tmp_path):
    cwd = tmp_path / "cwd"
    config_dir = tmp_path / "configs"
    cwd.mkdir()
    config_dir.mkdir()
    (cwd / "same.toml").write_text("")
    (config_dir / "same.toml").write_text("")

    monkeypatch.chdir(cwd)
    monkeypatch.setenv("LSTOOL_CONFIG_DIR", str(config_dir))

    with pytest.raises(CliError, match="ambiguous"):
        resolve_existing_path("same.toml")


def test_project_auth_override_uses_env_only_without_inline_auth(monkeypatch, tmp_path):
    config = tmp_path / "ls_project.toml"
    auth = tmp_path / "ls_auth.toml"
    config.write_text('host = "https://ls.example"\nproject = "demo"\n')
    auth.write_text("")

    monkeypatch.setenv("LSTOOL_CONFIG_AUTH", str(auth))

    assert project_auth_override(config) == auth.resolve()


def test_project_auth_override_skips_inline_token(monkeypatch, tmp_path):
    config = tmp_path / "ls_project.toml"
    auth = tmp_path / "ls_auth.toml"
    config.write_text('host = "https://ls.example"\ntoken = "secret"\n')
    auth.write_text("")

    monkeypatch.setenv("LSTOOL_CONFIG_AUTH", str(auth))

    assert project_auth_override(config) is None


def test_auth_sources_from_args_uses_lstool_config(monkeypatch):
    args = Namespace(config=None, auth=None, host=None, token=None)
    monkeypatch.setenv("LSTOOL_CONFIG", "project.toml")
    monkeypatch.setattr(
        "labelstudio_tools.cli_utils.resolve_config_path",
        lambda value: "resolved/project.toml",
    )
    monkeypatch.setattr(
        "labelstudio_tools.cli_utils.load_project_config_for_cli",
        lambda path: {"host": "https://ls.example", "token": "secret"},
    )

    assert auth_sources_from_args(args) == [
        {"host": "https://ls.example", "token": "secret"}
    ]


def test_auth_test_requires_host_and_token_together():
    args = Namespace(
        host="https://ls.example",
        token=None,
        ls=None,
        storage=None,
        ml=None,
        auth=None,
        config=None,
    )

    with pytest.raises(CliError, match="--host and --token"):
        run_auth_test(args)
