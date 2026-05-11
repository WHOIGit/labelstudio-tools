"""Project commands for the ``lstool`` CLI."""

from __future__ import annotations

import argparse
import importlib.metadata
import os

from . import config_wizard
from .cli_utils import (
    CliError,
    add_auth_source_args,
    auth_sources_from_args,
    load_project_config_for_cli,
    print_table,
    project_auth_override,
    project_manager_from_cli_config,
    resolve_config_path,
    server_version,
)
from .config import project_ref
from .projman import ProjectManager


def run_project_wizard(args: argparse.Namespace) -> int:
    wiz_args = argparse.Namespace(
        section="project",
        default=args.default,
        default_inline=args.default_inline,
        verbose=args.verbose,
        config_dir=args.config_dir or os.environ.get("LSTOOL_CONFIG_DIR"),
        auth=args.auth or os.environ.get("LSTOOL_CONFIG_AUTH"),
        outfile=args.outfile or os.environ.get("LSTOOL_CONFIG"),
    )
    if wiz_args.default:
        config_wizard.run_project_default_mode(wiz_args)
        return 0
    if wiz_args.default_inline:
        config_wizard.run_project_default_mode(wiz_args, inline=True)
        return 0

    state = config_wizard.State(args=wiz_args)
    config_wizard.step_descriptions(state)
    config_wizard.step_config_dir(state)
    config_wizard.step_host(state)
    config_wizard.step_auth_file(state)
    config_wizard.step_token(state)
    config_wizard.step_project_name(state)
    config_wizard.step_shortname(state)
    config_wizard.step_outfile(state)
    config_wizard.step_label_config(state)
    config_wizard.step_general(state)
    config_wizard.step_lstools(state)
    config_wizard.step_storage_loop(state)
    config_wizard.step_ml_loop(state)
    config_wizard.step_annotations(state)
    config_wizard.write_project_config(state)
    config_wizard.write_new_auth_file(state)
    return 0


def run_project_list(args: argparse.Namespace) -> int:
    sources = auth_sources_from_args(args)
    if not sources:
        raise CliError("no labelstudio entries found")
    rows = []
    show_host = len(sources) > 1
    for source in sources:
        manager = ProjectManager(source["host"], source["token"])
        for project in manager.list_projects():
            row = {
                "id": getattr(project, "id", ""),
                "title": getattr(project, "title", ""),
            }
            if show_host:
                row = {"host": source["host"], **row}
            if args.counts:
                row.update({
                    "tasks": getattr(project, "task_number", ""),
                    "annotations": getattr(project, "total_annotations_number", ""),
                    "predictions": getattr(project, "total_predictions_number", ""),
                })
            rows.append(row)
    headers = ["host", "id", "title"] if show_host else ["id", "title"]
    if args.counts:
        headers.extend(["tasks", "annotations", "predictions"])
    print_table(rows, headers=headers)
    return 0


def run_project_create(args: argparse.Namespace) -> int:
    return _run_project_apply(args, expected="create")


def run_project_update(args: argparse.Namespace) -> int:
    return _run_project_apply(args, expected="update")


def _run_project_apply(args: argparse.Namespace, *, expected: str) -> int:
    config_path = resolve_config_path(args.config)
    manager = project_manager_from_cli_config(config_path)
    auth_override = project_auth_override(config_path)
    auth_arg = str(auth_override) if auth_override else None
    plan = manager.plan_config(str(config_path), auth_arg)
    project_item = next((item for item in plan if item["kind"] == "project"), None)
    if project_item is None:
        raise CliError("project plan did not contain a project item")
    if expected == "create" and project_item["action"] != "create":
        raise CliError(
            f"project already exists: {project_item['title']!r} "
            f"(id={project_item.get('id')})")
    if expected == "update" and project_item["action"] == "create":
        merged = load_project_config_for_cli(config_path)
        raise CliError(f"project does not exist: {project_ref(merged)!r}")
    manager.create_project_from_config(
        str(config_path), auth_arg, dry_run=args.dry_run)
    return 0


def run_project_version(args: argparse.Namespace) -> int:
    rows = [
        {
            "component": "labelstudio-tools",
            "target": "package",
            "version": _dist_version("labelstudio-tools"),
            "status": "",
        },
        {
            "component": "label_studio_sdk",
            "target": "package",
            "version": _dist_version("label-studio-sdk"),
            "status": "",
        },
    ]
    if not args.nocheck:
        rows.append({
            "component": "update-check",
            "target": "PyPI",
            "version": "",
            "status": "not implemented",
        })

    sources = auth_sources_from_args(args, allow_missing=True)
    for source in sources:
        try:
            version = server_version(source["host"], source["token"])
            status = "ok"
        except Exception as exc:
            version = ""
            status = str(exc)
        rows.append({
            "component": "label-studio-server",
            "target": source["host"],
            "version": version,
            "status": status,
        })

    print_table(rows, headers=["component", "target", "version", "status"])
    return 0


def _dist_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return "(not installed)"


def add_project_parsers(subparsers) -> None:
    project = subparsers.add_parser("project", help="Manage Label Studio projects.")
    project_sub = project.add_subparsers(dest="project_command", required=True)

    wizard = project_sub.add_parser(
        "wizard", help="Scaffold or interactively build a project config.")
    defaults = wizard.add_mutually_exclusive_group()
    defaults.add_argument("--default", action="store_true")
    defaults.add_argument("--default-inline", action="store_true")
    wizard.add_argument("-v", "--verbose", action="store_true")
    wizard.add_argument("--config-dir")
    wizard.add_argument("--auth")
    wizard.add_argument("-o", "--outfile")
    wizard.set_defaults(func=run_project_wizard)

    list_cmd = project_sub.add_parser("list", help="List projects.")
    add_auth_source_args(list_cmd)
    list_cmd.add_argument("--counts", "--count", action="store_true")
    list_cmd.set_defaults(func=run_project_list)

    create = project_sub.add_parser("create", help="Create a project from config.")
    create.add_argument("-c", "--config")
    create.add_argument("--dry-run", action="store_true")
    create.set_defaults(func=run_project_create)

    update = project_sub.add_parser("update", help="Update a project from config.")
    update.add_argument("-c", "--config")
    update.add_argument("--dry-run", action="store_true")
    update.set_defaults(func=run_project_update)

    version = project_sub.add_parser("version", help="Print versions.")
    add_auth_source_args(version, config_optional=True)
    version.add_argument("--nocheck", action="store_true")
    version.set_defaults(func=run_project_version)
