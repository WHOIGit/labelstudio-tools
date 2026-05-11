"""Task commands for the ``lstool`` CLI."""

from __future__ import annotations

import argparse
import os
import time
from collections import defaultdict
from pathlib import Path

from .cli_utils import (
    CliError,
    add_config_arg,
    add_field_scope_args,
    add_task_selection_args,
    apply_field_scope,
    api_include_fields,
    infer_format,
    load_project_config_for_cli,
    output_records,
    print_table,
    requested_fields,
    resolve_config_path,
    resolve_output_path,
    selection_kwargs,
    task_manager_from_cli_config,
)
from .config import project_ref
from .snapshot_download import SnapshotManager
from .utils import s3_url_to_bucket_and_key


def run_tasks_view(args: argparse.Namespace) -> int:
    manager = task_manager_from_cli_config(resolve_config_path(args.config))
    include, exclude = requested_fields(args)
    sel = selection_kwargs(manager, args)
    tasks = manager.get_tasks(
        limit_fields_to=api_include_fields(include, exclude),
        **sel,
    )
    tasks = apply_field_scope(tasks, include, exclude)
    output_records(tasks, fmt=args.format)
    return 0


def run_tasks_download(args: argparse.Namespace) -> int:
    manager = task_manager_from_cli_config(resolve_config_path(args.config))
    include, exclude = requested_fields(args)
    if not args.tasks and not args.filter and not args.quiet:
        print("WARNING: no selection supplied; for whole-project exports, "
              "consider `lstool tasks download-snapshot`.")
    sel = selection_kwargs(manager, args)
    tasks = manager.get_tasks(
        limit_fields_to=api_include_fields(include, exclude),
        with_annotations=True,
        **sel,
    )
    tasks = apply_field_scope(tasks, include, exclude)
    outfile = resolve_output_path(args.outfile)
    output_records(tasks, fmt=infer_format(outfile), outfile=outfile)
    return 0


def run_tasks_download_s3(args: argparse.Namespace) -> int:
    manager = task_manager_from_cli_config(resolve_config_path(args.config))
    requested_fields(args)  # validates unsupported field-scope requests.
    if manager.s3 is None:
        raise CliError("config does not define an S3 storage usable by TaskManager")
    sel = selection_kwargs(manager, args)
    tasks = manager.get_tasks(limit_fields_to=["id", "data"], **sel)
    _validate_s3_fields(tasks, args.field)

    outdir = resolve_output_path(args.outdir)
    rows = []
    for task in tasks:
        for field_name in args.field:
            url = (task.get("data") or {}).get(field_name)
            if not isinstance(url, str) or not url.startswith("s3://"):
                continue
            relpath = _format_s3_pattern(manager, task, field_name, url, args.pattern)
            outfile = outdir / relpath
            rows.append({
                "task": task.get("id"),
                "field": field_name,
                "s3": url,
                "outfile": str(outfile),
            })
            if not args.dry_run:
                manager.download_s3url(url, str(outfile))

    print_table(rows, headers=["task", "field", "s3", "outfile"])
    return 0


def _validate_s3_fields(tasks: list[dict], fields: list[str]) -> None:
    bad = []
    for field in fields:
        has_s3 = any(
            isinstance((task.get("data") or {}).get(field), str)
            and (task.get("data") or {}).get(field).startswith("s3://")
            for task in tasks
        )
        if not has_s3:
            bad.append(field)
    if bad:
        raise CliError(
            "not S3-bearing task data field(s): "
            + ", ".join(bad)
            + "; run `lstool utils datafields --s3` to list valid choices")


def _format_s3_pattern(manager, task: dict, field_name: str,
                       s3_url: str, pattern: str) -> str:
    bucket, key = s3_url_to_bucket_and_key(s3_url)
    basename = os.path.basename(key)
    stem, ext = os.path.splitext(basename)
    data = task.get("data") or {}
    pk_fieldname = _pk_fieldname(manager)
    pk_value = ""
    if manager.task_pk_datafields:
        try:
            pk_value = manager.task_datafields_key(task)
        except Exception:
            pk_value = ""
    values = {}
    values.update(task)
    values.update(data)
    values.update({
        "id": task.get("id"),
        "pk": pk_value,
        "pk_fieldname": pk_fieldname,
        "field": data.get(field_name, ""),
        "field_name": field_name,
        "s3_bucket": bucket,
        "s3_key": key,
        "s3_basename": basename,
        "s3_stem": stem,
        "s3_ext": ext,
    })
    try:
        return pattern.format_map(values)
    except KeyError as exc:
        raise CliError(f"unknown download pattern placeholder: {exc.args[0]}") from exc


def _pk_fieldname(manager) -> str:
    pk = manager.task_pk_datafields
    if isinstance(pk, (tuple, list)):
        return "_".join(pk)
    return pk or ""


def run_tasks_download_snapshot(args: argparse.Namespace) -> int:
    config_path = resolve_config_path(args.config)
    merged = load_project_config_for_cli(config_path)
    project = project_ref(merged)
    manager = SnapshotManager(merged["host"], merged["token"], project)

    if args.list:
        rows = []
        for snap in manager.list_snapshots():
            rows.append({
                "type": "snapshot",
                "id": getattr(snap, "id", ""),
                "title": getattr(snap, "title", ""),
                "status": getattr(snap, "status", ""),
            })
        for view in manager.client.views.list(project=manager.project.id):
            rows.append({
                "type": "filterview",
                "id": getattr(view, "id", ""),
                "title": (getattr(view, "data", {}) or {}).get("title", ""),
                "status": "",
            })
        print_table(rows, headers=["type", "id", "title", "status"])
        return 0

    if not args.outfile:
        raise CliError("-o/--outfile is required with --snap or --filterview")
    outfile = resolve_output_path(args.outfile)
    if args.dry_run:
        target = args.snap if args.snap is not None else args.filterview
        action = "download snapshot" if args.snap is not None else "create filterview snapshot"
        print_table([{"action": action, "target": target, "outfile": str(outfile)}])
        return 0

    if args.snap is not None:
        snap = int(args.snap) if str(args.snap).isdigit() else args.snap
        manager.set_snapshot(snap)
    else:
        view = _get_view_by_name(manager, args.filterview)
        title = f'{manager.project.title} filterview "{args.filterview}" {int(time.time())}'
        manager.snap = manager.client.projects.exports.create(
            id=manager.project.id,
            title=title,
            task_filter_options={"view": view.id},
        )
        manager.wait_for_snapshot_completion()

    tasks = manager.download_snap(location=None, export_type="JSON")
    include, exclude = requested_fields(args)
    tasks = apply_field_scope(tasks, include, exclude)
    output_records(tasks, fmt=infer_format(outfile), outfile=outfile)
    return 0


def _get_view_by_name(manager: SnapshotManager, name: str):
    views = manager.client.views.list(project=manager.project.id)
    matches = [view for view in views if name in ((getattr(view, "data", {}) or {}).get("title", ""))]
    exact = [view for view in matches if name == ((getattr(view, "data", {}) or {}).get("title", ""))]
    if len(exact) == 1:
        return exact[0]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise CliError(f"no filterview found matching {name!r}")
    raise CliError(f"multiple filterviews found matching {name!r}")


def run_tasks_duplicates(args: argparse.Namespace) -> int:
    manager = task_manager_from_cli_config(resolve_config_path(args.config))
    if not manager.task_pk_datafields:
        raise CliError("duplicates requires `labelstudio-tools.pk` in the config")
    groups = _duplicate_groups(manager)
    if not groups:
        print("(none)")
        return 0

    keep = args.keep
    list_only = args.list or keep is None
    decisions, ambiguous = _duplicate_decisions(groups, keep or "first")
    if ambiguous and not list_only and not args.quiet:
        _print_duplicate_rows(groups, decisions, ambiguous)
        return 1
    _print_duplicate_rows(groups, decisions, ambiguous)
    if list_only:
        return 0 if not ambiguous else 1

    to_delete = []
    for key, decision in decisions.items():
        if decision.get("ambiguous"):
            continue
        to_delete.extend(decision["delete"])
    if not to_delete:
        return 0
    if not args.quiet and not _confirm(f"Delete {len(to_delete)} duplicate task(s)?"):
        return 1
    for task_id in to_delete:
        manager.client.tasks.delete(id=task_id)
    return 0


def _duplicate_groups(manager) -> dict[str, list[dict]]:
    tasks = manager.get_tasks(
        limit_fields_to=["id", "data", "created_at", "annotations"],
        with_annotations=True,
    )
    groups = defaultdict(list)
    for task in tasks:
        groups[manager.task_datafields_key(task)].append(task)
    return {key: value for key, value in groups.items() if len(value) > 1}


def _duplicate_decisions(groups: dict[str, list[dict]], keep: str) -> tuple[dict, set]:
    decisions = {}
    ambiguous = set()
    for key, tasks in groups.items():
        if keep == "first":
            keeper = min(tasks, key=lambda t: t.get("created_at") or "")
        elif keep == "latest":
            keeper = max(tasks, key=lambda t: t.get("created_at") or "")
        elif keep == "most-annotated":
            counts = [(task, _finished_annotation_count(task)) for task in tasks]
            max_count = max(count for _, count in counts)
            winners = [task for task, count in counts if count == max_count]
            if len(winners) != 1:
                ambiguous.add(key)
                decisions[key] = {"ambiguous": True, "keep": None, "delete": []}
                continue
            keeper = winners[0]
        else:
            raise CliError(f"invalid keep strategy: {keep}")
        decisions[key] = {
            "keep": keeper["id"],
            "delete": [task["id"] for task in tasks if task["id"] != keeper["id"]],
        }
    return decisions, ambiguous


def _finished_annotation_count(task: dict) -> int:
    return sum(
        1
        for annotation in task.get("annotations", []) or []
        if not annotation.get("was_cancelled") and not annotation.get("cancelled")
    )


def _print_duplicate_rows(groups: dict[str, list[dict]],
                          decisions: dict,
                          ambiguous: set) -> None:
    rows = []
    for key, tasks in groups.items():
        decision = decisions.get(key, {})
        for task in tasks:
            marker = ""
            if key in ambiguous:
                marker = "ambiguous"
            elif decision.get("keep") == task.get("id"):
                marker = "keep"
            elif task.get("id") in decision.get("delete", []):
                marker = "delete"
            rows.append({
                "pk": key,
                "task": task.get("id"),
                "created_at": task.get("created_at", ""),
                "annotations": _finished_annotation_count(task),
                "action": marker,
            })
    print_table(rows, headers=["pk", "task", "created_at", "annotations", "action"])


def run_tasks_delete(args: argparse.Namespace) -> int:
    manager = task_manager_from_cli_config(resolve_config_path(args.config))
    sel = selection_kwargs(manager, args)
    tasks = manager.get_tasks(limit_fields_to=["id", "data"], **sel)
    rows = [{"id": task.get("id"), "data": task.get("data", {})} for task in tasks]
    print_table(rows, headers=["id", "data"])
    if args.dry_run:
        return 0
    if not args.quiet and not _confirm(f"Delete {len(tasks)} task(s)?"):
        return 1
    for task in tasks:
        manager.client.tasks.delete(id=task["id"])
    return 0


def _confirm(prompt: str) -> bool:
    answer = input(f"{prompt} [y/N] ").strip().lower()
    return answer in ("y", "yes")


def add_tasks_parsers(subparsers) -> None:
    tasks = subparsers.add_parser("tasks", help="Read, export, and clean up tasks.")
    tasks_sub = tasks.add_subparsers(dest="tasks_command", required=True)

    view = tasks_sub.add_parser("view", help="Print tasks.")
    add_config_arg(view)
    add_task_selection_args(view, include_positional=True)
    add_field_scope_args(view)
    view.add_argument("--format", choices=["table", "json", "jsonl", "csv"],
                      default="table")
    view.set_defaults(func=run_tasks_view)

    download = tasks_sub.add_parser("download", help="Download selected tasks.")
    add_config_arg(download)
    download.add_argument("-o", "--outfile", required=True)
    add_task_selection_args(download)
    add_field_scope_args(download)
    download.add_argument("-q", "--quiet", action="store_true")
    download.set_defaults(func=run_tasks_download)

    download_s3 = tasks_sub.add_parser("download-s3", help="Download S3 task data.")
    add_config_arg(download_s3)
    download_s3.add_argument("-o", "--outdir", required=True)
    add_task_selection_args(download_s3, require=True)
    download_s3.add_argument("--field", nargs="+", required=True)
    download_s3.add_argument(
        "--pattern", default="{id:05}_{pk_fieldname}{s3_ext}")
    add_field_scope_args(download_s3)
    download_s3.add_argument("--dry-run", action="store_true")
    download_s3.set_defaults(func=run_tasks_download_s3)

    snapshot = tasks_sub.add_parser("download-snapshot", help="Download snapshots.")
    add_config_arg(snapshot)
    snap_mode = snapshot.add_mutually_exclusive_group(required=True)
    snap_mode.add_argument("--list", action="store_true")
    snap_mode.add_argument("--snap")
    snap_mode.add_argument("--filterview")
    snapshot.add_argument("-o", "--outfile")
    add_field_scope_args(snapshot)
    snapshot.add_argument("--dry-run", action="store_true")
    snapshot.set_defaults(func=run_tasks_download_snapshot)

    duplicates = tasks_sub.add_parser("duplicates", help="Find/delete duplicates.")
    add_config_arg(duplicates)
    duplicates.add_argument("--list", "--dry-run", dest="list", action="store_true")
    duplicates.add_argument("--keep", choices=["first", "latest", "most-annotated"])
    duplicates.add_argument("-q", "--quiet", action="store_true")
    duplicates.set_defaults(func=run_tasks_duplicates)

    delete = tasks_sub.add_parser("delete", help="Delete selected tasks.")
    add_config_arg(delete)
    add_task_selection_args(delete, require=True)
    delete.add_argument("--dry-run", action="store_true")
    delete.add_argument("-q", "--quiet", action="store_true")
    delete.set_defaults(func=run_tasks_delete)
