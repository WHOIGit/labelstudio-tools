"""Shared helpers and utility/auth commands for the ``lstool`` CLI."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import tomllib
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import urljoin

import requests
from tabulate import tabulate

from .config import load_config
from .config_wizard import (
    run_auth_default_mode,
    validate_ls_token,
    validate_ml_backend,
    validate_storage,
)
from .projman import ProjectManager
from .taskman import TaskManager


class CliError(Exception):
    """User-facing CLI error."""


class CliExit(Exception):
    """Return a specific exit code without printing another error."""

    def __init__(self, code: int):
        super().__init__(code)
        self.code = code


def tablefmt() -> str:
    return os.environ.get("LSTOOL_TABLEFMT", "github")


def print_table(rows: list[dict], *, headers: Optional[list[str]] = None) -> None:
    if not rows:
        print("(none)")
        return
    if headers is None:
        seen = []
        for row in rows:
            for key in row:
                if key not in seen:
                    seen.append(key)
        headers = seen
    print(tabulate([[row.get(h, "") for h in headers] for row in rows],
                   headers=headers, tablefmt=tablefmt()))


def resolve_existing_path(value: str, *, what: str = "path",
                          file_only: bool = True) -> Path:
    """Resolve an existing path using CLI.md LSTOOL_CONFIG_DIR semantics."""
    path = Path(value).expanduser()
    if path.is_absolute():
        if not _path_exists(path, file_only):
            raise FileNotFoundError(f"{what} not found: {path}")
        return path.resolve()

    cwd_candidate = (Path.cwd() / path).resolve()
    config_dir = os.environ.get("LSTOOL_CONFIG_DIR")
    if not config_dir:
        if not _path_exists(cwd_candidate, file_only):
            raise FileNotFoundError(
                f"{what} {value!r} not found in cwd ({Path.cwd()})")
        return cwd_candidate

    config_candidate = (Path(config_dir).expanduser() / path).resolve()
    candidates = _dedupe_paths([config_candidate, cwd_candidate])
    existing = [p for p in candidates if _path_exists(p, file_only)]
    if len(existing) == 1:
        return existing[0]
    if not existing:
        listed = ", ".join(str(p) for p in candidates)
        raise FileNotFoundError(f"{what} {value!r} not found; checked: {listed}")
    listed = ", ".join(str(p) for p in existing)
    raise CliError(
        f"ambiguous {what} {value!r}; exists in multiple locations: {listed}")


def maybe_existing_path(value: str, *, file_only: bool = True) -> Optional[Path]:
    try:
        return resolve_existing_path(value, file_only=file_only)
    except FileNotFoundError:
        return None


def resolve_config_path(value: Optional[str]) -> Path:
    value = value or os.environ.get("LSTOOL_CONFIG")
    if not value:
        raise CliError("config required: pass -c/--config or set LSTOOL_CONFIG")
    return resolve_existing_path(value, what="config")


def resolve_auth_path(value: Optional[str]) -> Optional[Path]:
    if value is None:
        return None
    return resolve_existing_path(value, what="auth file")


def resolve_output_path(value: str) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else Path.cwd() / path


def _path_exists(path: Path, file_only: bool) -> bool:
    return path.is_file() if file_only else path.exists()


def _dedupe_paths(paths: Iterable[Path]) -> list[Path]:
    out = []
    seen = set()
    for path in paths:
        key = str(path)
        if key not in seen:
            out.append(path)
            seen.add(key)
    return out


def read_toml(path: Path) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)


def normalize_auth_lists(data: dict) -> dict:
    data = dict(data)
    for key in ("labelstudio", "storage", "ml_backend"):
        value = data.get(key)
        if value is None:
            data[key] = []
        elif isinstance(value, dict):
            data[key] = [value]
    return data


def project_auth_override(config_path: Path) -> Optional[Path]:
    """Return LSTOOL_CONFIG_AUTH fallback when the project has no auth/secrets."""
    auth_env = os.environ.get("LSTOOL_CONFIG_AUTH")
    if not auth_env:
        return None
    raw = read_toml(config_path)
    if raw.get("auth"):
        return None
    if _has_inline_secret(raw):
        return None
    return resolve_existing_path(auth_env, what="auth file")


def _has_inline_secret(raw: dict) -> bool:
    if raw.get("token"):
        return True
    storages = raw.get("storage", [])
    if isinstance(storages, dict):
        storages = [storages]
    for storage in storages:
        if storage.get("aws_access_key_id") or storage.get("aws_secret_access_key"):
            return True
    ml = raw.get("ml_backend")
    mls = ml if isinstance(ml, list) else ([ml] if isinstance(ml, dict) else [])
    for entry in mls:
        if entry.get("user") or entry.get("pass"):
            return True
    return False


def load_project_config_for_cli(config_path: Path) -> dict:
    auth_override = project_auth_override(config_path)
    return load_config(str(config_path), str(auth_override) if auth_override else None)


def project_manager_from_cli_config(config_path: Path) -> ProjectManager:
    auth_override = project_auth_override(config_path)
    return ProjectManager.from_config(
        str(config_path), str(auth_override) if auth_override else None)


def task_manager_from_cli_config(config_path: Path) -> TaskManager:
    auth_override = project_auth_override(config_path)
    return TaskManager.from_config(
        str(config_path), str(auth_override) if auth_override else None)


def add_config_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-c", "--config", help="Project config TOML.")


def add_task_selection_args(parser: argparse.ArgumentParser, *,
                            require: bool = False,
                            include_positional: bool = False) -> None:
    if include_positional:
        parser.add_argument("task", nargs="*", help="Task ids or pk values.")
    group = parser.add_mutually_exclusive_group(required=require)
    group.add_argument("--tasks", nargs="+",
                       help="Task ids, pk values, or one line-delimited file.")
    group.add_argument("--filter", help="Filter file (.json or .toml).")


def add_field_scope_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-i", "--include-fields", nargs="+",
                        help="Fields or one line-delimited file.")
    parser.add_argument("-x", "--exclude-fields", nargs="+",
                        help="Fields or one line-delimited file.")


def expand_line_values(values: Optional[list[str]]) -> Optional[list[str]]:
    if not values:
        return None
    if len(values) == 1:
        path = maybe_existing_path(values[0])
        if path is not None:
            return [
                line.strip()
                for line in path.read_text().splitlines()
                if line.strip() and not line.lstrip().startswith("#")
            ]
    return values


def load_filter_arg(value: str) -> dict:
    path = resolve_existing_path(value, what="filter file")
    suffix = path.suffix.lower()
    if suffix == ".toml":
        return read_toml(path)
    with open(path) as f:
        return json.load(f)


def resolve_task_ids(manager: TaskManager, values: Optional[list[str]]) -> Optional[list[int]]:
    values = expand_line_values(values)
    if not values:
        return None
    ids = []
    pk_values = []
    for value in values:
        try:
            ids.append(int(value))
        except ValueError:
            pk_values.append(value)
    if not pk_values:
        return ids
    if not manager.task_pk_datafields:
        raise CliError("pk lookup requires `labelstudio-tools.pk` in the config")
    manager.cache_task_by_pk(manager.task_pk_datafields)
    by_pk = manager.cached_task_by_pk or {}
    for value in pk_values:
        task = by_pk.get(manager._normalize_pk_key(value))
        if task is None:
            raise CliError(f"no task found for pk value {value!r}")
        ids.append(task["id"])
    return ids


def selection_kwargs(manager: TaskManager, args: argparse.Namespace, *,
                     positional_attr: str = "task") -> dict:
    positional = getattr(args, positional_attr, None) or []
    if positional and (getattr(args, "tasks", None) or getattr(args, "filter", None)):
        raise CliError("positional task ids, --tasks, and --filter are mutually exclusive")
    if getattr(args, "filter", None):
        return {"filter_dict": load_filter_arg(args.filter)}
    values = positional or getattr(args, "tasks", None)
    ids = resolve_task_ids(manager, values)
    return {"ids": ids} if ids else {}


def requested_fields(args: argparse.Namespace) -> tuple[Optional[list[str]], Optional[list[str]]]:
    include = expand_line_values(getattr(args, "include_fields", None))
    exclude = expand_line_values(getattr(args, "exclude_fields", None))
    for fields in (include, exclude):
        if fields and ("annotations" in fields or "predictions" in fields):
            raise NotImplementedError(
                "container-level annotations/predictions field scoping is not implemented")
    return include, exclude


def api_include_fields(include: Optional[list[str]],
                       exclude: Optional[list[str]]) -> Optional[list[str]]:
    fields = include
    if fields is None and exclude:
        return None
    if fields is None:
        return None
    out = []
    for field in fields:
        top = "data" if field.startswith("data.") else field
        if top not in out:
            out.append(top)
    return out


def apply_field_scope(tasks: list[dict],
                      include: Optional[list[str]],
                      exclude: Optional[list[str]]) -> list[dict]:
    return [_scope_one_task(task, include, exclude) for task in tasks]


def _scope_one_task(task: dict,
                    include: Optional[list[str]],
                    exclude: Optional[list[str]]) -> dict:
    if include:
        scoped = {}
        for field in include:
            _include_field(scoped, task, field)
    else:
        scoped = dict(task)
    for field in exclude or []:
        _exclude_field(scoped, field)
    return scoped


def _include_field(out: dict, task: dict, field: str) -> None:
    if field == "data":
        out["data"] = dict(task.get("data", {}))
    elif field.startswith("data."):
        key = field.split(".", 1)[1]
        if key in task.get("data", {}):
            out.setdefault("data", {})[key] = task["data"][key]
    elif field in task:
        out[field] = task[field]
    elif field in task.get("data", {}):
        out.setdefault("data", {})[field] = task["data"][field]


def _exclude_field(task: dict, field: str) -> None:
    if field == "data":
        task.pop("data", None)
    elif field.startswith("data."):
        data = task.get("data")
        if isinstance(data, dict):
            data.pop(field.split(".", 1)[1], None)
    else:
        task.pop(field, None)
        data = task.get("data")
        if isinstance(data, dict):
            data.pop(field, None)


def flatten_row(row: dict) -> dict:
    flat = {}
    for key, value in row.items():
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                flat[f"{key}.{subkey}"] = _cell(subvalue)
        else:
            flat[key] = _cell(value)
    return flat


def _cell(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def output_records(records: list[dict], *, fmt: str,
                   outfile: Optional[Path] = None) -> None:
    if fmt == "json":
        text = json.dumps(records, indent=2, ensure_ascii=False)
        _write_or_print(text + "\n", outfile)
        return
    if fmt == "jsonl":
        text = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records)
        _write_or_print(text, outfile)
        return
    if fmt == "csv":
        rows = [flatten_row(r) for r in records]
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
        if outfile is None:
            writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        else:
            outfile.parent.mkdir(parents=True, exist_ok=True)
            with open(outfile, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
        return
    if fmt == "table":
        print_table([flatten_row(r) for r in records])
        return
    raise CliError(f"unsupported output format: {fmt}")


def infer_format(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".json":
        return "json"
    if suffix == ".jsonl":
        return "jsonl"
    if suffix == ".csv":
        return "csv"
    raise CliError(f"cannot infer output format from extension: {path}")


def _write_or_print(text: str, outfile: Optional[Path]) -> None:
    if outfile is None:
        print(text, end="")
        return
    outfile.parent.mkdir(parents=True, exist_ok=True)
    outfile.write_text(text)


def auth_header(token: str) -> dict:
    is_legacy_token = len(token) <= 40
    auth_type = "Token" if is_legacy_token else "Bearer"
    return {"Authorization": f"{auth_type} {token}"}


def auth_sources_from_args(args: argparse.Namespace, *,
                           allow_missing: bool = False) -> list[dict]:
    explicit_modes = [
        bool(getattr(args, "config", None)),
        bool(getattr(args, "auth", None)),
        bool(getattr(args, "host", None) or getattr(args, "token", None)),
    ]
    if sum(explicit_modes) > 1:
        raise CliError("choose exactly one auth source: -c, --auth, or --host/--token")
    if getattr(args, "host", None) or getattr(args, "token", None):
        if not args.host or not args.token:
            raise CliError("--host and --token must be supplied together")
        return [{"host": args.host, "token": args.token}]
    if getattr(args, "auth", None):
        auth_path = resolve_auth_path(args.auth)
        auth_data = normalize_auth_lists(read_toml(auth_path))
        return list(auth_data.get("labelstudio", []))
    if not getattr(args, "config", None) and not os.environ.get("LSTOOL_CONFIG"):
        if allow_missing:
            return []
        raise CliError("config required: pass -c/--config or set LSTOOL_CONFIG")
    config_path = resolve_config_path(args.config)
    merged = load_project_config_for_cli(config_path)
    return [{"host": merged.get("host"), "token": merged.get("token")}]


def add_auth_source_args(parser: argparse.ArgumentParser, *,
                         config_optional: bool = False) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-c", "--config", help="Project config TOML.")
    group.add_argument("--auth", help="Standalone auth TOML.")
    group.add_argument("--host", help="Label Studio host.")
    parser.add_argument("--token", help="Label Studio token; requires --host.")


def run_auth_wizard(args: argparse.Namespace) -> int:
    if args.config_dir is None:
        args.config_dir = os.environ.get("LSTOOL_CONFIG_DIR")
    if args.outfile is None:
        args.outfile = os.environ.get("LSTOOL_CONFIG_AUTH")
    if args.default:
        run_auth_default_mode(args)
        return 0
    raise NotImplementedError("interactive auth wizard is not implemented")


def run_auth_test(args: argparse.Namespace) -> int:
    if bool(args.host) != bool(args.token):
        raise CliError("--host and --token must be supplied together")
    if args.host and args.token and (args.ls is not None or
                                     args.storage is not None or
                                     args.ml is not None):
        raise CliError("--host/--token cannot be combined with narrowing flags")

    rows = []
    if args.host and args.token:
        ok, msg = validate_ls_token(args.host, args.token)
        rows.append(_status_row("labelstudio", args.host, ok, msg))
        print_table(rows, headers=["type", "target", "status", "diagnostic"])
        return 0 if ok else 1

    if args.auth:
        auth_data = normalize_auth_lists(read_toml(resolve_auth_path(args.auth)))
        rows.extend(_auth_file_test_rows(auth_data, args))
    else:
        config_path = resolve_config_path(args.config)
        merged = load_project_config_for_cli(config_path)
        rows.extend(_project_config_test_rows(merged, args))

    print_table(rows, headers=["type", "target", "status", "diagnostic"])
    return 0 if all(r["status"] == "ok" for r in rows) else 1


def _auth_file_test_rows(auth_data: dict, args: argparse.Namespace) -> list[dict]:
    rows = []
    all_targets = args.ls is None and args.storage is None and args.ml is None
    if all_targets or args.ls is not None:
        for entry in auth_data.get("labelstudio", []):
            host = entry.get("host", "")
            if isinstance(args.ls, str) and args.ls != host:
                continue
            ok, msg = validate_ls_token(host, entry.get("token", ""))
            rows.append(_status_row("labelstudio", host, ok, msg))
    if all_targets or args.storage is not None:
        for entry in auth_data.get("storage", []):
            bucket = entry.get("bucket", "")
            if isinstance(args.storage, str) and args.storage != bucket:
                continue
            ok, msg = validate_storage(entry)
            rows.append(_status_row("storage", bucket, ok, msg))
    if all_targets or args.ml is not None:
        for entry in auth_data.get("ml_backend", []):
            target = entry.get("name") or entry.get("backend_url", "")
            if isinstance(args.ml, str) and args.ml not in (target, entry.get("backend_url")):
                continue
            ok, msg = validate_ml_backend(
                entry.get("backend_url", ""), entry.get("user"), entry.get("pass"))
            rows.append(_status_row("ml_backend", target, ok, msg))
    return rows


def _project_config_test_rows(merged: dict, args: argparse.Namespace) -> list[dict]:
    rows = []
    all_targets = args.ls is None and args.storage is None and args.ml is None
    if all_targets or args.ls is not None:
        host = merged.get("host", "")
        if not isinstance(args.ls, str) or args.ls == host:
            ok, msg = validate_ls_token(host, merged.get("token", ""))
            rows.append(_status_row("labelstudio", host, ok, msg))
    if all_targets or args.storage is not None:
        for entry in merged.get("storage", []):
            bucket = entry.get("bucket", "")
            if isinstance(args.storage, str) and args.storage != bucket:
                continue
            ok, msg = validate_storage(entry)
            rows.append(_status_row("storage", bucket, ok, msg))
    if (all_targets or args.ml is not None) and merged.get("ml_backend"):
        entry = merged["ml_backend"]
        target = entry.get("name") or entry.get("backend_url", "")
        if not isinstance(args.ml, str) or args.ml in (target, entry.get("backend_url")):
            ok, msg = validate_ml_backend(
                entry.get("backend_url", ""), entry.get("user"), entry.get("pass"))
            rows.append(_status_row("ml_backend", target, ok, msg))
    return rows


def _status_row(kind: str, target: str, ok: bool, msg: str) -> dict:
    return {
        "type": kind,
        "target": target,
        "status": "ok" if ok else "fail",
        "diagnostic": msg,
    }


def run_utils_labels(args: argparse.Namespace) -> int:
    if getattr(args, "wizard", False):
        raise NotImplementedError("utils labels --wizard is not implemented")
    manager = task_manager_from_cli_config(resolve_config_path(args.config))
    if args.xml:
        xml = getattr(manager.project, "label_config", None)
        if xml is None:
            raise CliError("project object does not expose label_config")
        if args.outfile:
            outfile = resolve_output_path(args.outfile)
            outfile.parent.mkdir(parents=True, exist_ok=True)
            outfile.write_text(xml)
        else:
            print(xml)
        return 0
    rows = []
    for name, control in manager.config_controls().items():
        rows.append({
            "name": name,
            "type": getattr(control, "type", ""),
            "to_name": getattr(control, "to_name", ""),
            "labels": ", ".join(getattr(control, "labels", []) or []),
        })
    print_table(rows, headers=["name", "type", "to_name", "labels"])
    return 0


def run_utils_cachelabels(args: argparse.Namespace) -> int:
    manager = task_manager_from_cli_config(resolve_config_path(args.config))
    if args.new_anno or args.new_pred:
        raise NotImplementedError("creating new cache-label fields is not implemented")
    anno_cached, pred_cached = discover_cachelabels(manager)
    if args.update is None:
        rows = []
        controls = manager.config_control_labels()
        for tag in sorted(controls):
            rows.append({
                "tag": tag,
                "annotation_cache": "yes" if tag in anno_cached else "",
                "prediction_cache": "yes" if tag in pred_cached else "",
                "labels": ", ".join(controls.get(tag) or []),
            })
        print_table(rows, headers=["tag", "annotation_cache",
                                   "prediction_cache", "labels"])
        return 0

    tags = args.update or sorted(set(anno_cached) | set(pred_cached))
    sel = selection_kwargs(manager, args)
    ids = sel.get("ids")
    if "filter_dict" in sel:
        ids = [t["id"] for t in manager.get_tasks(
            filter_dict=sel["filter_dict"], limit_fields_to=["id"])]
    for tag in tags:
        if tag in anno_cached or args.update:
            manager.update_cachelabel(tag, from_predictions=False, ids=ids)
        if tag in pred_cached:
            manager.update_cachelabel(tag, from_predictions=True, ids=ids)
    return 0


def discover_cachelabels(manager: TaskManager) -> tuple[set[str], set[str]]:
    anno = set()
    pred = set()
    for field in manager.data_fields():
        field_id = field.get("id") or field.get("name") or ""
        if field_id.startswith("cache_predictions_"):
            pred.add(field_id.removeprefix("cache_predictions_"))
        elif field_id.startswith("cache_"):
            anno.add(field_id.removeprefix("cache_"))
    return anno, pred


def run_utils_datafields(args: argparse.Namespace) -> int:
    if args.s3:
        raise NotImplementedError("utils datafields --s3 is not implemented")
    manager = task_manager_from_cli_config(resolve_config_path(args.config))
    rows = []
    for field in manager.data_fields():
        rows.append({
            "id": field.get("id", ""),
            "title": field.get("title", ""),
            "type": field.get("type", ""),
        })
    print_table(rows, headers=["id", "title", "type"])
    return 0


def run_utils_validate_s3(args: argparse.Namespace) -> int:
    manager = task_manager_from_cli_config(resolve_config_path(args.config))
    if manager.s3 is None:
        raise CliError("config does not define an S3 storage usable by TaskManager")
    sel = selection_kwargs(manager, args)
    tasks = manager.get_tasks(limit_fields_to=["id", "data"], **sel)
    broken = []
    for task in tasks:
        for field, value in (task.get("data") or {}).items():
            if not isinstance(value, str) or not value.startswith("s3://"):
                continue
            exists = manager.s3url_exists(value)
            if not exists:
                broken.append({
                    "task": task.get("id"),
                    "field": field,
                    "s3": value,
                })
    print_table(broken, headers=["task", "field", "s3"])
    return 1 if broken else 0


def server_version(host: str, token: str) -> str:
    response = requests.get(urljoin(host.rstrip("/") + "/", "api/version"),
                            headers=auth_header(token), timeout=10)
    response.raise_for_status()
    data = response.json()
    if isinstance(data, dict):
        return data.get("version") or data.get("release") or json.dumps(data)
    return str(data)


def add_auth_parsers(subparsers) -> None:
    auth = subparsers.add_parser("auth", help="Configure and test auth.")
    auth_sub = auth.add_subparsers(dest="auth_command", required=True)

    test = auth_sub.add_parser("test", help="Validate LS/S3/ML auth targets.")
    source = test.add_mutually_exclusive_group()
    source.add_argument("-c", "--config", help="Project config TOML.")
    source.add_argument("--auth", help="Standalone auth TOML.")
    source.add_argument("--host", help="Label Studio host.")
    test.add_argument("--token", help="Label Studio token; requires --host.")
    test.add_argument("--ls", nargs="?", const=True)
    test.add_argument("--storage", "--s3", dest="storage", nargs="?", const=True)
    test.add_argument("--ml", nargs="?", const=True)
    test.set_defaults(func=run_auth_test)

    wizard = auth_sub.add_parser("wizard", help="Scaffold an auth config.")
    wizard.add_argument("--default", action="store_true",
                        help="Write a stub auth TOML and exit.")
    wizard.add_argument("-v", "--verbose", action="store_true")
    wizard.add_argument("--config-dir")
    wizard.add_argument("-o", "--outfile")
    wizard.set_defaults(func=run_auth_wizard)


def add_utils_parsers(subparsers) -> None:
    utils = subparsers.add_parser("utils", help="Inspection and maintenance helpers.")
    utils_sub = utils.add_subparsers(dest="utils_command", required=True)

    labels = utils_sub.add_parser("labels", help="Inspect labeling config.")
    add_config_arg(labels)
    labels_mode = labels.add_mutually_exclusive_group()
    labels_mode.add_argument("--list", action="store_true")
    labels_mode.add_argument("--xml", action="store_true")
    labels_mode.add_argument("--wizard", action="store_true")
    labels.add_argument("-o", "--outfile")
    labels.set_defaults(func=run_utils_labels)

    cachelabels = utils_sub.add_parser("cachelabels", help="Inspect/update cache labels.")
    add_config_arg(cachelabels)
    cache_mode = cachelabels.add_mutually_exclusive_group()
    cache_mode.add_argument("--list", action="store_true")
    cache_mode.add_argument("--update", nargs="*")
    cache_mode.add_argument("--new-anno", nargs="+")
    cache_mode.add_argument("--new-pred", nargs="+")
    add_task_selection_args(cachelabels)
    cachelabels.set_defaults(func=run_utils_cachelabels)

    datafields = utils_sub.add_parser("datafields", help="List task data fields.")
    add_config_arg(datafields)
    datafields.add_argument("--s3", action="store_true")
    datafields.set_defaults(func=run_utils_datafields)

    validate_s3 = utils_sub.add_parser("validate-s3", help="Validate S3 references.")
    add_config_arg(validate_s3)
    add_task_selection_args(validate_s3)
    validate_s3.set_defaults(func=run_utils_validate_s3)
