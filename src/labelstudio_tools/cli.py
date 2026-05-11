"""Top-level ``lstool`` command-line interface."""

from __future__ import annotations

import argparse
import sys

import requests

from .cli_project import add_project_parsers
from .cli_tasks import add_tasks_parsers
from .cli_utils import CliError, CliExit, add_auth_parsers, add_utils_parsers


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lstool",
        description="Unified CLI for labelstudio-tools.",
        epilog=(
            "Environment: LSTOOL_CONFIG, LSTOOL_CONFIG_DIR, "
            "LSTOOL_CONFIG_AUTH, LSTOOL_TABLEFMT."
        ),
    )
    subparsers = parser.add_subparsers(dest="section", required=True)
    add_auth_parsers(subparsers)
    add_project_parsers(subparsers)
    add_tasks_parsers(subparsers)
    add_utils_parsers(subparsers)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        parser.print_help()
        return 0
    try:
        args = parser.parse_args(argv)
        return int(args.func(args) or 0)
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130
    except CliExit as exc:
        return exc.code
    except NotImplementedError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except (CliError, FileNotFoundError, ValueError, requests.RequestException) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
