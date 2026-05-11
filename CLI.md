# `lstool` — Command-Line Interface

A unified CLI over the `labelstudio_tools` package: auth setup, project
management, task operations, and utilities.

---

## Table of Contents

- [Installation & invocation](#installation--invocation)
- [Environment variables](#environment-variables)
- [Path resolution](#path-resolution)
- [Common args matrix](#common-args-matrix)
- [Auth Commands](#auth-commands)
  - [`auth test`](#auth-test)
  - [`auth wizard`](#auth-wizard)
- [Project Commands](#project-commands)
  - [Common project flags](#common-project-flags)
  - [`project wizard`](#project-wizard)
  - [`project list`](#project-list)
  - [`project create`](#project-create)
  - [`project update`](#project-update)
  - [`project version`](#project-version)
- [Task Commands](#task-commands)
  - [Common selection flags](#common-selection-flags)
  - [`tasks view`](#tasks-view)
  - [`tasks download`](#tasks-download)
  - [`tasks download-s3`](#tasks-download-s3)
  - [`tasks download-snapshot`](#tasks-download-snapshot)
  - [`tasks duplicates`](#tasks-duplicates)
  - [`tasks delete`](#tasks-delete)
- [Utility Commands](#utility-commands)
  - [`utils labels`](#utils-labels)
  - [`utils cachelabels`](#utils-cachelabels)
  - [`utils datafields`](#utils-datafields)
  - [`utils validate-s3`](#utils-validate-s3)
- [Exit codes](#exit-codes)

---

## Installation & invocation

`lstool` is the console-script entry point installed by the package:

```bash
pip install -e .       # editable install
lstool --help          # top-level help (lists sections, env vars, exceptions)
lstool auth --help     # group help
lstool tasks view --help
```

`python -m labelstudio_tools` is equivalent to `lstool --help` (prints the
help overview and exits).

Most commands need a project config (TOML). Exceptions that skip the
config requirement:

- `lstool project wizard --default` / `--default-inline`
- `lstool auth wizard --default`
- (future stubs may add more — see help text on each)

---

## Environment variables

| Variable               | Effect                                                                                            |
| ---------------------- | ------------------------------------------------------------------------------------------------- |
| `LSTOOL_CONFIG`        | Default path for `-c/--config` when not supplied on the command line.                             |
| `LSTOOL_CONFIG_DIR`    | Search root for relative `-c/--config` values (see [Path resolution](#path-resolution)). Also used as the `--config-dir` preset for the wizards. |
| `LSTOOL_CONFIG_AUTH`   | Fallback auth file when the project config has no `auth` field and no inline secrets. |
| `LSTOOL_TABLEFMT`      | Tabulate format used by every command that prints a table (default `github` if unset).            |

Precedence for every variable: **explicit flag > env var > built-in default**.

---

## Path resolution

When `-c/--config PATH` (or the value of `LSTOOL_CONFIG`) is **absolute**,
it's used as-is.

When the path is **relative**, resolution depends on whether
`LSTOOL_CONFIG_DIR` is set:

- **`LSTOOL_CONFIG_DIR` not set** → resolve relative to current working
  directory; `FileNotFoundError` if absent.
- **`LSTOOL_CONFIG_DIR` set** → resolve against both `$LSTOOL_CONFIG_DIR/PATH`
  and `cwd/PATH`:
  - Exactly one exists → use that one.
  - Neither exists → `FileNotFoundError` listing both candidates.
  - **Both exist → ambiguity error** (exit 1) listing both candidates. Pick
    one by supplying an absolute path, removing one of the files, or
    unsetting `LSTOOL_CONFIG_DIR`.

The same rules apply to every other file-path argument the CLI accepts —
including the `LSTOOL_CONFIG_AUTH` fallback, `--filter PATH`, and any
list-file passed to `--tasks` / `-i/--include-fields` /
`-x/--exclude-fields`. In every case a relative path is searched against
`$LSTOOL_CONFIG_DIR` and `cwd`, with the same exists-in-both → ambiguity
error.

(Paths written in a project config's `auth = "…"` field continue to
resolve relative to the config file's directory, then cwd — unchanged.)

---

## Common args matrix

Every command honors the environment variables in [Path resolution](#path-resolution)
and [Environment variables](#environment-variables): `LSTOOL_CONFIG` fills
in `-c/--config` when unset, `LSTOOL_CONFIG_DIR` is the search root for
relative paths, `LSTOOL_CONFIG_AUTH` is the auth fallback. Only the
per-command flags below differ; ✓ means the command accepts the flag /
honors that variable.

| Section  | Subcommand         | `-c/--config` | `--dry-run` | `LSTOOL_TABLEFMT` |
| -------- | ------------------ | :-----------: | :---------: | :---------------: |
| auth     | `test`             |       ✓       |      —      |         ✓         |
| auth     | `wizard`           |       —       |      —      |         —         |
| project  | `wizard`           |       —       |      —      |         —         |
| project  | `list`             |       ✓       |      —      |         ✓         |
| project  | `create`           |       ✓       |      ✓      |         —         |
| project  | `update`           |       ✓       |      ✓      |         —         |
| project  | `version`          |       ✓       |      —      |         —         |
| tasks    | `view`             |       ✓       |      —      |         ✓         |
| tasks    | `download`         |       ✓       |      —      |         —         |
| tasks    | `download-s3`      |       ✓       |      ✓      |         —         |
| tasks    | `download-snapshot`|       ✓       |      ✓      |         —         |
| tasks    | `duplicates`       |       ✓       |      —      |         ✓         |
| tasks    | `delete`           |       ✓       |      ✓      |         —         |
| utils    | `labels`           |       ✓       |      —      |         ✓         |
| utils    | `cachelabels`      |       ✓       |      —      |         ✓         |
| utils    | `datafields`       |       ✓       |      —      |         ✓         |
| utils    | `validate-s3`      |       ✓       |      —      |         ✓         |

---

## Auth Commands

Configure and verify the credentials `lstool` uses to talk to Label
Studio, S3, and ML backends. Run these before the project commands when
you're setting up a new host.

| Subcommand                  | Purpose                                                  |
| --------------------------- | -------------------------------------------------------- |
| [`auth test`](#auth-test)   | Validate LS / S3 / ML auth targets.                      |
| [`auth wizard`](#auth-wizard) | Scaffold (or interactively build) an auth file.        |

### `auth test`

```
lstool auth test (-c CONFIG | --auth AUTH | --host HOST --token TOKEN)
                 [--ls [HOST] | --storage [BUCKET] |
                  --s3 [BUCKET] | --ml [NAME_OR_URL]]
```

Validates auth targets and prints a status table.

Source modes (exactly one required; mutually exclusive):

- `-c CONFIG` → test every auth target referenced by the project config
  (LS host+token, every `[[storage]]` bucket via `head_bucket`, every
  `[ml_backend]` URL via `GET /`).
- `--auth FILE` → test every entry in a standalone auth file
  (each `[[labelstudio]]`, `[[storage]]`, `[[ml_backend]]`).
- `--host H --token T` → only the LS host check. **Cannot** be combined
  with `--ls/--storage/--s3/--ml` (there's only the one host to test).

Narrowing flags (apply only with `-c` or `--auth`):

- `--ls [HOST]` → only LS, optionally limited to one host entry.
- `--storage [BUCKET]` / `--s3 [BUCKET]` → only S3 storages, optionally one
  bucket.
- `--ml [NAME_OR_URL]` → only ML backends, optionally one entry.

Output: a table (✓/✗ + diagnostic), formatted via `LSTOOL_TABLEFMT`.

### `auth wizard`

```
lstool auth wizard [--default] [-v/--verbose]
                   [--config-dir DIR] [-o OUTFILE]
```

With `--default` → writes a stub auth TOML (`ls_auth.toml` by default)
into `--config-dir`. `--verbose` adds inline field-description comments.

Without `--default` → currently raises `NotImplementedError` (interactive
auth-only wizard is a stub).

`--config-dir` defaults to `./configs` only when `LSTOOL_CONFIG_DIR` is
unset; a `--config-dir` flag value always overrides the env var. `-o` is
the auth-file basename within `--config-dir` (or an absolute path
elsewhere). `LSTOOL_CONFIG_AUTH` presets the `-o` default.

Refuses to overwrite an existing file.

Config: NOT required.

---

## Project Commands

Manage Label Studio projects: build a new config, push it to a server
(create / update), get a project overview, and check versions. Most
commands run against a single project defined by a config file; `list` and
`version` can also operate directly off an auth file or `--host`/`--token`
for quick inspections across multiple hosts.

| Subcommand                            | Purpose                                                |
| ------------------------------------- | ------------------------------------------------------ |
| [`project wizard`](#project-wizard)   | Scaffold or interactively build a project config TOML. |
| [`project list`](#project-list)       | List projects on a host (or every host in an auth file). |
| [`project create`](#project-create)   | Create a project on LS from a config. Errors if it exists. |
| [`project update`](#project-update)   | Patch an existing project to match a config. Errors if missing. |
| [`project version`](#project-version) | Print CLI / SDK / server versions and update-check status. |

### `project wizard`

```
lstool project wizard [--default | --default-inline]
                      [-v/--verbose]
                      [--config-dir DIR] [--auth FILE] [-o OUTFILE]
```

Launches the interactive wizard (see `labelstudio_tools.config_wizard`).

Default modes (mutually exclusive — skip the wizard entirely):

- `--default` → writes stub `ls_project.toml` (no inline secrets;
  references `ls_auth.toml`) into `--config-dir`. Use
  [`auth wizard --default`](#auth-wizard) to create the matching auth
  stub.
- `--default-inline` → writes a single `ls_project.toml` with secrets
  embedded inline (no separate auth file).

`--config-dir` defaults to `./configs` only
when `LSTOOL_CONFIG_DIR` is unset; a `--config-dir` flag value always
overrides the env var.

`--verbose` adds inline field-description comments to generated stubs and
is equivalent to "Yes" on the interactive wizard's "include descriptions"
prompt.

Env-var presets used by the interactive wizard:

- `LSTOOL_CONFIG` → preset for `-o/--outfile` (project config path).
- `LSTOOL_CONFIG_DIR` → preset for `--config-dir`.
- `LSTOOL_CONFIG_AUTH` → preset for the auth-file picker (`--auth` /
  in-wizard selection).

Refuses to overwrite an existing file.

Config: NOT required.

### `project list`

```
lstool project list (-c CONFIG | --auth AUTH | --host H --token T)
                    [--counts]
```

Lists projects on the host. Auth-source modes are mutually exclusive; see
[Common project flags](#common-project-flags). Output columns: `id`,
`title` (`host` prepended when listing across multiple hosts). With
`--counts`, also adds `tasks`, `annotations`, `predictions`. Table
formatted via `LSTOOL_TABLEFMT`.

### `project create`

```
lstool project create [-c CONFIG] [--dry-run]
```

Creates a project on Label Studio from the project config. Errors out if a
project with the same title already exists on the host. Use
[`project update`](#project-update) to patch an existing one. With
`--dry-run`, prints the plan and exits without applying.

Config: required.

### `project update`

```
lstool project update [-c CONFIG] [--dry-run]
```

Patches an existing project to match the config. Errors if no project with
the same title (or `project_id` pin) exists. Same plan/apply behavior as
`create`.

Config: required.

### `project version`

```
lstool project version [-c CONFIG | --auth AUTH | --host H --token T]
                       [--nocheck]
```

Prints:

- `labelstudio-tools` package version (this CLI).
- `label_studio_sdk` version (via `importlib.metadata`) and a check against
  the latest released version on PyPI.
- Label Studio server version from `GET {host}/api/version` for every host
  reachable through the chosen auth source. (No auth source given → just
  the package + SDK lines, no server check.)

When the auth source is a project config or auth file with multiple
`[[labelstudio]]` entries, server versions are reported per host.

`--nocheck` skips the PyPI / network update checks (offline-friendly). The
release-check is stubbed with `NotImplementedError` until wired; package +
SDK + server versions still print.

---

## Task Commands

Read, export, and clean up the tasks in a Label Studio project. Every
`tasks` subcommand reads a project config (via `-c/--config` or
`LSTOOL_CONFIG`) and shares the [common selection flags](#common-selection-flags)
below.

| Subcommand                                                | Purpose                                                       |
| --------------------------------------------------------- | ------------------------------------------------------------- |
| [`tasks view`](#tasks-view)                               | Print one or more tasks (table / JSON / JSONL / CSV).         |
| [`tasks download`](#tasks-download)                       | Export selected tasks (with annotations) to a file.           |
| [`tasks download-s3`](#tasks-download-s3)                 | Download S3 objects referenced by task data fields.           |
| [`tasks download-snapshot`](#tasks-download-snapshot)     | Whole-project / filter-view export via `SnapshotManager`.     |
| [`tasks duplicates`](#tasks-duplicates)                   | Find and (optionally) remove duplicate tasks by `pk`.         |
| [`tasks delete`](#tasks-delete)                           | Delete selected tasks from the project.                       |

### Common selection flags

Selection and field-scoping flags shared by tasks subcommands. Same
selection semantics also apply to `utils validate-s3` and (where
indicated) `utils cachelabels`.

#### `--tasks` / positional task ids

Three input styles, wherever a flag is offered:

- A space-separated list of integers (task ids): `--tasks 1 2 3 4`
- A space-separated list of pk-values: `--tasks ID0042 ID0099 ID0101`
  (resolved against `task.data[pk]` from the `[labelstudio-tools]` section)
- A single path to a line-delimited text file: `--tasks ids.txt`
  (one id or pk-value per line; blank lines and `#` comments allowed)

`tasks view` additionally accepts the same values as **positional**
arguments (`lstool tasks view 1 2 3` ≡ `lstool tasks view --tasks 1 2 3`).

Mutually exclusive with `--filter`.

#### `--filter`

```
--filter PATH                 Path to a filter file (.json or .toml).
```

Format produced by future `utils filter-maker` (see roadmap). Mutually
exclusive with `--tasks` / positional ids.

#### `-i/--include-fields`, `-x/--exclude-fields`

Field-scoping for task output. Multi-valued; same file/value duality:

- Space-separated list of strings: `-i image caption id`
- A single path to a line-delimited text file: `-i fields.txt`

Names refer to top-level task fields (`id`, `created_at`, `data`,
`annotations`, …). The special name `data` includes every sub-key of
`task.data`. `annotations` and `predictions` as containers are NOT yet
implemented (raise `NotImplementedError`); list explicit sub-fields if
needed.

Available on every `tasks` command **except** `duplicates` and `delete`.

### `tasks view`

```
lstool tasks view [-c CONFIG]
                  [TASK [TASK ...]] [--tasks TASK [TASK ...] | --filter PATH]
                  [-i/--include-fields F [F ...]]
                  [-x/--exclude-fields F [F ...]]
                  [--format {table,json,jsonl,csv}]
```

Prints one or more tasks. Selection by positional args, `--tasks`, or
`--filter` (mutually exclusive). Each value can be a task id (int) or a
pk-value resolved against `task.data[pk]`.

Format default: `table` (uses `LSTOOL_TABLEFMT`).

Config: required.

### `tasks download`

```
lstool tasks download [-c CONFIG] -o/--outfile OUTFILE
                      (--tasks TASK [TASK ...] | --filter PATH)
                      [-i/--include-fields F [F ...]]
                      [-x/--exclude-fields F [F ...]]
                      [-q/--quiet]
```

Downloads selected tasks (with annotations) to `OUTFILE`. Format inferred
from extension (`.json` / `.jsonl` / `.csv`).

If neither `--tasks` nor `--filter` is given, prints a warning recommending
[`tasks download-snapshot`](#tasks-download-snapshot) for whole-project
exports, then proceeds. Use `-q/--quiet` to skip the prompt.

Config: required.

### `tasks download-s3`

```
lstool tasks download-s3 [-c CONFIG] -o/--outdir DIR
                         (--tasks TASK [TASK ...] | --filter PATH)
                         --field FIELD [FIELD ...]
                         [--pattern PATTERN]
                         [-i/--include-fields F [F ...]]
                         [-x/--exclude-fields F [F ...]]
                         [--dry-run]
```

Downloads the S3 object referenced at `task.data[FIELD]` (per task,
per field) into `DIR`. Multiple `--field` values may be passed; the
`--pattern` placeholders disambiguate per-field destinations.

Requirements:

- At least one of `--tasks` / `--filter` (selection is required).
- `--field` is required. If any provided `FIELD` is not an S3-bearing
  data field on this project, the command errors out and prompts:
  `run 'lstool utils datafields --s3'` to list valid choices.

Default pattern:

```
{id:05}_{pk_fieldname}{s3_ext}
```

Pattern placeholders:

| Placeholder         | Value                                                |
| ------------------- | ---------------------------------------------------- |
| `{id}`              | task id (supports format spec, e.g. `{id:05}`)       |
| `{pk}`              | value of `task.data[pk]` (the pk field's value)      |
| `{pk_fieldname}`    | name of the pk field (e.g. `image`)                  |
| `{field}`           | value of `task.data[--field]` for the current field  |
| `{s3_key}`          | full S3 object key                                   |
| `{s3_basename}`     | basename of the S3 object key                        |
| `{s3_ext}`          | extension of the S3 object key (e.g. `.jpg`); empty if absent |
| `{<DATAFIELD>}`     | value of any other `task.data[<DATAFIELD>]` or top-level task field |

Diagnostics:

- `--dry-run` → resolve every filename pattern and print what would be
  downloaded. No downloads.

Config: required.

### `tasks download-snapshot`

```
lstool tasks download-snapshot [-c CONFIG]
                               (--list | --snap SNAP_ID | --filterview NAME)
                               [-o/--outfile OUTFILE]
                               [-i/--include-fields F [F ...]]
                               [-x/--exclude-fields F [F ...]]
                               [--dry-run]
```

Wraps `SnapshotManager`. Whole-project (or filterview-scoped) export, with
annotations.

Exactly one of:

- `--list` → list available snapshots & filterviews on the project.
  `-o/--outfile` not required.
- `--snap SNAP_ID` → download an existing snapshot by id.
- `--filterview NAME` → create a fresh snapshot scoped to a filter view,
  then download.

With `--snap` / `--filterview`, `-o/--outfile` is required. Format inferred
from extension (`.json` / `.jsonl` / `.csv`).

`--dry-run` shows what would be created / fetched without performing it.

Config: required.

### `tasks duplicates`

```
lstool tasks duplicates [-c CONFIG]
                        [--list]
                        [--keep {first,latest,most-annotated}]
                        [-q/--quiet]
```

Finds duplicate tasks by `task.data[pk]` (pk from
`[labelstudio-tools]`).

- `--list` (alias `--dry-run`) → show duplicate groups (no kept-marker).
- `--list --keep KEEP` → show groups + marks which task in each group
  would be kept. No deletions.
- `--keep KEEP` (alone) → delete losers in each group, keeping one per
  `--keep` strategy. Asks for confirmation before deleting.

`--keep` strategies (no default — must be specified):

- `first` — earliest `created_at`.
- `latest` — most recent `created_at`.
- `most-annotated` — most *finished* annotations. Ties → no deletions, the
  command prints the ambiguous groups and exits non-zero. With
  `-q/--quiet`, ambiguous groups are silently skipped and unambiguous
  groups proceed to deletion (`-q` also skips the confirmation prompt).

Output formatted via `LSTOOL_TABLEFMT`.

Config: required.

### `tasks delete`

```
lstool tasks delete [-c CONFIG]
                    (--tasks TASK [TASK ...] | --filter PATH)
                    [--dry-run] [-q/--quiet]
```

Deletes selected tasks from the project. Exactly one of `--tasks` /
`--filter` is required (mutually exclusive). Asks for confirmation before
deleting; `-q/--quiet` skips the prompt. With `--dry-run`, lists matching
tasks without deleting.

Config: required.

---

## Utility Commands

Read-only inspection commands and project-side maintenance helpers.
`cachelabels` and `validate-s3` accept the
[common selection flags](#common-selection-flags) (`--tasks` / `--filter`)
to scope their work.

| Subcommand                                          | Purpose                                                       |
| --------------------------------------------------- | ------------------------------------------------------------- |
| [`utils labels`](#utils-labels)                     | List annotation control tags or dump the labeling-config XML. |
| [`utils cachelabels`](#utils-cachelabels)           | Inspect / refresh / create LS "cache labels" data fields.     |
| [`utils datafields`](#utils-datafields)             | List distinct `task.data` keys (optionally S3-bearing only).  |
| [`utils validate-s3`](#utils-validate-s3)           | HEAD-check every S3 reference and report broken ones.         |

### `utils labels`

```
lstool utils labels [-c CONFIG] [--list | --xml] [-o OUTFILE]
```

Inspects the project's labeling configuration.

- `--list` (default if no flag) → list annotation control tags as a table
  (name, type, to-name, labels). Uses `LSTOOL_TABLEFMT`.
- `--xml` → dump the raw `label_config` XML to stdout, or to `-o OUTFILE`
  if given.

Config: required.

### `utils cachelabels`

```
lstool utils cachelabels [-c CONFIG]
                         [--list | --update [TAG ...] |
                          --new-anno TAG | --new-pred TAG]
                         [--tasks TASK [TASK ...] | --filter PATH]
```

Operates on Label Studio's experimental "cache labels" feature, which
copies annotation/prediction values into per-tag data fields named
`cache_<tag>` / `cache_predictions_<tag>` (then usable as filter fields).

Sub-actions (mutually exclusive, only one per invocation):

- `--list` (default) → show current state:
  - cached annotation tags (`cache_<tag>` data fields present)
  - cached prediction tags (`cache_predictions_<tag>` data fields present)
  - available control tags from the labeling config
- `--update` → refresh **all** existing cache fields server-side.
- `--update TAG [TAG ...]` → refresh only the named tags.
- `--new-anno TAG [TAG ...]` → create a new cache from the named annotation control
  tag.
- `--new-pred TAG [TAG ...]` → create a new cache from the named prediction control
  tag.

`--update` / `--new-anno` / `--new-pred` accept the [common selection flags](#common-selection-flags)
`--tasks` / `--filter` to scope the operation to specific tasks (otherwise
the whole project).

Backed by future `TaskManager` methods; sub-actions raise
`NotImplementedError` until those land. `--list` works as soon as the
discovery helper is in place.

Output formatted via `LSTOOL_TABLEFMT`.

Config: required.

### `utils datafields`

```
lstool utils datafields [-c CONFIG] [--s3]
```

Lists distinct keys observed across `task.data` for the project (via a
project-side LS endpoint — no per-task enumeration).

- `--s3` → show only fields whose values are S3 storage paths.

Output formatted via `LSTOOL_TABLEFMT`. Stubs with `NotImplementedError`
if no helper exists in `TaskManager`/`ProjectManager` yet.

Config: required.

### `utils validate-s3`

```
lstool utils validate-s3 [-c CONFIG]
                         [--tasks TASK [TASK ...] | --filter PATH]
```

For each selected task (or every task if no selection), HEADs every
S3-referenced object to confirm it exists. Prints a table of broken
references; exits non-zero if any are missing.

`--tasks` / `--filter` follow the same format as
[Common selection flags](#common-selection-flags).

Output formatted via `LSTOOL_TABLEFMT`.

Config: required.

---

## Exit codes

| Code | Meaning                                                            |
| ---- | ------------------------------------------------------------------ |
| 0    | Success (or a dry-run that resolved cleanly).                      |
| 1    | Error printed to stderr (bad input, network, auth, ambiguity, path-ambiguity, …). |
| 2    | argparse usage error (unknown flag, missing required arg, …).      |
| 130  | Interrupted (Ctrl+C).                                              |
