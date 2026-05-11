# Labelstudio Tools

`labelstudio-tools` is a Python 3.11+ library and CLI for working with Label
Studio projects idempotently. It extends the official `label-studio-sdk` with
helpers for project setup, bulk task management, snapshots, S3 references, cache
labels, and Label Studio task/annotation payload construction.

## Installation

```bash
pip install git+https://github.com/WHOIGit/labelstudio-tools.git
```

For development:

```bash
pip install -e ".[dev]"
lstool --help
```

## CLI Quickstart

```bash
lstool auth wizard --default
lstool project wizard --default
lstool project list
lstool tasks view --format table
```

The CLI honors:

- `LSTOOL_CONFIG`: default project config path.
- `LSTOOL_CONFIG_DIR`: search root for relative config/auth/filter paths.
- `LSTOOL_CONFIG_AUTH`: fallback auth file when the project config has no
  `auth` field and no inline secrets.
- `LSTOOL_TABLEFMT`: table format used by table-printing commands.

See `CLI.md` for the full command surface.

## Library Quickstart

Use `TaskManager` for task-centric workflows:

```python
from labelstudio_tools import TaskManager

tasks = TaskManager.from_config("configs/ls_project.toml")

rows = tasks.get_tasks(limit_fields_to=["id", "data"])
report, responses = tasks.create_tasks(
    tasks=my_task_payloads,
    pk_datafields="image",
    dry_run=True,
)
```

Use `ProjectManager` for project-level resources:

```python
from labelstudio_tools import ProjectManager

projects = ProjectManager.from_config("configs/ls_project.toml")
plan = projects.plan_config("configs/ls_project.toml")
projects.create_project_from_config("configs/ls_project.toml", dry_run=True)
```

Use `SnapshotManager` for export snapshots:

```python
from labelstudio_tools import SnapshotManager

snapshots = SnapshotManager.from_config("configs/ls_project.toml")
snapshots.make_snapshot()
snapshots.wait_for_snapshot_completion()
data = snapshots.download_snap()
```

## Config Files

Project configs are TOML. Secrets can live inline, but the recommended pattern
is a project config plus a sidecar auth file.

```toml
# configs/ls_project.toml
host = "https://labelstudio.example.org"
project = "Demo Project"
label_config = "label_ui.xml"
auth = "ls_auth.toml"

[labelstudio-tools]
pk = "image"
cache = "RAM"

[[storage]]
type = "s3"
mode = "source"
title = "demo source"
bucket = "my-bucket"
endpoint_url = "https://s3.example.org"
bucket_prefix = "tasks/"
```

```toml
# configs/ls_auth.toml
[[labelstudio]]
host = "https://labelstudio.example.org"
token = "LABEL_STUDIO_TOKEN"

[[storage]]
type = "s3"
bucket = "my-bucket"
endpoint_url = "https://s3.example.org"
aws_access_key_id = "..."
aws_secret_access_key = "..."
```

`LSTOOL_CONFIG_AUTH` is also honored by the core config loader when a project
config does not specify `auth` and does not include inline secrets.

## Idempotent Task Management

`TaskManager.create_task()` and `TaskManager.create_tasks()` use primary-key
data fields to skip tasks that already exist in Label Studio.

```python
report, responses = tasks.create_tasks(
    tasks=my_task_payloads,
    pk_datafields="image",
    force_recache=True,
)
```

Useful helpers include:

- `cache_tasks()` and `cache_task_by_pk()` for repeated existence checks.
- `task_exists()` for one-off idempotent checks.
- `find_duplicate_tasks()` and `remove_duplicate_tasks()` for duplicate cleanup.
- `update_task()`, `add_annotation()`, and `add_prediction()` for direct task
  mutation.

## Retrieval, Filters, And Views

```python
from labelstudio_tools.utils import simple_task_filter_builder

task_filter = simple_task_filter_builder(
    field="dataset",
    value="train",
    operator="equal",
)

subset = tasks.get_tasks(
    with_annotations=True,
    filter_dict=task_filter,
    view="Review Queue",
)
```

`TaskManager` also exposes `list_views()`, `get_view()`, `data_fields()`, and
label-config inspection helpers such as `config_control_labels()`.

## S3 Helpers

When a project config includes S3 storage credentials, `TaskManager` can validate
and transfer S3 objects referenced by task data:

```python
exists = tasks.s3url_exists("s3://my-bucket/path/image.jpg")
tasks.download_s3url("s3://my-bucket/path/image.jpg", "downloads/image.jpg")
tasks.upload_s3url("local/file.jpg", "s3://my-bucket/path/file.jpg")
```

Lower-level helpers are available from `labelstudio_tools.utils`.

## Task Payload Helpers

`taskclass.py` contains base classes and helpers for building Label Studio
task/annotation payloads:

```python
from labelstudio_tools import BBox, ResultField, BaseRegion, BaseAnnotation, BaseTask
```

These are intended for project-specific task builders that need consistent
Label Studio result dictionaries.

## Experimental Areas

The cache-label helpers wrap Label Studio's experimental cache-label action:

```python
tasks.update_cachelabels(
    control_tags=["label"],
    with_counters=False,
    from_predictions=False,
    timeout_groups="auto",
)
```

The labeling UI builder is available as `labelstudio_tools.ui_builder` and
requires YAML input plus an XML template.

## Status

Stable enough for day-to-day use:

- `TaskManager.from_config()`
- `ProjectManager.from_config()`
- idempotent task creation
- task retrieval/filtering
- project config planning/application
- S3 existence/upload/download helpers
- snapshot export helpers

Still being tightened:

- cache-label creation and refresh ergonomics
- CLI coverage for every library helper
- file/folder-backed task caching beyond RAM
- generated filter builders

## License

MIT License
