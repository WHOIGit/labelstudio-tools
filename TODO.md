# TODO

## CLI

- Task caching: options beyond RAM; cache system needs re-evaluation/overhaul.
- Filter template fill: allow one field to be left as `{}` in a template file,
  then `--filter FILE VALUE` fills that value.
- Add `utils labels --wizard`.
- Add `utils filter-maker`.
- Implement PyPI/latest-version check for `project version`.
- Support container-level `annotations` / `predictions` include/exclude fields.
- Implement `utils cachelabels --new-anno` and `--new-pred`.
- Re-evaluate `utils datafields --s3` detection against Label Studio column
  metadata and project data.

## Library

- Add focused tests for `config.load_config()` auth merge precedence, missing
  auth behavior, singular vs array table normalization, and env substitution
  decision.
- Add `ProjectManager.plan_config()` tests with fake SDK clients, including
  redaction, storage directions, ML backend payloads, project-id pinning, and
  create/update/noop diffs.
- Add `ProjectManager.create_project_from_config()` tests with fake SDK clients.
- Add `TaskManager.get_tasks()` tests for pagination, filters, views, field
  inclusion, and error responses.
- Add idempotent task tests for `create_task()` and `create_tasks()`.
- Add duplicate detection/deletion strategy tests.
- Add cache-label update chunking and failure-handling tests.
- Add S3 helper tests for URL parsing, exists checks, upload/download paths,
  and filename-only downloads.
- Add `SnapshotManager` lifecycle tests with mocked SDK export calls.
- Add `task_filtering.parse_task_filter()` tests for bad inputs and data-field
  validation.
- Add `ui_builder.build_label_config()` tests for success and malformed
  XML/YAML paths.
- Convert print-heavy manager methods to return structured report data, then
  let CLI code render those reports.
- Decide whether env-var substitution should be supported in TOML configs.
- Remove or repurpose the now-empty `project_builder/` package directory.
- Document or remove deprecated `read_token()` in a future breaking release.

## README Follow-Up

- Add a short config/auth precedence section.
- Add a table of stable vs experimental APIs.
- Add one realistic `create_tasks()` example showing expected task payload
  shape.
- Add one `ProjectManager.plan_config()` output example after redaction.
- Add a `SnapshotManager.from_config()` example with output file handling.
- Mention that `read_token()` is deprecated.

## Deferred Consideration

- Consider whether a future project-builder package should include basics for
  task building.
- Build task scripts for project-specific workflows.
- Add scripts that map filenames/filepaths to PIDs.
- Add script that compares images in S3 against a PID list.
- Add script that compares images in S3 against Label Studio project tasks,
  optionally limited to a PID list.
- Add script that creates Label Studio tasks from images in S3 with metadata
  only and no annotations.
- Add script that updates tasks with annotations/predictions based on CSV.
- Add script that moves/converts tasks and annotations from one project to
  another.

## Closed May 11

- Switched public docs away from the old facade-class reference and to
  `TaskManager`, `ProjectManager`, and `SnapshotManager`.
- Standardized package metadata to Python `>=3.11`.
- Added `webcolors` for config wizard named colors.
- Added `PyYAML` for `ui_builder`.
- Moved `ui_builder` to `labelstudio_tools.ui_builder`.
- Deprecated implicit token-file reading via `read_token()` and stopped using
  it in managers.
- Centralized legacy `Token` vs modern `Bearer` auth header selection.
- Standardized core `load_config()` to honor `LSTOOL_CONFIG_AUTH` fallback.
- Added `SnapshotManager.from_config()`.
- Redacted secret-like fields in project plan output.
- Replaced several runtime `assert` and failure `print()` paths with
  exceptions.
- Fixed `TaskManager.get_tasks()` filter pagination mutation.
- Fixed `TaskManager.task_exists(..., use_cache=False)` for full task dicts.
- Fixed `TaskManager.create_tasks(..., dry_run=True)` report state.
- Standardized `TaskManager.remove_duplicate_tasks()` strategy names to
  `first`, `latest`, and `most-annotated`.
- Fixed cache-label chunk size handling.
- Fixed S3 downloads to filenames in the current directory.
- Fixed `task_filtering.py` column filtering and validation style.
- Removed generated `__pycache__` directories from `src/`.
