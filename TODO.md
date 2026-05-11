# TODO

## CLI

- Task caching: options beyond RAM; cache system needs re-evaluation/overhaul.
- Filter template fill: allow one field to be left as `{}` in a template file,
  then `--filter FILE VALUE` fills that value.
- Add `utils labels --wizard`.
- Add `utils filter-maker`.

- Implement PyPI/latest-version check for `project version`.
- Complete `tasks duplicates` semantics from CLI.md: `latest`,
  finished-annotation counts, and ambiguous tie handling.
- Support container-level `annotations` / `predictions` include/exclude fields.
- Implement `utils cachelabels --new-anno` and `--new-pred`.
- Re-evaluate `utils datafields --s3` detection against Label Studio column
  metadata and project data.

## Deferred Consideration

- Implement `ui_builder`.
- Consider whether `project_builder/` should include basics for task building.
- Build task scripts for project-specific workflows.
- Add tests for core modules, snapshot downloads, task tools, and cache-label
  updates.
- Add scripts that map filenames/filepaths to PIDs.
- Add script that compares images in S3 against a PID list.
- Add script that compares images in S3 against Label Studio project tasks,
  optionally limited to a PID list.
- Add script that creates Label Studio tasks from images in S3 with metadata
  only and no annotations.
- Add script that updates tasks with annotations/predictions based on CSV.
- Add script that moves/converts tasks and annotations from one project to
  another.
