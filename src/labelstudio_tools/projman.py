import json
import os
from typing import Union

import requests
from urllib.parse import urljoin
from label_studio_sdk.client import LabelStudio

from .config import load_config
from .utils import read_token, attr_list_decorator, s3_read_config


class ProjectManager:
    """Project-level Label Studio management (CE-compatible)."""

    def __init__(self, host: str, token: str):
        self.host = host
        self.token = read_token(token)
        self.client = LabelStudio(base_url=self.host, api_key=self.token)

    @classmethod
    def from_config(cls, config: Union[str, dict],
                    auth_config: Union[str, dict] = None):
        """Build a ProjectManager from a project config (with optional auth file).

        Auth resolution layers (each overrides the previous):
          1. `config['auth']` — string path to a sidecar auth file. Resolved
             relative to the config file's directory first, then cwd (with a
             warning). The file is the *base* layer.
          2. Inline secrets in `config` — override file values; a UserWarning
             fires when an inline value differs from the file value.
          3. `auth_config` parameter (path or dict) — silently overrides
             everything below.

        Auth-file load failure is deferred: if the file doesn't exist, a
        FileNotFoundError is only raised when a required value (e.g. `token`)
        isn't already present inline. ML-backend auth (`user`/`pass`) is
        always optional.
        """
        merged = load_config(config, auth_config)
        return cls(host=merged['host'], token=merged['token'])

    @property
    def headers(self) -> dict:
        is_legacy_token = len(self.token) <= 40
        auth_type = 'Token' if is_legacy_token else 'Bearer'
        return {'Content-Type': 'application/json',
                'Authorization': f'{auth_type} {self.token}'}

    # --- Project CRUD ---

    @attr_list_decorator
    def list_projects(self):
        return self.client.projects.list()

    def get_project(self, project: Union[int, str]):
        if isinstance(project, int):
            return self.client.projects.get(id=project)
        elif isinstance(project, str):
            projects = self.list_projects()
            matches = [p for p in projects if project in p.title]
            exact_matches = [p for p in matches if project == p.title]
            if len(exact_matches) == 1:
                return exact_matches[0]
            elif len(matches) == 1:
                return matches[0]
            elif len(matches) == 0:
                raise ValueError(f"No project found with name containing '{project}'")
            else:
                raise ValueError(f"Multiple projects found with name containing '{project}': "
                                 f"{ {p.id: p.title for p in matches} }")
        elif project is None:
            return None
        else:
            raise ValueError("Project must be an integer ID, a string name, or None.")

    def create_project(self, title: str, label_config: str = None,
                       label_config_file: str = None, description: str = None, **kwargs):
        if label_config_file is not None:
            with open(label_config_file, 'r') as f:
                label_config = f.read()
        return self.client.projects.create(
            title=title, label_config=label_config, description=description, **kwargs)

    def plan_config(self, config: Union[str, dict],
                    auth_config: Union[str, dict] = None,
                    base_dir: str = None) -> list:
        """Compute the changes that create_project_from_config would apply.

        See `from_config` for how `auth_config` is resolved.

        Returns a list of plan items, each a dict:
          {'kind':    'project' | 'storage' | 'ml_backend',
           'title':   str,
           'action':  'create' | 'update' | 'noop',
           'id':      int | None,                       # None for create
           'changes': {field: (current, new)} | None,   # for action='update'
           'fields':  dict | None}                      # for action='create'

        Storage and ml_backend planning is skipped when the project does not yet
        exist (cannot list children of a non-existent project) — those items
        will appear as 'create' on the actual run after the project exists.
        """
        if base_dir is None and isinstance(config, str):
            cfg_dir = os.path.dirname(os.path.abspath(config))
            base_dir = os.path.dirname(cfg_dir)
        config = load_config(config, auth_config)

        plan = []

        # --- Project ---
        desired_proj = self._project_kwargs(config, base_dir)
        title = desired_proj.get('title')
        # `project_id` in the config explicitly pins the lookup; otherwise we
        # match by title. A pinned id that doesn't exist is an error (we don't
        # silently fall back to creating).
        pinned_id = config.get('project_id')
        if pinned_id is not None:
            try:
                existing_proj = self.client.projects.get(id=pinned_id)
            except Exception as e:
                raise ValueError(
                    f"project_id={pinned_id} from config not found on this LS instance"
                ) from e
        else:
            existing_proj = next(
                (p for p in self.list_projects() if p.title == title), None)
        if existing_proj is None:
            plan.append({'kind': 'project', 'title': title, 'action': 'create',
                         'id': None, 'changes': None, 'fields': desired_proj})
            proj_id = None
        else:
            diff = _diff_kwargs(
                existing_proj, desired_proj,
                # LS strips trailing whitespace from label_config on storage.
                normalize={'label_config': lambda s: s.rstrip() if isinstance(s, str) else s},
            )
            plan.append({'kind': 'project', 'title': title,
                         'action': 'update' if diff else 'noop',
                         'id': existing_proj.id,
                         'changes': diff or None, 'fields': None})
            proj_id = existing_proj.id

        # --- Storages ---
        existing_storages = self.list_import_storages(proj_id) if proj_id else []
        for storage_cfg in config.get('storage', []):
            desired = self._storage_kwargs(storage_cfg)
            s_title = desired['title']
            existing = next((s for s in existing_storages if s.title == s_title), None)
            if existing is None:
                plan.append({'kind': 'storage', 'title': s_title, 'action': 'create',
                             'id': None, 'changes': None, 'fields': desired})
            else:
                diff = _diff_kwargs(
                    existing, desired,
                    # Secrets are write-only; LS does not return them.
                    ignore=('aws_access_key_id', 'aws_secret_access_key'),
                )
                plan.append({'kind': 'storage', 'title': s_title,
                             'action': 'update' if diff else 'noop',
                             'id': existing.id,
                             'changes': diff or None, 'fields': None})

        # --- ML backend ---
        if 'ml_backend' in config:
            desired = self._ml_kwargs(config['ml_backend'])
            ml_title = desired['title']
            existing_backends = self.client.ml.list(project=proj_id) if proj_id else []
            existing = next((b for b in existing_backends if b.title == ml_title), None)
            if existing is None:
                plan.append({'kind': 'ml_backend', 'title': ml_title, 'action': 'create',
                             'id': None, 'changes': None, 'fields': desired})
            else:
                diff = _diff_kwargs(
                    existing, desired,
                    ignore=('basic_auth_pass',),  # write-only
                    normalize={'extra_params': _normalize_jsonish},
                )
                plan.append({'kind': 'ml_backend', 'title': ml_title,
                             'action': 'update' if diff else 'noop',
                             'id': existing.id,
                             'changes': diff or None, 'fields': None,
                             # LS PATCH requires url in payload (healthcheck_),
                             # so we stash the full desired kwargs to resend.
                             'desired': desired})

        return plan

    def create_project_from_config(self, config: Union[str, dict],
                                   auth_config: Union[str, dict] = None,
                                   base_dir: str = None,
                                   dry_run: bool = False):
        """Create or patch project + storages + ML backend from a config.

        Idempotent: existing project/storage/ml-backend are patched only on
        field-level differences. The plan is printed before any apply.

        See `from_config` for how `auth_config` is resolved and `plan_config`
        for the plan schema. If `dry_run=True`, prints the plan and returns None.
        """
        if base_dir is None and isinstance(config, str):
            cfg_dir = os.path.dirname(os.path.abspath(config))
            base_dir = os.path.dirname(cfg_dir)
        plan = self.plan_config(config, auth_config, base_dir=base_dir)
        print_config_plan(plan)
        if dry_run:
            return None

        project_obj = None
        proj_id = None
        for item in plan:
            kind, action = item['kind'], item['action']
            if kind == 'project' and item['id'] is not None:
                # Capture id even on noop so we can return the project at the end.
                proj_id = item['id']
            if action == 'noop':
                continue
            if kind == 'project':
                if action == 'create':
                    sdk_f = {k: v for k, v in item['fields'].items()
                             if k not in _UNTYPED_PROJECT_FIELDS}
                    raw_f = {k: v for k, v in item['fields'].items()
                             if k in _UNTYPED_PROJECT_FIELDS}
                    project_obj = self.client.projects.create(**sdk_f)
                    proj_id = project_obj.id
                    if raw_f:
                        self._raw_patch_project(proj_id, raw_f)
                    print(f"  -> created project '{project_obj.title}' (id={project_obj.id})")
                else:  # update
                    proj_id = item['id']
                    fields = {k: new for k, (_, new) in item['changes'].items()}
                    sdk_f = {k: v for k, v in fields.items()
                             if k not in _UNTYPED_PROJECT_FIELDS}
                    raw_f = {k: v for k, v in fields.items()
                             if k in _UNTYPED_PROJECT_FIELDS}
                    if sdk_f:
                        self.client.projects.update(id=proj_id, **sdk_f)
                    if raw_f:
                        self._raw_patch_project(proj_id, raw_f)
                    print(f"  -> updated project (id={proj_id}): {sorted(fields)}")
            elif kind == 'storage':
                if action == 'create':
                    self.client.import_storage.s3.create(project=proj_id, **item['fields'])
                    print(f"  -> created storage '{item['title']}'")
                else:
                    fields = {k: new for k, (_, new) in item['changes'].items()}
                    self.client.import_storage.s3.update(id=item['id'], **fields)
                    print(f"  -> updated storage '{item['title']}' (id={item['id']}): {sorted(fields)}")
            elif kind == 'ml_backend':
                if action == 'create':
                    self.client.ml.create(project=proj_id, **item['fields'])
                    print(f"  -> created ml_backend '{item['title']}'")
                else:
                    # LS's ML PATCH validator calls healthcheck_(**attrs) and
                    # setup_(**attrs), which require `url` and `project` in the
                    # payload — partial-update with only changed fields 500s
                    # when those aren't included. Resend the full desired
                    # kwargs plus `project`. Display still shows the diff.
                    full = item.get('desired') or {
                        k: new for k, (_, new) in item['changes'].items()}
                    self.client.ml.update(id=item['id'], project=proj_id, **full)
                    print(f"  -> updated ml_backend '{item['title']}' (id={item['id']}): "
                          f"{sorted(item['changes'])}")

        if project_obj is None and proj_id is not None:
            project_obj = self.get_project(proj_id)
        return project_obj

    # --- Kwargs builders (shared by plan + apply) ---

    def _project_kwargs(self, project_config: dict,
                        base_dir: str = None) -> dict:
        kwargs = {}
        if 'project' in project_config:
            kwargs['title'] = project_config['project']
        general = project_config.get('general', {})
        if 'description' in general:
            kwargs['description'] = general['description']
        if 'task_sampling' in general:
            kwargs['sampling'] = _SAMPLING_MAP.get(
                general['task_sampling'], general['task_sampling'])
        if 'color' in general:
            kwargs['color'] = general['color']
        if 'label_config' in project_config:
            path = project_config['label_config']
            if base_dir and not os.path.isabs(path):
                path = os.path.join(base_dir, path)
            with open(path) as f:
                kwargs['label_config'] = f.read()
        annot = project_config.get('annotations', {})
        if 'instructions' in annot:
            kwargs['expert_instruction'] = annot['instructions']
        if 'show_before_labeling' in annot:
            kwargs['show_instruction'] = bool(annot['show_before_labeling'])
        prelabeling = annot.get('prelabeling', {})
        if 'enable' in prelabeling:
            kwargs['evaluate_predictions_automatically'] = bool(prelabeling['enable'])
            kwargs['show_collab_predictions'] = bool(prelabeling['enable'])
        if 'model_name' in prelabeling:
            kwargs['model_version'] = prelabeling['model_name']
        ml = project_config.get('ml_backend', {})
        if 'start_training_on_annotation_update' in ml:
            kwargs['start_training_on_annotation_update'] = bool(
                ml['start_training_on_annotation_update'])
        return kwargs

    def _storage_kwargs(self, storage_cfg: dict) -> dict:
        """Translate a (already auth-merged) storage entry → s3.create/update kwargs."""
        bucket = storage_cfg.get('bucket')
        endpoint_url = storage_cfg.get('endpoint_url')
        kwargs = {
            'title': storage_cfg.get('title') or bucket or 's3',
            'bucket': bucket,
            's3endpoint': endpoint_url,
        }
        for k in ('aws_access_key_id', 'aws_secret_access_key',
                  'aws_session_token', 'aws_sse_kms_key_id'):
            if storage_cfg.get(k) is not None:
                kwargs[k] = storage_cfg[k]
        if 'bucket_prefix' in storage_cfg:
            kwargs['prefix'] = storage_cfg['bucket_prefix']
        if 'presigned_urls' in storage_cfg:
            kwargs['presign'] = bool(storage_cfg['presigned_urls'])
        if 'presigned_urls_expiry' in storage_cfg:
            kwargs['presign_ttl'] = int(storage_cfg['presigned_urls_expiry'])
        if 'file_name_filter' in storage_cfg:
            kwargs['regex_filter'] = storage_cfg['file_name_filter']
        if 'scan_all_subfolders' in storage_cfg:
            kwargs['recursive_scan'] = bool(storage_cfg['scan_all_subfolders'])
        if 'import_method' in storage_cfg:
            # 'tasks' = JSON task files; 'blobs' = treat each file as a media blob
            kwargs['use_blob_urls'] = (storage_cfg['import_method'] == 'blobs')
        return kwargs

    def _raw_patch_project(self, project_id: int, fields: dict) -> None:
        url = urljoin(self.host, f'/api/projects/{project_id}/')
        resp = requests.patch(url, headers=self.headers, json=fields)
        resp.raise_for_status()

    def _ml_kwargs(self, ml_cfg: dict) -> dict:
        """Translate a (already auth-merged) ml_backend entry → ml.create/update kwargs."""
        name = ml_cfg.get('name')
        backend_url = ml_cfg.get('backend_url')
        kwargs = {
            'title': name or backend_url,
            'url': backend_url,
            'is_interactive': bool(ml_cfg.get('interactive', False)),
        }
        if 'extra_params' in ml_cfg:
            # The ML backend SDK's cache (cache.py: "Value must be a string")
            # requires extra_params to round-trip as a JSON string, not a dict.
            ep = ml_cfg['extra_params']
            kwargs['extra_params'] = ep if isinstance(ep, str) else json.dumps(ep)
        if ml_cfg.get('user') is not None:
            kwargs['basic_auth_user'] = ml_cfg['user']
            kwargs['auth_method'] = 'BASIC_AUTH'
        if ml_cfg.get('pass') is not None:
            kwargs['basic_auth_pass'] = ml_cfg['pass']
        return kwargs

    def project_info(self, project: Union[int, str]) -> dict:
        proj = self.get_project(project)
        return {
            'id': proj.id,
            'title': proj.title,
            'description': proj.description,
            'task_number': proj.task_number,
            'total_annotations_number': proj.total_annotations_number,
            'total_predictions_number': proj.total_predictions_number,
            'label_config': proj.label_config,
            'created_at': str(proj.created_at),
        }

    # --- S3 Storage ---

    def add_s3_export_storage(self, project_id: int, bucket: str, prefix: str = None, **s3_kwargs):
        ...

    def sync_s3_import_storage(self, storage_id: int):
        ...

    def list_import_storages(self, project_id: int):
        return self.client.import_storage.s3.list(project=project_id)

    def add_s3_storage_from_config(self, project_id: int, config: Union[str, dict]):
        config = s3_read_config(config).copy()
        config['project_id'] = project_id
        config.pop('config')  # used by botocore
        if 'endpoint_url' in config:
            config['s3endpoint'] = config.pop('endpoint_url')
        return self.client.import_storage.s3.create(**config)

    # --- ML Backend ---

    def add_ml_backend(self, project_id: int, url: str, title: str = None,
                       is_interactive: bool = False, **kwargs):
        return self.client.ml.create(
            project=project_id, url=url, title=title,
            is_interactive=is_interactive, **kwargs)

    def add_ml_backend_from_config(self, project_id: int, config: Union[str, dict]):
        if isinstance(config, str):
            with open(config, 'r') as f:
                config = json.load(f)
        return self.add_ml_backend(
            project_id=project_id,
            url=config['url'],
            title=config.get('title'),
            is_interactive=config.get('is_interactive', False),
        )

    # --- Copy & Migrate (CE-compatible) ---

    def _fetch_tasks_raw(self, host, headers, project_id):
        """Fetch tasks via raw API (no annotations/predictions)."""
        url = urljoin(host, '/api/tasks')
        params = {'project': project_id, 'page_size': 10000}
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get('tasks', data)

    def copy_project(self, source_project: Union[int, str], new_title: str,
                     include_tasks: bool = False, include_annotations: bool = False,
                     include_predictions: bool = False):
        """CE-compatible project copy (SDK duplicate is Enterprise-only).

        1. Get source project settings (label_config, description, etc.)
        2. Create new project with same settings
        3. If include_tasks: export tasks from source, import into new
        4. If include_annotations/predictions: include in export

        Uses SnapshotManager for export when annotations/predictions needed.
        """
        source = self.get_project(source_project)

        # Create new project with same settings
        new_project = self.client.projects.create(
            title=new_title,
            label_config=source.label_config,
            description=source.description,
        )

        if not include_tasks:
            return new_project

        if include_annotations or include_predictions:
            from .snapshot_download import SnapshotManager
            sm = SnapshotManager(self.host, self.token, source.id)
            sm.make_snapshot(title=f'copy_project export for "{new_title}"')
            sm.wait_for_snapshot_completion()
            tasks = sm.download_snap()
            sm.cleanup_snapshot()

            if not include_annotations:
                for task in tasks:
                    task.pop('annotations', None)
            if not include_predictions:
                for task in tasks:
                    task.pop('predictions', None)
        else:
            tasks = self._fetch_tasks_raw(self.host, self.headers, source.id)

        if tasks:
            self.client.projects.import_tasks(id=new_project.id, request=tasks)

        return new_project

    def migrate_project(self, source_host: str, source_token: str,
                        source_project: Union[int, str], new_title: str = None,
                        include_tasks: bool = True, include_annotations: bool = True):
        """Copy project from another LS instance to this instance.

        1. Create ProjectManager for source
        2. Get source project settings
        3. Export tasks from source (via SnapshotManager if annotations needed)
        4. Create project on self
        5. Import tasks
        """
        source_pm = ProjectManager(host=source_host, token=source_token)
        source = source_pm.get_project(source_project)

        if new_title is None:
            new_title = source.title

        # Create project on this instance
        new_project = self.client.projects.create(
            title=new_title,
            label_config=source.label_config,
            description=source.description,
        )

        if not include_tasks:
            return new_project

        # Export from source
        if include_annotations:
            from .snapshot_download import SnapshotManager
            sm = SnapshotManager(source_host, source_token, source.id)
            sm.make_snapshot(title=f'migrate_project export for "{new_title}"')
            sm.wait_for_snapshot_completion()
            tasks = sm.download_snap()
            sm.cleanup_snapshot()
        else:
            tasks = self._fetch_tasks_raw(source_host, source_pm.headers, source.id)

        if tasks:
            self.client.projects.import_tasks(id=new_project.id, request=tasks)

        return new_project


# Project fields the SDK's projects.create/update don't expose as named params.
# These are sent via raw PATCH /api/projects/{id}/.
_UNTYPED_PROJECT_FIELDS = frozenset({'start_training_on_annotation_update'})


# --- Split-config helpers ---

def _normalize_jsonish(v):
    """Parse v as JSON if it looks like one; otherwise return as-is.

    Used so that `extra_params` stored as a JSON string by LS can be compared
    structurally against a dict from the config.
    """
    if v is None or v == '':
        return None
    if isinstance(v, str):
        try:
            return json.loads(v)
        except (ValueError, TypeError):
            return v
    return v


def _diff_kwargs(existing, desired: dict, ignore=(), aliases=None,
                 normalize=None) -> dict:
    """Return {field: (current, new)} for fields in `desired` that differ on `existing`.

    - `ignore`: iterable of `desired` keys to skip entirely.
    - `aliases`: {desired_key: attr_on_existing} when the request-side and
      response-side names differ (e.g. 's3endpoint' vs 's3_endpoint').
    - `normalize`: {desired_key: callable} applied to both sides before compare
      (e.g. parse JSON-string extra_params before comparing).
    """
    aliases = aliases or {}
    normalize = normalize or {}
    ignore = set(ignore)
    diff = {}
    for k, new in desired.items():
        if k in ignore:
            continue
        attr = aliases.get(k, k)
        current = getattr(existing, attr, None)
        norm = normalize.get(k)
        if norm is not None:
            if norm(current) == norm(new):
                continue
        elif current == new:
            continue
        diff[k] = (current, new)
    return diff


def print_config_plan(plan: list) -> None:
    """Pretty-print a plan returned by ProjectManager.plan_config."""
    if not plan:
        print("Plan: (empty)")
        return
    print("Plan:")
    for item in plan:
        kind = item['kind']
        title = item['title']
        action = item['action']
        ident = f" id={item['id']}" if item.get('id') is not None else ""
        print(f"  [{kind:<10}] '{title}'{ident}: {action.upper()}")
        if action == 'create' and item.get('fields'):
            for k in sorted(item['fields']):
                v = item['fields'][k]
                print(f"      + {k} = {_short_repr(v)}")
        elif action == 'update' and item.get('changes'):
            for k in sorted(item['changes']):
                cur, new = item['changes'][k]
                print(f"      ~ {k}: {_short_repr(cur)} -> {_short_repr(new)}")


def _short_repr(v, maxlen=80):
    s = repr(v)
    if len(s) > maxlen:
        s = s[:maxlen - 3] + '...'
    return s


_SAMPLING_MAP = {
    'sequential': 'Sequential sampling',
    'uniform': 'Uniform sampling',
    'uncertainty': 'Uncertainty sampling',
}
