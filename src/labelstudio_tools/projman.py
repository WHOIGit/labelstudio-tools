import copy
import json
import os
import warnings
from typing import Union

import requests
from urllib.parse import urljoin
from label_studio_sdk.client import LabelStudio

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
        config_path = config if isinstance(config, str) else None
        config = _load_json(config)
        config = _merge_auth(config, config_path, auth_config)
        # Only token is required to instantiate; storage/ml validation is
        # deferred to plan/apply so a manager can be built without storages.
        if not config.get('token'):
            err = config.get('__auth_file_error__')
            msg = (f"No `token` found inline or via auth file for host "
                   f"{config.get('host')!r}")
            if err is not None:
                raise FileNotFoundError(f"{err}. {msg}")
            raise ValueError(msg)
        return cls(host=config['host'], token=config['token'])

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
        config_path = config if isinstance(config, str) else None
        if base_dir is None and config_path is not None:
            cfg_dir = os.path.dirname(os.path.abspath(config_path))
            base_dir = os.path.dirname(cfg_dir)
        config = _load_json(config)
        config = _merge_auth(config, config_path, auth_config)
        _check_required_auth(config)
        config.pop('__auth_file_error__', None)

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
                    project_obj = self.client.projects.create(**item['fields'])
                    proj_id = project_obj.id
                    print(f"  -> created project '{project_obj.title}' (id={project_obj.id})")
                else:  # update
                    proj_id = item['id']
                    fields = {k: new for k, (_, new) in item['changes'].items()}
                    self.client.projects.update(id=proj_id, **fields)
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


def _load_json(path_or_dict: Union[str, dict]) -> dict:
    if isinstance(path_or_dict, dict):
        return path_or_dict
    with open(path_or_dict) as f:
        return json.load(f)


def _resolve_auth_path(auth_path: str, config_path: str = None) -> str:
    """Resolve a relative auth-file path against the config file's dir, then cwd.

    Absolute paths are used as-is. Returns the absolute path; raises FileNotFoundError
    if no candidate exists. Emits a warning when falling back to cwd.
    """
    if os.path.isabs(auth_path):
        if not os.path.isfile(auth_path):
            raise FileNotFoundError(f"auth file not found: {auth_path}")
        return auth_path

    cwd_candidate = os.path.abspath(auth_path)
    if config_path:
        config_dir = os.path.dirname(os.path.abspath(config_path))
        config_candidate = os.path.join(config_dir, auth_path)
        if os.path.isfile(config_candidate):
            return config_candidate
        if os.path.isfile(cwd_candidate):
            warnings.warn(
                f"auth file {auth_path!r} not found relative to config dir "
                f"({config_dir!r}); using cwd: {cwd_candidate!r}",
                RuntimeWarning, stacklevel=2)
            return cwd_candidate
        raise FileNotFoundError(
            f"auth file {auth_path!r} not found relative to config dir "
            f"({config_dir!r}) or cwd ({os.getcwd()!r})")
    if not os.path.isfile(cwd_candidate):
        raise FileNotFoundError(
            f"auth file {auth_path!r} not found in cwd ({os.getcwd()!r})")
    return cwd_candidate


def _merge_auth(config: dict, config_path: str = None,
                auth_override: Union[str, dict] = None) -> dict:
    """Merge auth values into `config`, returning a new dict.

    Resolution order (each layer can override the previous):
      1. Auth file referenced by `config['auth']` — base layer.
      2. Inline values in `config` — override file values; a UserWarning is
         emitted whenever an inline value differs from the file value.
      3. `auth_override` parameter (path or dict) — overrides everything,
         silently.

    Auth-file load failure is deferred: if the file doesn't exist, the load
    error is captured and only re-raised by `_check_required_auth` when an
    inline value still isn't supplying what we need. ML-backend auth values
    (`user`, `pass`) are always optional.
    """
    # Layer 1: auth file (referenced by `config['auth']`)
    auth_file_data = None
    auth_file_error = None
    if config.get('auth'):
        try:
            resolved = _resolve_auth_path(config['auth'], config_path)
            with open(resolved) as f:
                auth_file_data = json.load(f)
        except FileNotFoundError as e:
            auth_file_error = e

    # Layer 3: function param (loaded eagerly, errors not deferred)
    override_data = None
    if auth_override is not None:
        if isinstance(auth_override, dict):
            override_data = auth_override
        else:
            with open(auth_override) as f:
                override_data = json.load(f)

    merged = copy.deepcopy(config)
    merged.pop('auth', None)  # consumed; not part of plan/apply

    # Apply layer 1 underneath the inline values: only fill missing keys, but
    # warn when an inline value differs from a file value (= layer 2 overrides
    # layer 1).
    if auth_file_data is not None:
        _apply_auth_layer(merged, auth_file_data,
                          mode='fill_missing', warn_on_collision=True)
    # Apply layer 3 on top: silently overrides anything below.
    if override_data is not None:
        _apply_auth_layer(merged, override_data,
                          mode='override', warn_on_collision=False)

    # Stash for the caller's validation step. Removed before plan/apply use it.
    merged['__auth_file_error__'] = auth_file_error
    return merged


def _check_required_auth(merged: dict) -> None:
    """Raise if required auth values are missing. ML-backend auth is exempt.

    Required:
      - `token` (any project that talks to LS at all).
      - `aws_access_key_id` + `aws_secret_access_key` per storage entry.

    If a referenced auth file failed to load and a required value is missing,
    raise FileNotFoundError with that context. Otherwise raise ValueError.
    """
    auth_file_error = merged.get('__auth_file_error__')
    missing = []
    if not merged.get('token'):
        missing.append(f"labelstudio token (host={merged.get('host')!r})")
    for sc in merged.get('storage', []):
        sid = (f"storage bucket={sc.get('bucket')!r} "
               f"endpoint_url={sc.get('endpoint_url')!r}")
        if not sc.get('aws_access_key_id'):
            missing.append(f"{sid}: aws_access_key_id")
        if not sc.get('aws_secret_access_key'):
            missing.append(f"{sid}: aws_secret_access_key")
    if not missing:
        return
    msg = "Missing required auth values: " + "; ".join(missing)
    if auth_file_error is not None:
        raise FileNotFoundError(f"{auth_file_error}. {msg}")
    raise ValueError(msg)


def _apply_auth_layer(merged: dict, auth_data: dict,
                      mode: str, warn_on_collision: bool) -> None:
    """Apply a single auth layer onto `merged` (mutates in place).

    mode='fill_missing': source values fill only where target is missing the key.
    mode='override': source overrides target keys.
    warn_on_collision: emit a UserWarning when target has a different value
        than source for the same key.
    """
    # LS host/token
    host = merged.get('host')
    if host is not None:
        match = _find_one(auth_data.get('labelstudio', []),
                          lambda e: e.get('host') == host,
                          name=f"labelstudio host={host!r}")
        if match is not None:
            _apply_layer_dict(merged, match, skip=('host',),
                              mode=mode, warn_on_collision=warn_on_collision,
                              context=f"labelstudio[{host!r}]")

    # Storages
    for storage_cfg in merged.get('storage', []):
        bucket = storage_cfg.get('bucket')
        endpoint_url = storage_cfg.get('endpoint_url')
        match = _find_one(
            auth_data.get('s3', []),
            lambda e: e.get('bucket') == bucket and e.get('endpoint_url') == endpoint_url,
            name=f"s3 bucket={bucket!r} endpoint_url={endpoint_url!r}")
        if match is not None:
            _apply_layer_dict(storage_cfg, match, skip=('bucket', 'endpoint_url'),
                              mode=mode, warn_on_collision=warn_on_collision,
                              context=f"storage[{bucket!r}@{endpoint_url!r}]")

    # ML backend
    ml_cfg = merged.get('ml_backend')
    if ml_cfg:
        name = ml_cfg.get('name')
        backend_url = ml_cfg.get('backend_url')
        match = None
        if name is not None:
            match = _find_one(auth_data.get('ml_backend', []),
                              lambda e: e.get('name') == name,
                              name=f"ml_backend name={name!r}")
        if match is None and backend_url is not None:
            match = _find_one(
                auth_data.get('ml_backend', []),
                lambda e: e.get('backend_url') == backend_url,
                name=f"ml_backend backend_url={backend_url!r}")
        if match is not None:
            _apply_layer_dict(ml_cfg, match, skip=('name', 'backend_url'),
                              mode=mode, warn_on_collision=warn_on_collision,
                              context=f"ml_backend[{name or backend_url!r}]")


def _apply_layer_dict(target: dict, source: dict, skip=(),
                      mode: str = 'fill_missing',
                      warn_on_collision: bool = True,
                      context: str = "") -> None:
    skip = set(skip)
    for k, v in source.items():
        if k in skip:
            continue
        if k in target:
            if target[k] != v:
                if warn_on_collision:
                    warnings.warn(
                        f"{context}: inline {k!r} overrides auth-file value "
                        f"({target[k]!r} vs {v!r})",
                        UserWarning, stacklevel=4)
                if mode == 'override':
                    target[k] = v
        else:
            target[k] = v


def _find_one(entries, pred, name: str = ""):
    matches = [e for e in entries if pred(e)]
    if len(matches) > 1:
        raise ValueError(f"Multiple auth entries match {name}")
    return matches[0] if matches else None
