import json
import os
from typing import Union

import requests
from urllib.parse import urljoin
from label_studio_sdk.client import LabelStudio

from .utils import read_token, attr_list_decorator, s3_read_config, env_var_substitution


class ProjectManager:
    """Project-level Label Studio management (CE-compatible)."""

    def __init__(self, host: str, token: str):
        self.host = host
        self.token = read_token(token)
        self.client = LabelStudio(base_url=self.host, api_key=self.token)

    @classmethod
    def from_config(cls, config: Union[str, dict], use_dotenv_secrets=True):
        if isinstance(config, str):
            with open(config, 'r') as f:
                config = json.load(f)

        if use_dotenv_secrets:
            config = env_var_substitution(config, use_dotenv=True)

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

    def create_project_from_config(self, config: Union[str, dict]):
        """Create project + storage + ML backend from a unified config file.

        Config format:
        {
            "project": {"title": "...", "label_config": "path/to/ui.xml", "description": "..."},
            "storage": {"type": "s3", "config_file": "path/to/s3_config.json", "prefix": "..."},
            "ml_backend": {"url": "http://...", "title": "...", "is_interactive": false}
        }

        Paths in config are relative to config file location if config is a filepath.
        """
        config_dir = None
        if isinstance(config, str):
            config_dir = os.path.dirname(os.path.abspath(config))
            with open(config, 'r') as f:
                config = json.load(f)

        def _resolve_path(path):
            if config_dir and not os.path.isabs(path):
                return os.path.join(config_dir, path)
            return path

        # Create project (idempotent: skip if title already exists)
        proj_cfg = config['project']
        title = proj_cfg['title']
        existing_projects = self.list_projects()
        project = next((p for p in existing_projects if p.title == title), None)
        if project is not None:
            print(f"Project '{title}' already exists (id={project.id}), skipping creation.")
        else:
            label_config_file = None
            if 'label_config' in proj_cfg:
                label_config_file = _resolve_path(proj_cfg['label_config'])
            project = self.create_project(
                title=title,
                label_config_file=label_config_file,
                description=proj_cfg.get('description'),
            )
            print(f"Created project '{title}' (id={project.id}).")

        # Add storage (idempotent: skip if storage title already exists on this project)
        if 'storage' in config:
            storage_cfg = config['storage']
            if storage_cfg.get('type') == 's3':
                s3_config_file = _resolve_path(storage_cfg['config_file'])
                s3_config = s3_read_config(s3_config_file)
                # Merge any extra keys from storage section into s3 config (overrides file values)
                skip_keys = {'type', 'config_file'}
                for k, v in storage_cfg.items():
                    if k not in skip_keys:
                        s3_config[k] = v
                storage_title = s3_config.get('title', s3_config.get('bucket', 's3'))
                existing_storages = self.list_import_storages(project.id)
                existing_storage = next((s for s in existing_storages if s.title == storage_title), None)
                if existing_storage is not None:
                    print(f"Storage '{storage_title}' already exists on project '{title}', skipping.")
                else:
                    self.add_s3_storage_from_config(project_id=project.id, config=s3_config)
                    print(f"Added S3 storage '{storage_title}' to project '{title}'.")

        # Add ML backend (idempotent: skip if backend with same title already exists)
        if 'ml_backend' in config:
            ml_cfg = config['ml_backend']
            if ml_cfg.get('url'):
                ml_title = ml_cfg.get('title', ml_cfg['url'])
                existing_backends = self.client.ml.list(project_id=project.id)
                existing_backend = next((b for b in existing_backends if b.title == ml_title), None)
                if existing_backend is not None:
                    print(f"ML backend '{ml_title}' already exists on project '{title}', skipping.")
                else:
                    self.add_ml_backend(
                        project_id=project.id,
                        url=ml_cfg['url'],
                        title=ml_title,
                        is_interactive=ml_cfg.get('is_interactive', False),
                    )
                    print(f"Added ML backend '{ml_title}' to project '{title}'.")

        return project

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
