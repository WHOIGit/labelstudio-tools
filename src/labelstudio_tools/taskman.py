import json
import os
import os.path
from copy import deepcopy
from typing import Union, Literal
import datetime as dt

import requests
import botocore
from tqdm import tqdm
from urllib.parse import urljoin
from label_studio_sdk.client import LabelStudio
from label_studio_sdk.types import View
from label_studio_sdk.data_manager import Filters, Column, Type, Operator

from .utils import read_token, parse_task_filter, attr_list_decorator, \
    s3_client_and_bucket, s3_object_exists, s3_url_to_bucket_and_key, estimate_chunks, \
    env_var_substitution
from .utils import chunk_my_dict


class _TaskCache:
    """Internal cache for task lookups by primary key fields."""

    def __init__(self):
        self.tasks = None
        self.tasks_timestamp = None
        self.by_pk = None
        self.pk_fields = None

    @property
    def is_loaded(self) -> bool:
        return self.tasks is not None

    def clear(self):
        self.tasks = None
        self.tasks_timestamp = None
        self.by_pk = None
        self.pk_fields = None

    def load(self, tasks: list):
        self.tasks = tasks
        self.tasks_timestamp = dt.datetime.now()
        self.by_pk = None  # invalidate pk index

    def build_pk_index(self, pk_fields, normalize_key_fn, task_datafields_key_fn):
        """Build the by_pk dict. Raises on duplicates."""
        datafields_to_task = {}
        duplicates = {}
        for task in self.tasks:
            key = normalize_key_fn(task_datafields_key_fn(task, pk_fields))
            if key in datafields_to_task:
                if key in duplicates:
                    datafields_to_task[key].append(task)
                    duplicates[key] += 1
                else:
                    datafields_to_task[key] = [datafields_to_task[key], task]
                    duplicates[key] = 1
            else:
                datafields_to_task[key] = task
        if duplicates:
            raise ValueError(f'Cache has duplicates: {[datafields_to_task[key] for key in duplicates]}')
        self.by_pk = datafields_to_task
        self.pk_fields = pk_fields

    def lookup(self, key: str):
        if self.by_pk is None:
            return None
        return self.by_pk.get(key)


class TaskManager:
    def __init__(self, host:str, token:str, project:Union[int,str],
                 pk:Union[str,tuple[str]]=None, s3_config:Union[dict,str]=None):
        self.host = host
        self.token = read_token(token)
        self.client:LabelStudio = LabelStudio(base_url=self.host, api_key=self.token)
        self.project = self.get_project(project)
        self.task_pk_datafields = pk

        self.s3, self.s3_bucket = self.get_s3client(s3_config) if s3_config else (None, None)

        self._cache = _TaskCache()

    @classmethod
    def from_config(cls, config:Union[str,dict], use_dotenv_secrets=True):
        config_dir = None
        if isinstance(config, str):
            config_dir = os.path.dirname(os.path.abspath(config))
            with open(config, 'r') as f:
                config = json.load(f)

        if use_dotenv_secrets:
            config = env_var_substitution(config, use_dotenv=True)

        # Resolve relative s3_config path against config file location
        if config_dir and isinstance(config.get('s3_config'), str) and not os.path.isabs(config['s3_config']):
            config['s3_config'] = os.path.join(config_dir, config['s3_config'])

        return cls(**config)


    @property
    def headers(self):
        return { 'Content-Type': 'application/json',
                 'Authorization': f'Token {self.token}' }

    # Backward-compat cache properties
    @property
    def cached_tasks(self):
        return self._cache.tasks

    @cached_tasks.setter
    def cached_tasks(self, value):
        if value is None:
            self._cache.tasks = None
        else:
            self._cache.load(value)

    @property
    def cached_tasks_timestamp(self):
        return self._cache.tasks_timestamp

    @cached_tasks_timestamp.setter
    def cached_tasks_timestamp(self, value):
        self._cache.tasks_timestamp = value

    @property
    def cached_task_by_pk(self):
        return self._cache.by_pk

    # PROJECTS #

    @attr_list_decorator
    def list_projects(self):
        return self.client.projects.list()

    def get_project(self, project):
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
                raise ValueError(f"Multiple projects found with name containing '{project}': {({p.id:p.title for p in matches})}")
        elif project is None:
            return None
        else:
            raise ValueError("Project must be an integer ID, a string name, or None.")

    def set_project(self, project):
        self.project = self.get_project(project)

    def project_counts(self):
        url = '/api/projects/counts'
        url = urljoin(self.host, url)

        params = dict(ids=self.project.id)
        response = requests.get(url, params=params, headers=self.headers)
        if response.status_code == 200:
            return response.json()['results'][0]
        else:
            raise ValueError(f"Status Code {response.status_code}: {response.json()}")


    # LABEL CONFIGURATION #

    def config_asdict(self):
        return  self.project.parsed_label_config
    def config_objects(self):
        return {o.name:o for o in self.project.get_label_interface().objects}
    def config_controls(self):
        return {c.name:c for c in self.project.get_label_interface().controls}
    def config_control_labels(self):
        return {name:control_obj.labels for name,control_obj in self.config_controls().items()}
    def config_control_labels_detailed(self):
        return {c.name:c.labels_attrs for c in self.project.get_label_interface().controls}


    # DATA FIELDS #

    @attr_list_decorator
    def data_fields(self):
        #https://ichthyolith.whoi.edu/api/dm/columns?project=6
        url = '/api/dm/columns'
        url = urljoin(self.host, url)

        params = {'project': self.project.id}

        response = requests.get(url, params=params, headers=self.headers)
        if response.status_code != 200:
            try:
                detail = response.json().get('detail', response.text)
            except Exception:
                detail = response.text or '(empty response)'
            raise ValueError(f"Status Code {response.status_code}: {detail}")
        data = response.json()['columns']
        data = [item for item in data if 'parent' in item and item['parent'] == 'data']
        return data


    # VIEWS #

    @attr_list_decorator
    def list_views(self):
        return self.client.views.list(project=self.project.id)

    def get_view(self, view):
        if isinstance(view, int):
            return self.client.views.get(id=view)
        elif isinstance(view, str):
            views = self.list_views()
            matches = [v for v in views if view in v.data['title']]
            exact_matches = [v for v in matches if view == v.data['title']]
            if len(exact_matches) == 1:
                return exact_matches[0]
            elif len(matches) == 1:
                return matches[0]
            elif len(matches) == 0:
                raise ValueError(f"No view found with name containing '{view}'")
            else:
                raise ValueError(f"Multiple views found with name containing '{view}': {({v.id:v.data['title'] for v in matches})}")
        elif isinstance(view, View):
            return view
        elif view is None:
            return None
        else:
            raise ValueError("View must be an integer ID, a string name, or None.")


    # TASKS #

    def parse_task_filter(self, filter_dict):
        return parse_task_filter(filter_dict, self.data_fields(attrs='id'))

    def get_tasks(self,
                  ids: list[int] = None,
                  exclude_ids: list[int] = None,
                  limit_fields_to: list[str] = None,
                  with_annotations: bool = False,
                  filter_dict: Union[dict, str] = None,
                  view: Union[int, str] = None,
                  resolve_uri: bool = False, add_data_presigned=None,
                  page: int = None, page_size: int = 10_000):
        url = '/api/tasks'
        url = urljoin(self.host, url)

        payload = dict(project=self.project.id)

        # Response Format #
        if resolve_uri and add_data_presigned:
            raise ValueError('resolve_uri and add_data_presigned are mutually exclusive')
        elif not resolve_uri:
            payload['resolve_uri'] = resolve_uri  # for not presigned s3_urls
        if with_annotations:
            payload['fields'] = 'all'  # default is 'task_only'
        if limit_fields_to:
            payload['include'] = limit_fields_to  # eg [id,data,annotations]
            if 'annotations' in limit_fields_to or 'predictions' in limit_fields_to:
                payload['fields'] = 'all'

        # Pagination #
        if page is not None:
            payload['page'] = page
        if page_size and page_size!=100:  # 100 is default-max
            payload['page_size'] = page_size

        # Filtering #
        if view:
            view = self.get_view(view)
            payload['view'] = view.id

        query = dict()
        if filter_dict:
            filter_dict = self.parse_task_filter(filter_dict)
            query['filters'] = filter_dict
        if ids and exclude_ids:
            raise ValueError('ids and exclude_ids are mutually exclusive arguments')
        elif ids:
            query['selectedItems'] = dict(all=False, included=ids)
        elif exclude_ids:
            query['selectedItems'] = dict(all=True, excluded=exclude_ids)
        # TODO query['ordering']

        if query:
            query = json.dumps(query)
            payload['query'] = query

        response = requests.get(url, params=payload, headers=self.headers)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            try:
                detail = response.json().get("detail", response.text)
            except ValueError:
                detail = response.text
            raise ValueError(f"{e} | {detail}") from e

        tasks = response.json()['tasks']
        if add_data_presigned:
            self.add_task_data_presigned_urls(tasks)

        # but what if there's more tasks than default page_size?
        # if we do nothing, it will truncate; so let's get it all!
        # Note: if page_size is None, it self-defaults to 100
        # hence the following recursive call.
        if page is None and len(tasks)==page_size:
            # it's likely we're need to pull more tasks
            next_page = 1
            more_tasks = tasks
            total = None
            if ids is None and exclude_ids is None and view is None and filter_dict is None:
                # well I reckon we're trying to grab the whole project then!
                total = self.project_counts()['task_number']
            with tqdm(initial=page_size,
                      unit='task',
                      unit_scale=True,
                      desc="get_tasks auto-paginate",
                      total=total) as pbar:
                while len(more_tasks)==page_size:
                    next_page += 1
                    more_tasks = self.get_tasks(
                        ids=ids,
                        exclude_ids=exclude_ids,
                        limit_fields_to=limit_fields_to,
                        with_annotations=with_annotations,
                        filter_dict=filter_dict,
                        view=view,
                        resolve_uri=resolve_uri,
                        add_data_presigned=add_data_presigned,
                        page=next_page,
                        page_size=page_size)
                    tasks.extend(more_tasks)
                    pbar.update(len(more_tasks))

        return tasks

    def add_task_data_presigned_urls(self, tasks):
        task_ids = [task['id'] for task in tasks]
        presigned_tasks = self.get_tasks(ids=task_ids, limit_fields_to=['id', 'data'], resolve_uri=True)
        presigned_tasks = {t['id']: t['data'] for t in presigned_tasks}
        for task in tasks:
            presigned_data = presigned_tasks[task['id']]
            presigned_data = {k: v for k, v in presigned_data.items()
                             if isinstance(v, str) and v.startswith('s3://')}
            task['data_presigned'] = presigned_data

    # TASK CACHING & PRIMARY KEY #

    @staticmethod
    def _normalize_pk_key(key) -> str:
        """Deterministic key normalization for cache lookups."""
        if isinstance(key, tuple):
            return "|".join(str(k) for k in key)
        return str(key)

    def _normalize_pk_fields(self, data_fields):
        """Normalize data_fields to a consistent tuple form."""
        if isinstance(data_fields, str):
            return (data_fields,)
        if isinstance(data_fields, tuple):
            return data_fields
        if isinstance(data_fields, list):
            return tuple(data_fields)
        return data_fields

    def cache_tasks(self, fields=('id', 'data')):
        self._cache.load(self.get_tasks(limit_fields_to=fields))

    def task_datafields_key(self, task, data_fields: Union[str,tuple[str]] = None):
        if data_fields is None:
            assert self.task_pk_datafields
            data_fields = self.task_pk_datafields
        if 'data' in task:
            if isinstance(data_fields, str):
                key = task['data'][data_fields]
            else:
                key = tuple([task['data'][field] for field in data_fields])
        else:
            if isinstance(data_fields, str):
                key = task[data_fields]
            else:
                key = tuple([task[field] for field in data_fields])
        return self._normalize_pk_key(key)

    def tasks_by_pk(self, tasks, data_fields: Union[str, tuple[str]] = None):
        tasks_dict = {}
        for task in tasks:
            key = self.task_datafields_key(task, data_fields)
            if key not in tasks_dict:
                tasks_dict[key] = task
            else:
                raise KeyError(f'Duplicate Key: {key}')
        return tasks_dict

    def cache_task_by_pk(self, data_fields: Union[str, tuple[str]]=None):
        if not self._cache.is_loaded:
            self.cache_tasks()

        data_fields = data_fields or self.task_pk_datafields
        self._cache.build_pk_index(data_fields, self._normalize_pk_key, self.task_datafields_key)
        if data_fields:
            self.task_pk_datafields = data_fields

    def task_exists(self, task_data, data_fields: Union[str, tuple[str]], use_cache=True) -> Union[dict,None]:
        data_fields_norm = self._normalize_pk_fields(data_fields)
        matchme = self.task_datafields_key(task_data, data_fields)

        if use_cache:
            cached_pk_fields = self._normalize_pk_fields(self.task_pk_datafields) if self.task_pk_datafields else None
            if data_fields_norm != cached_pk_fields or self._cache.by_pk is None:
                self.cache_task_by_pk(data_fields)
            result = self._cache.lookup(matchme)
            if result is not None:
                return result

        else:
            if isinstance(data_fields, str):
                data_fields = (data_fields,)
            filter_items = []
            for field in data_fields:
                item = dict(filter=f"filter:tasks:data.{field}",
                            operator=Operator.EQUAL,
                            value=task_data[field],
                            type=Type.Unknown)
                filter_items.append(item)
            filter_dict = dict(conjunction=Filters.AND,
                               items=filter_items)
            tasks = self.get_tasks(filter_dict=filter_dict, limit_fields_to=['id'])
            if len(tasks) == 1:
                return tasks[0]
            elif len(tasks) > 1:
                raise ValueError(f'Multiple tasks found for {matchme}')

        return None

    def create_task(self, task: dict, pk_datafields: Union[str, tuple[str]], dry_run=False, use_cache=False):
        assert pk_datafields
        ls_id, task_exists, task_created = None, False, False

        fetched_task = self.task_exists(task, pk_datafields, use_cache=use_cache)
        if fetched_task:
            ls_id = fetched_task['id']
            task_exists = True
            return ls_id, task_exists, task_created

        if not dry_run:
            new_task = self.client.tasks.create(project=self.project.id, data=task)
            ls_id = new_task.id
        else:
            ls_id = None
        task_created = True

        return ls_id, task_exists, task_created

    def create_tasks(self, tasks: list[dict], pk_datafields: Union[str, tuple[str]], dry_run=False, force_recache=True, max_mb=100):
        assert pk_datafields
        tasks = self.tasks_by_pk(tasks, pk_datafields)
        new_tasks = {}
        report = {}
        import_tasks_responses = []

        # Skip Exists
        if force_recache:
            self.cache_tasks()
        for key, task in tasks.items():
            cached_task = self.task_exists(task, pk_datafields)
            if cached_task:
                report[key] = dict(task_id=cached_task['id'], task_exists=True, task_created=False, task=task)
            else:
                report[key] = dict(task_id=None, task_exists=False, task_created=True, task=task)
                new_tasks[key] = task

        n_existing = len(tasks) - len(new_tasks)
        pbar = tqdm(total=len(tasks), initial=n_existing, desc='uploading tasks', unit='task')

        if not dry_run and new_tasks:
            chunk_size, chunk_count = estimate_chunks(new_tasks.values(), MAX_MB=max_mb)
            chunked_new_tasks = chunk_my_dict(new_tasks, chunk_size)

            for chunk in chunked_new_tasks:
                key0 = list(chunk.keys())[0]
                # print('tasks[key0].keys():',chunk[key0].keys())
                # print("task[key0]['data'].keys()", chunk[key0]['data'].keys())
                # print("tasks[key0]['predictions'][0].keys():", chunk[key0]['predictions'][0].keys())

                import_tasks_response = self.client.projects.import_tasks(
                    id=self.project.id,
                    request=list(chunk.values()),
                    return_task_ids=True,
                )
                import_tasks_responses.append(import_tasks_response)

                upload_ids = import_tasks_response.task_ids
                for upload_id, newtask_key in zip(upload_ids, chunk.keys()):
                    report[newtask_key]['task_id'] = upload_id

                pbar.update(len(chunk))
        else:
            for k, v in report.items():
                v['task_created'] = False

        pbar.close()

        return report, import_tasks_responses

    def update_task(self, task, patch_data=None):
        url = '/api/tasks/{id}'
        url = urljoin(self.host, url)
        url = url.format(id=task['id'])

        if patch_data:
            task_data = deepcopy(task['data'])
            task_data.update(patch_data)
            payload = dict(data=task_data)
        else:
            payload = dict(data=task['data'])

        response = requests.patch(url, json=payload, headers=self.headers)

        # Check if the request was successful
        if response.status_code != 200:
            print(f"Failed to retrieve data: {response.status_code} {response.content}")

        return response.json()


    def add_annotation(self, task_id: int, annotation):
        url = '/api/tasks/{id}/annotations'
        url = urljoin(self.host, url)
        url = url.format(id=task_id)
        payload = annotation

        response = requests.post(url, json=payload, headers=self.headers)

        # Check if the request was successful
        if response.status_code != 201:
            print(f"Failed to retrieve data: {response.status_code} {response.content}")

        return response.json()

    def add_prediction(self, task_id: int, prediction: dict):
        url = '/api/predictions'
        url = urljoin(self.host, url)

        payload = dict(
            task=task_id,
            **prediction,
        )

        response = requests.post(url, json=payload, headers=self.headers)

        # Check if the request was successful
        if response.status_code != 201:
            print(f"Failed to retrieve data: {response.status_code} {response.json()}")

        return response.json()


    # DUPLICATE TASKS #

    def find_duplicate_tasks(self, data_fields: Union[str, tuple[str]] = None,
                             use_cache: bool = True) -> dict[str, list[dict]]:
        """Find tasks with duplicate primary key values.
        Returns {key: [task1, task2, ...]} for all keys with >1 task.
        """
        data_fields = data_fields or self.task_pk_datafields
        assert data_fields, "data_fields or task_pk_datafields must be set"

        if use_cache:
            if not self._cache.is_loaded:
                self.cache_tasks()
            tasks = self._cache.tasks
        else:
            tasks = self.get_tasks(limit_fields_to=['id', 'data'])

        groups = {}
        for task in tasks:
            key = self.task_datafields_key(task, data_fields)
            groups.setdefault(key, []).append(task)

        return {k: v for k, v in groups.items() if len(v) > 1}

    def remove_duplicate_tasks(self, data_fields: Union[str, tuple[str]] = None,
                               keep: Literal['first', 'last', 'most_annotations'] = 'first',
                               dry_run: bool = True) -> dict:
        """Find and optionally remove duplicate tasks.
        keep: which duplicate to keep ('first', 'last', or 'most_annotations').
        Returns report of what was/would be deleted.
        """
        duplicates = self.find_duplicate_tasks(data_fields, use_cache=not dry_run)
        report = {}
        for key, tasks in duplicates.items():
            if keep == 'first':
                keeper, to_delete = tasks[0], tasks[1:]
            elif keep == 'last':
                keeper, to_delete = tasks[-1], tasks[:-1]
            elif keep == 'most_annotations':
                tasks_sorted = sorted(tasks, key=lambda t: len(t.get('annotations', [])), reverse=True)
                keeper, to_delete = tasks_sorted[0], tasks_sorted[1:]
            else:
                raise ValueError(f"Invalid keep strategy: {keep}")

            report[key] = {'keep': keeper['id'], 'delete': [t['id'] for t in to_delete]}

            if not dry_run:
                for task in to_delete:
                    self.client.tasks.delete(id=task['id'])

        return report


    # CACHE LABELS #

    def update_cachelabel(self,
                          control_tag: str,
                          with_counters: bool = False,
                          from_predictions: bool = False,
                          ids: list[int] = None,
                          exclude_ids: list[int] = None,
                          view: Union[int, str, View] = None, ):
        url = '/api/dm/actions'
        url = urljoin(self.host, url)
        params = dict(id='cache_labels', project=self.project.id)
        payload = dict(
            project=self.project.id,
            source='Predictions' if from_predictions else 'Annotations',
            control_tag=control_tag,
            with_counters='Yes' if with_counters else 'No',
        )

        # Filtering #
        if view:
            view = self.get_view(view)
            params['tabID'] = view.id

        if ids and exclude_ids:
            raise ValueError('ids and exclude_ids are mutually exclusive arguments')
        elif ids:
            payload['selectedItems'] = dict(all=False, included=ids)
        elif exclude_ids:
            payload['selectedItems'] = dict(all=True, excluded=exclude_ids)

        response = requests.post(url, params=params, json=payload, headers=self.headers)

        # Check if the request was successful
        if response.status_code != 200:
            print(f"Failed to retrieve data: {response.status_code} {response.json()}")


    def update_cachelabels(self,
                           control_tags: list[str],
                           with_counters: bool = False,
                           from_predictions = False,
                           timeout_groups = 'auto',
                           timeout_seconds:int = 30,
                           items_per_second:int = 500):

        # make sure control_tags are valid
        self.validate_labels(control_tags)

        # determine if sub-views are needed to avoid timeout
        if timeout_groups == 'auto':
            timeout_groups = self.timeout_groups_required(timeout_seconds, items_per_second)
            if timeout_groups <= 1:
                timeout_groups = []
            else:
                ids = [task['id'] for task in self.get_tasks(limit_fields_to=['id'])]
                chunk_size = min(10000, len(ids)//timeout_groups)
                timeout_groups = [ids[x:x+chunk_size] for x in range(0, len(ids), chunk_size)]

        pbar = tqdm(control_tags)
        for tag in pbar:
            if timeout_groups:
                for idx,id_group in enumerate(timeout_groups):
                    pbar.set_description(f'Updating "{tag}" (group_idx {idx+1} of {len(timeout_groups)})')
                    self.update_cachelabel(tag, with_counters, from_predictions, ids=id_group)
            else:
                pbar.set_description(f'Updating "{tag}"')
                self.update_cachelabel(tag, with_counters, from_predictions)


    def timeout_groups_required(self, timeout_seconds=30, items_per_second=500):
        timeout_views = self.project.total_annotations_number // (items_per_second * timeout_seconds)
        return timeout_views+1

    def validate_labels(self, control_tags):
        project_control_labels = self.config_control_labels()
        for tag in control_tags:
            if tag not in project_control_labels:
                raise ValueError(f'Control tag "{tag}" not in project control tags: {project_control_labels}')



    # S3 #

    def set_project_s3(self):
        # todo create s3 cloud SOURCE for remote Ls project
        raise NotImplementedError

    @staticmethod
    def get_s3client(s3_config: Union[dict,str]):
        if isinstance(s3_config, str):
            with open(s3_config, 'r') as f:
                s3_config = json.load(f)
        s3client, bucket = s3_client_and_bucket(s3_config)
        return s3client, bucket

    def s3key_to_url(self, s3key, bucket=None):
        if bucket is None:
            bucket = self.s3_bucket.name
        return f's3://{bucket}/{s3key}'

    def s3key_exists(self, s3key, bucket=None):
        if bucket is None:
            bucket = self.s3_bucket.name
        obj = self.s3.Object(bucket, s3key)
        try: obj.load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise e
        return True

    def s3url_exists(self, s3url):
        bucket, key = s3_url_to_bucket_and_key(s3url)
        return self.s3key_exists(key,bucket)

    def validate_task_s3_objects(self, task):
        s3_objects = {k:v for k,v in task['data'].items() if isinstance(v,str) and v.startswith('s3://')}
        s3_exists = {}
        for k,url in s3_objects.items():
            bucket,s3key = s3_url_to_bucket_and_key(url)
            s3_exists[k] = self.s3key_exists(s3key, bucket)
        return s3_exists

    def validate_all_task_s3_objects(self, tasks=None):
        assert self.task_pk_datafields
        if tasks is None and not self.cached_tasks:
            self.cache_tasks()
        if tasks is None:
            tasks = self.tasks_by_pk(self.cached_tasks)
        elif tasks:
            tasks = self.tasks_by_pk(tasks)

        report = {}
        bad_tasks_pks = []
        for key,task in tqdm(tasks.items()):
            report[key] = self.validate_task_s3_objects(task)
            if any([v==False for v in report[key].values()]):
                bad_tasks_pks.append(key)
        return report, bad_tasks_pks

    def download_s3key(self, s3key, outfile, bucket=None, clobber=True):
        if os.path.isfile(outfile) and not clobber:
            return False
        bucket = self.s3.Bucket(bucket) if bucket else self.s3_bucket
        os.makedirs(os.path.dirname(outfile), exist_ok=True)
        bucket.download_file(s3key, outfile)
        return True

    def download_s3url(self, s3url, outfile, clobber=True):
        bucket,s3key = s3_url_to_bucket_and_key(s3url)
        return self.download_s3key(s3key, outfile, bucket, clobber)

    def upload_s3key(self, filepath, s3key, bucket=None, clobber=True):
        if not os.path.isfile(filepath):
            raise ValueError(f'Input file "{filepath}" does not exist')
        if not clobber and self.s3key_exists(s3key, bucket):
            return False
        bucket = self.s3.Bucket(bucket) if bucket else self.s3_bucket
        bucket.upload_file(filepath, s3key)
        return True

    def upload_s3url(self, filepath, s3url, clobber=True):
        bucket,s3key = s3_url_to_bucket_and_key(s3url)
        return self.upload_s3key(filepath, s3key, bucket, clobber)

    def s3key_to_s3url(self, s3key:str, bucket=None):
        assert s3key[0] != '/'
        if bucket is None:
            bucket = self.s3_bucket.name
        return f's3://{bucket}/{s3key}'
