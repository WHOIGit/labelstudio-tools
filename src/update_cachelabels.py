import os
import json
from typing import Union

from tqdm import tqdm
import requests
from urllib.parse import urljoin
from label_studio_sdk import Project, View

from core import LSProject


class LabelCacher(LSProject):

    def __init__(self, host: str, token: str, project: Union[int, str, Project],
                 timeout_seconds: int = 30, items_per_second: int = 500):
        super().__init__(host, token, project)

        # values used to check if sub-views necessary
        self.timeout_seconds = timeout_seconds
        self.items_per_second = items_per_second


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
                           from_predictions=False,
                           timeout_views='auto'):

        # make sure control_tags are valid
        self.validate_labels(control_tags)

        # determine if sub-views are needed to avoid timeout
        if timeout_views == 'auto':
            timeout_views = self.timeout_views_required(self.timeout_seconds, self.items_per_second)
            if timeout_views <= 1:
                timeout_views = []
            else:
                # create N views over which tasks/annotations are distributed
                # will also need to clean these up afterwards, optionally
                raise NotImplementedError

        pbar = tqdm(control_tags)
        for tag in pbar:
            if timeout_views:
                for view in timeout_views:
                    pbar.set_description(f'Updating "{tag}" (view_id={view.id})')
                    self.update_cachelabel(tag, with_counters, from_predictions, view=view)
            else:
                pbar.set_description(f'Updating "{tag}"')
                self.update_cachelabel(tag, with_counters, from_predictions)


    def timeout_views_required(self, timeout_seconds=30, items_per_second=500):
        timeout_views = self.project.total_annotations_number // (items_per_second * timeout_seconds)
        return timeout_views+1

    def validate_labels(self, control_tags):
        project_control_labels = self.config_control_labels()
        for tag in control_tags:
            if tag not in project_control_labels:
                raise ValueError(f'Control tag "{tag}" not in project control tags: {project_control_labels}')


def read_fields(field_args):
    # for use with argparse, where
    # parser.add_argument('--fields', '-f', metavar='F', nargs='*', required=True, help='Annotation fields to cache. A line-delimited text file also accepted')
    fields = []
    for field in field_args:
        if os.path.isfile(field):
                with open(field) as f:
                    if field.endswith('.json'):
                        json_dict = json.load(f)
                        fields.extend( json_dict.keys() )
                    else:
                        fields.extend( f.read().strip().splitlines() )
        else:
            fields.append(field)
    return fields

