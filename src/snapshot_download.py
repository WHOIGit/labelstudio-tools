import datetime as dt
import io
import json
import time
from typing import Union

from label_studio_sdk.base_client import httpx
from label_studio_sdk.client import LabelStudio
from label_studio_sdk.types import Export


from core import LabelStudioPlusBaseClass
from utils import read_token, attr_list_decorator


class SnapshotManager:
    def __init__(self, host, token, project, snapshot=None):
        self.host = host
        self.token = read_token(token)
        self.client = LabelStudio(base_url=self.host, api_key=self.token)
        self.project = self.get_project(project)
        self.filterview = None
        self.snap = self.get_snapshot(snapshot)

    get_project = LabelStudioPlusBaseClass.get_project


    @attr_list_decorator
    def list_snapshots(self):
        return self.client.projects.exports.list(id=self.project.id)


    def get_snapshot(self, snapshot):
        if isinstance(snapshot, int):
            return self.client.projects.exports.get(export_pk=snapshot, id=self.project.id)
        elif isinstance(snapshot, Export):
            return self.client.projects.exports.get(export_pk=snapshot.id, id=self.project.id)
        elif isinstance(snapshot, str):
            snaps = self.list_snapshots()
            matches = [s for s in snaps if snapshot in s.title]
            exact_matches = [s for s in matches if snapshot == s.title]
            if len(exact_matches) == 1:
                return exact_matches[0]
            elif len(matches) == 1:
                return matches[0]
            elif len(matches) == 0:
                raise ValueError(f"No snapshot found with name containing '{snapshot}'")
            elif len(matches) > 1:
                raise ValueError(f"Multiple snapshots found with name containing '{snapshot}': {{s.id:s.title for s in matches}}")
        elif snapshot is None:
            return None
        else:
            raise ValueError("Snapshot must be an integer ID, a snapshot (to update), a string name, or None.")


    def set_snapshot(self, snapshot):
        self.snap = self.get_snapshot(snapshot)


    def make_full_snapshot(self, title):
        print('Creating a project snapshot...')
        return self.client.projects.exports.create(id=self.project.id, title=title)


    def make_filtered_snapshot(self, title, filter_dict:dict):
        print('Creating a FILTERED snapshot....')
        # filter_dict = {"conjunction": "and",
        #                "items": [ {"filter": f"filter:tasks:data.{field}",
        #                            "operator": operator,
        #                            "value": value,
        #                            "type": fieldtype} ]}
        self.filterview = self.client.views.create(
            project=self.project.id,
            data=dict(
                filters=filter_dict,
                title=f'Snapshot FilterView for "{title}"')
        )

        return self.client.projects.exports.create(
                id=self.project.id, title=title,
                task_filter_options=dict(view=self.filterview.id)
                )


    def make_snapshot(self, title=None, filter_obj=None):
        if title is None:
            title = f'{self.project.title} at {dt.datetime.now().isoformat(timespec="seconds")}'

        if filter_obj:
            func,args = self.make_filtered_snapshot, (title, filter_obj)
        else:
            func,args = self.make_full_snapshot, (title,)

        try:
            snap = func(*args)
        except httpx.ReadTimeout as e:
            snaps = self.client.projects.exports.list(id=self.project.id)
            snap = [snap for snap in snaps if snap.title == title][0]

        self.snap = snap


    def wait_for_snapshot_completion(self):
        # Wait for snapshot to complete
        while self.snap.status in ["created", "in_progress"]:
            print(f'Snapshot status: {self.snap.id} "{self.snap.title}" - {self.snap.status}')
            self.snap = self.get_snapshot(self.snap.id)
            time.sleep(10)
        assert self.snap.status == 'completed', f'Snapshot status is "{self.snap.status}"'
        print(f'Snapshot status: {self.snap.id} "{self.snap.title}" - {self.snap.status}')


    def download_snap(self, location: Union[io.BufferedIOBase, io.TextIOBase, str, None] = None, export_type='JSON'):
        print(f'Downloading Snapshot as {export_type}')
        chunks = self.client.projects.exports.download(export_pk=self.snap.id, id=self.project.id, export_type=export_type)

        if isinstance(location, (io.BufferedIOBase, io.TextIOBase)):
            flo = location
        elif isinstance(location, str):
            flo = open(location, 'wb')
        else:  # None, hold it in memory
            flo = io.StringIO()

        for chunk in chunks:
            if isinstance(flo, io.TextIOBase):
                flo.write(chunk.decode())
            else:
                flo.write(chunk)

        if isinstance(location, str):
            flo.close()
        elif location is None:
            flo.seek(0)
            if export_type == 'JSON':
                content = json.load(flo)
            else:
                content = flo.read()
            flo.close()
            return content
        #else: ... # handled by external context manager


    def cleanup_snapshot(self, snap=None, cleanup_filterview=True):
        if snap is None:
            snap = self.snap
        else:
            snap = self.get_snapshot(snap)

        try:
            self.client.projects.exports.delete(export_pk=snap.id, id=self.project.id)
            print(f"DELETED: Snapshot {snap.id} deleted successfully")
        except Exception as e:
            print(type(e), f'Error deleting snapshot {snap.id} "{snap.title}"')

        if cleanup_filterview and self.filterview:
            print(self.filterview)
            view_title = self.filterview.data['title']
            views = self.client.views.list(project=self.project.id)
            try:
                view = [v for v in views if v.data['title'] == view_title][0]
                self.client.views.delete(view.id)
                print(f'DELETED: View {view.id} "{view_title}" deleted successfully')
            except Exception as e:
                print(type(e), f'View "{view_title}" failed to be removed')




