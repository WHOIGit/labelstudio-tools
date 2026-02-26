# Labelstudio Tools
Advanced Labelstudio API toolkit

`labelstudio-tools` extends the official label-studio-sdk with:
- Idempotent bulk task actions
- Client-side task caching
- Snapshot (export) automation
- Bulk downloads
- S3 validation & transfer helpers
- Cache-Label management
- View & filter helpers

It is designed for power users managing large Label Studio projects programmatically.

## Installation
```python
pip install git+https://github.com/your-org/labelstudio-tools.git
```
## Quick Start
```python
from labelstudio_tools import LabelStudioPlus

ls = LabelStudioPlus(
    host="https://your-labelstudio-instance.com",
    token="LABELSTUDIO_API_TOKEN",
    project="Some Project Name"  # or project_id integer
)

# or

ls = LabelStudioPlus.from_config('path/to/config.json')
# where config.json looks like
# {
#   "host": "https://my-labelstudio-instance.com",
#   "token": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
#   "project": 1,
#   "pk": "image",
#   "s3_config": {
#     "bucket": "myBucket",
#     "endpoint_url": "http://mys3.endpoint.com",
#     "aws_access_key_id": "XXXXXXXXXXXXXXXXXXXX",
#     "aws_secret_access_key": "xXxxXXXxxxxXxxXxxxXXxXXXXXxXXXXXxXxxXXxx"
#   }
# }

# fetch all tasks from the project
tasks = ls.get_tasks(limit_fields_to=["id", "data"])
```

## Features

### Idempotent Bulk Uploads

Avoid duplicate task creation using primary-key fields.

```python
report, responses = ls.create_tasks(
    tasks=my_tasks,
    pk_datafields="image"
)
```
- Skips existing tasks
- Chunked uploads (respects connection size limits)
- Detailed report of created vs existing tasks, as well as chunk api responses
- Optional dry-run mode


### Client-Side Task Caching

Speed up repeated lookups.
```python
ls.cache_tasks()
ls.cache_task_by_pk("image")

existing = ls.task_exists(task_data, data_fields="image", use_cache=True)
```

Useful for:
- Deduplication
- Fast existence checks
- Large dataset management

### Advanced Task Retrieval
```python
from labelstudio_tools.utils import simple_task_filter_builder
my_filter = simple_task_filter_builder(field='myDatafield', value='some_value', operator='equal')
#{"conjunction": "and",
# "items": [{"filter": f"filter:tasks:data.myDatafield",
#          "operator": "equal",
#          "value": "some_value",
#          "type": "String"}]
# }
tasks = ls.get_tasks(
    with_annotations=True,
    view="Some Specified View",
    filter_dict=my_filter,
)
```
Supports:
- View-based filtering
- Explicit ID selection
- Pagination auto-handling for large requests, with progress bar
- Resolving S3 URLs to presigned URLs
- Optional inclusion of annotations

### S3 Integration

Optional S3 support for validating and transferring task data, if s3_config specified.
Transfer functions have a `clobber` argument that will skip the actual transfer if a same-key or same-filename already exists. 

```python
ls.s3key_to_url('somewhere/something') # --> 's3://mybucket/somewhere/something'
ls.s3key_exists('somewhere/something') # --> false
ls.upload_s3url('path/to/local_file.ext', s3url='s3://mybucket/somewhere/something', clobber=False)
ls.s3key_exists('somewhere/something') # --> true
ls.download_s3url('s3://mybucket/somewhere/something', outfile='path/to/downloaded_file.ext', clobber=False)
```

### Snapshot & Export Management

Great for bulk-downloads of your data and annotation
Managed via SnapshotManager.
```python
from labelstudio_tools import SnapshotManager

snapman = SnapshotManager(host=..., token=..., project=...)
snapman.make_snapshot(title=..., filter_obj=...)

# check or wait for snapshot to be ready
snapman.is_snap_ready()  # --> true/false or...
snapman.wait_for_snapshot_completion(sleep_cycle_seconds=10)

# then downlaod
data = snapman.download_snap()

# and optionally cleanup if you will not be downloading again
snapman.cleanup_snapshot()
```

### Label Cache Management

Cached Labels is an experimental Labelstudio feature that creates datafields from annotations or predictions. It's the only way to filter on annotations at time of writing. If an annotation/prediction is ever updated, label-caching will have to be re-run for the changes to be reflected in that data field. The following functions automates the requests for creating Cached Labels across multiple labels. If there are many tasks, requests can time out. The functions below can automatically chunk tasks using views to avoid timeouts. 

```python
ls.update_cachelabels(
    control_tags=["my_annotation_label", "another_label"],
    with_counters=False,
    from_predictions = False,
    timeout_groups = 'auto'
)
```


# License

MIT License