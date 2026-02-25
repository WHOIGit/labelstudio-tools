import os
import json
from label_studio_sdk.data_manager import Filters, Column, Type, Operator

Types = [v for k, v in vars(Type).items() if not k.startswith('_')]
Operators = [v for k, v in vars(Operator).items() if not k.startswith('_')]
Columns = [v for k, v in vars(Column).items() if not k.startswith('_') or k!='data']


def simple_task_filter_builder(field, value, operator='equal', fieldtype='String'):
    filter_dict = {"conjunction": "and",
        "items": [{"filter": f"filter:tasks:data.{field}",
                 "operator": operator,
                 "value": value,
                 "type": fieldtype}]
        }
    return filter_dict


def parse_task_filter(filter_obj, data_fields=None):
    if isinstance(filter_obj, dict):
        task_filter = filter_obj
    elif os.path.isfile(filter_obj):
        with open(filter_obj) as f:
            task_filter = json.load(f)
    else:
        task_filter = json.loads(filter_obj)

    assert 'items' in task_filter
    assert isinstance(task_filter['items'], list)
    assert 'conjunction' in task_filter
    conjunction = task_filter['conjunction'].lower()
    assert conjunction in [Filters.OR,Filters.AND]

    filter_items = []
    for item in task_filter['items']:
        assert item['type'] in Types
        assert item['operator'] in Operators

        filter_field = item['filter']
        if filter_field.startswith('filter:'):
            # will naivly be added back by Filters.item(...)
            filter_field = filter_field.split(":",1)[1]

        if not filter_field in Columns:  # then it's a data field
            if filter_field.startswith('tasks:data.'):
                # will naivly be added back by Column.data(...)
                filter_field = filter_field.split(".",1)[1]
            if data_fields:
                # todo fails on empty project, because there are no fields
                #print('parse_task_filter data_fields:',data_fields)
                assert filter_field in data_fields
            filter_field = Column.data(filter_field)
        else:
            filter_field = item['filter']

        filter_item = Filters.item(
            filter_field,
            item['operator'],
            item['type'],
            Filters.value(item['value']) )
        filter_items.append( filter_item )

    return Filters.create(conjunction, filter_items)