import json
import os
from functools import wraps
from itertools import islice
import math

def read_token(token_path):
    if os.path.isfile(token_path):
        with open(token_path) as f:
            return f.read().strip()
    return token_path  # assume it's the token itself

def attr_list_decorator(func):
    @wraps(func)
    def wrapper(self, *args, attrs=None, **kwargs):
        items = func(self, *args, **kwargs)   # pass self (and any args)
        if attrs is None:
            return items
        if isinstance(attrs, list):
            return [tuple(item[a] if isinstance(item,dict) else getattr(item, a) for a in attrs) for item in items]
        return [item[attrs] if isinstance(item,dict) else getattr(item, attrs) for item in items]
    return wrapper
# todo add 'dict_key' which returns a dict instead, based on the key
# todo add able to enter not just attributes but dict-keys and list index
# todo recursively access attrs and dicts

def chunk_my_dict(d, chunksize):
    it = iter(d.items())
    while True:
        chunk = dict(islice(it, chunksize))
        if not chunk:
            break
        yield chunk


def total_results_count(d:dict) -> int:
    # d = {"predictions": [{"results": [...]}, {"results": [...]}, ...]}
    preds = d.get("predictions", [])
    annots = d.get("annotations", [])
    pred_results_count = sum([len(pred.get("result", [])) for pred in preds])
    annots_results_count = sum([len(annot.get("result", [])) for annot in annots])
    return pred_results_count + annots_results_count

def largest_by_results_count(ls_dicts):
    # returns (index, dict, count)
    best_i, best_d, best_count = -1, None, -1
    for idx, ls_dict in enumerate(ls_dicts):
        results_count = total_results_count(ls_dict)
        if results_count > best_count:
            best_i, best_d, best_count = idx, ls_dict, results_count
    return best_i, best_d, best_count

def json_size_mb(obj, *, ensure_ascii=False, separators=(",", ":"), sort_keys=False) -> float:
    b = json.dumps(
        obj,
        ensure_ascii=ensure_ascii,
        #separators=separators,
        sort_keys=sort_keys
    ).encode("utf-8")
    return len(b) / (1024 * 1024)

def estimate_chunks(ls_tasks, MAX_MB):
    _, biggest_task, biggest_results_count = largest_by_results_count(ls_tasks)
    biggest_task_mb = json_size_mb(biggest_task)
    if biggest_task_mb > MAX_MB:
        raise ValueError(f"Largest single task is {biggest_task_mb:.2f} MiB > {MAX_MB} MiB")
    total_MB_estimate = len(ls_tasks) * biggest_task_mb  # an over-estimate
    chunk_count = math.ceil(total_MB_estimate / MAX_MB)
    chunk_size = math.ceil(len(ls_tasks) / chunk_count)
    #print(f'biggest_task_mb: {biggest_task_mb} ({biggest_results_count} results), total_MB_estimate: {total_MB_estimate}, chunk_size: {chunk_size}, chunk_amount: {chunk_count}')
    return chunk_size, chunk_count
