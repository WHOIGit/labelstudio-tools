"""Shared config loading for labelstudio-tools modules.

All `from_config` entry points (ProjectManager, TaskManager, ...) route through
`load_config` so that auth resolution, path resolution, validation, and tool
namespacing live in one place.
"""
import copy
import json
import os
import warnings
from typing import Union


def load_config(config: Union[str, dict],
                auth_config: Union[str, dict] = None) -> dict:
    """Load a project config, merge auth, validate, and return the merged dict.

    Auth resolution layers (each overrides the previous):
      1. `config['auth']` — string path to a sidecar auth file. Resolved
         relative to the config file's directory first, then cwd (with a
         warning). Base layer.
      2. Inline secrets in `config` — override file values; a UserWarning
         fires when an inline value differs from the file value.
      3. `auth_config` parameter (path or dict) — silently overrides
         everything below.

    Auth-file load failure is deferred: only raises FileNotFoundError if a
    required value (`token`, per-storage `aws_access_key_id` /
    `aws_secret_access_key`) is missing. ML-backend auth (`user`/`pass`) is
    always optional. With no auth file at all, missing required values raise
    ValueError.
    """
    config_path = config if isinstance(config, str) else None
    config = _load_json(config)
    config = _merge_auth(config, config_path, auth_config)
    _check_required_auth(config)
    config.pop('__auth_file_error__', None)
    return config


def tool_section(merged: dict) -> dict:
    """Return the `labelstudio-tools` section (or {} if missing).

    Tool-specific settings live under this namespace so they don't pollute the
    LS-state space. Keys are flat (e.g. `pk`, `cache`, `storage_title`).
    """
    return merged.get('labelstudio-tools', {})


def project_ref(merged: dict) -> Union[int, str, None]:
    """Return the project identifier: `project_id` (int) if set, else `project` (title)."""
    if merged.get('project_id') is not None:
        return merged['project_id']
    return merged.get('project')


def find_storage(merged: dict, *, bucket: str = None, title: str = None,
                 endpoint_url: str = None) -> dict:
    """Find a single storage entry in merged config by criteria.

    Raises ValueError if 0 or >1 entries match. With no criteria, returns the
    only storage (errors if there isn't exactly one).
    """
    storages = merged.get('storage', [])
    matches = [s for s in storages
               if (bucket is None or s.get('bucket') == bucket)
               and (title is None or s.get('title') == title)
               and (endpoint_url is None or s.get('endpoint_url') == endpoint_url)]
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one storage matching "
            f"bucket={bucket!r} title={title!r} endpoint_url={endpoint_url!r}; "
            f"got {len(matches)}")
    return matches[0]


def storage_to_s3_config(storage: dict) -> dict:
    """Extract s3-client kwargs from a (auth-merged) storage entry."""
    out = {}
    field_map = {
        'bucket': 'bucket',
        'endpoint_url': 'endpoint_url',
        'aws_access_key_id': 'aws_access_key_id',
        'aws_secret_access_key': 'aws_secret_access_key',
        'aws_session_token': 'aws_session_token',
        'bucket_prefix': 'prefix',
    }
    for src, dst in field_map.items():
        if storage.get(src) is not None:
            out[dst] = storage[src]
    return out


# --- internal: file/auth loading -------------------------------------------

def _load_json(path_or_dict: Union[str, dict]) -> dict:
    if isinstance(path_or_dict, dict):
        return path_or_dict
    with open(path_or_dict) as f:
        return json.load(f)


def _resolve_auth_path(auth_path: str, config_path: str = None) -> str:
    """Resolve a relative auth-file path against the config file's dir, then cwd.

    Absolute paths are used as-is. Returns the absolute path; raises
    FileNotFoundError if no candidate exists. Emits a warning when falling
    back to cwd.
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
    """Merge auth values into config; see `load_config` for layering semantics.

    Returns a new dict. Auth-file load errors are stashed under
    `__auth_file_error__` and are only re-raised by `_check_required_auth`
    when a required value is still missing.
    """
    auth_file_data = None
    auth_file_error = None
    if config.get('auth'):
        try:
            resolved = _resolve_auth_path(config['auth'], config_path)
            with open(resolved) as f:
                auth_file_data = json.load(f)
        except FileNotFoundError as e:
            auth_file_error = e

    override_data = None
    if auth_override is not None:
        if isinstance(auth_override, dict):
            override_data = auth_override
        else:
            with open(auth_override) as f:
                override_data = json.load(f)

    merged = copy.deepcopy(config)
    merged.pop('auth', None)

    if auth_file_data is not None:
        _apply_auth_layer(merged, auth_file_data,
                          mode='fill_missing', warn_on_collision=True)
    if override_data is not None:
        _apply_auth_layer(merged, override_data,
                          mode='override', warn_on_collision=False)

    merged['__auth_file_error__'] = auth_file_error
    return merged


def _check_required_auth(merged: dict) -> None:
    """Raise if required auth values are missing. ML-backend auth is exempt."""
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
    """Apply one auth layer onto merged (mutates in place)."""
    host = merged.get('host')
    if host is not None:
        match = _find_one(auth_data.get('labelstudio', []),
                          lambda e: e.get('host') == host,
                          name=f"labelstudio host={host!r}")
        if match is not None:
            _apply_layer_dict(merged, match, skip=('host',),
                              mode=mode, warn_on_collision=warn_on_collision,
                              context=f"labelstudio[{host!r}]")

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
