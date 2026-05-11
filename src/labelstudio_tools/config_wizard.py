"""Interactive wizard for creating a new Label-Studio project config (TOML).

Run with:
    python -m labelstudio_tools.config_wizard [-v] [--dir DIR] [--auth FILE] [-o OUT]

Always writes a NEW project config (warns before overwriting). For editing an
existing config, just open the .toml file in an editor.
"""
import argparse
import itertools
import re
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import questionary
import requests
import tomllib
from PIL import ImageColor
from prompt_toolkit import print_formatted_text
from prompt_toolkit.formatted_text import FormattedText
from tabulate import tabulate


# --- Curated colors (hex + display name) ----------------------------------

CURATED_COLORS = [
    ("White",  "#FDFDFC"),
    ("Red",    "#FF4C25"),
    ("Orange", "#FF750F"),
    ("Yellow", "#ECB800"),
    ("Green",  "#9AC422"),
    ("Pine",   "#34988D"),
    ("Blue",   "#617ADA"),
    ("Purple", "#CC6FBE"),
]

# --- Per-field descriptions (printed when verbose / "Include descriptions") ---

DESCRIPTIONS = {
    "config_dir":   "Directory holding TOML configs. Existing configs in here will be offered as 'Load from' options. Default output location for new files.",
    "auth_file":    "Auth file holds secrets (LS token, S3 keys, ML-backend creds). Choose existing, [New] (create separate file), [inline] (embed in project config), or [defer] (skip auth entirely).",
    "host":         "Label Studio instance URL (e.g. https://ls.example.org).",
    "token":        "Personal API token from Label Studio (Account & Settings → Access Token). Leave blank to defer.",
    "project_name": "Display name for the project on Label Studio. Must be unique on the instance.",
    "shortname":    "Internal identifier (no spaces or path separators). Used to suggest filenames.",
    "outfile":      "Filename to write the new project config to.",
    "label_config": "Labeling-config XML file (defines the LS labeling UI). This wizard does NOT validate the XML — out of scope.",
    "description":  "Free-text project description shown in LS.",
    "task_sampling":"How tasks are presented: sequential (in order) or random/uniform (shuffle).",
    "color":        "Project tile color in LS UI.",
    "pk":           "Field name in task data used as the primary identifier (e.g. 'image').",
    "cache":        "TaskManager cache backend. 'RAM' is the only option currently.",
    "storage":      "Connect a bucket. mode=source/import (data in) or target/export (data out).",
    "ml_backend":   "Connect an ML model server. Used for prelabeling and/or active learning.",
    "annotations":  "Default annotation behavior settings.",
}


# Per-field one-liner comments, only emitted by --default --verbose.
FIELD_COMMENTS = {
    "host":         "Label Studio instance URL (REQUIRED).",
    "project":      "Display name on Label Studio.",
    "label_config": "Path to labeling-config XML (REQUIRED).",
    "auth":         "Path to auth file holding secrets.",
    "token":        "LS API token (REQUIRED — usually lives in the auth file).",
    "pk":           "Field in task data used as primary identifier.",
    "cache":        "TaskManager cache backend ('RAM' is the only option).",
    "description":  "Free-text project description.",
    "task_sampling":"Order tasks are presented: sequential | uniform | uncertainty.",
    "color":        "Project tile color in LS UI (#RRGGBB).",
    "type":         "Storage backend type (only 's3' implemented).",
    "mode":         "source/import (data in) or target/export (data out).",
    "title":        "Display name for this storage in LS.",
    "bucket":       "S3 bucket name.",
    "endpoint_url": "S3 endpoint URL.",
    "bucket_prefix":"Prefix path within the bucket (optional).",
    "presigned_urls":"Generate presigned URLs for tasks.",
    "presigned_urls_expiry":"Presigned-URL expiry in minutes.",
    "import_method":"'tasks' (tasks from json, jsonl, parquet files) or 'blobs' (tasks from media-files).",
    "file_name_filter":"Regex filter for filenames (default '(?!)' = none).",
    "scan_all_subfolders":"Recurse into subfolders.",
    "name":         "ML backend display name.",
    "backend_url":  "ML backend service URL.",
    "interactive":  "Allow interactive prelabeling tools (smart tools).",
    "extra_params": "JSON object of extra params passed to the backend.",
    "start_training_on_annotation_update":"Trigger training on each annotation save.",
    "annotation_prelabeling":"Auto-apply backend predictions as annotations.",
    "instructions": "Annotator instructions text.",
    "show_before_labeling":"Show instructions modal before labeling.",
    "aws_access_key_id":     "S3 access key ID.",
    "aws_secret_access_key": "S3 secret access key.",
    "user":         "HTTP basic-auth user for ML backend.",
    "pass":         "HTTP basic-auth password for ML backend.",
}


# --- TOML emitter (tomli_w can't render [[arrays-of-tables]] as blocks) ----

class TomlBuilder:
    def __init__(self):
        self.lines: list[str] = []

    def header_comment(self, text: str) -> None:
        for line in text.splitlines():
            self.lines.append(f"# {line}" if line else "#")
        self.lines.append("")

    def kv(self, key: str, value: Any, comment: Optional[str] = None) -> None:
        line = f"{key} = {self._fmt(value)}"
        if comment:
            line += f"  # {comment}"
        self.lines.append(line)

    def section(self, name: str) -> None:
        self._ensure_blank()
        self.lines.append(f"[{name}]")

    def array_section(self, name: str) -> None:
        self._ensure_blank()
        self.lines.append(f"[[{name}]]")

    def comment(self, text: str) -> None:
        for line in text.splitlines():
            self.lines.append(f"# {line}" if line else "#")

    def blank(self) -> None:
        if self.lines and self.lines[-1] != "":
            self.lines.append("")

    def render(self) -> str:
        out = "\n".join(self.lines)
        return out + ("\n" if not out.endswith("\n") else "")

    def _ensure_blank(self) -> None:
        if self.lines and self.lines[-1] != "":
            self.lines.append("")

    def _fmt(self, v: Any) -> str:
        if v is None:
            return '""'
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (int, float)):
            return str(v)
        if isinstance(v, str):
            return self._fmt_str(v)
        if isinstance(v, dict):
            if not v:
                return "{}"
            inner = ", ".join(f"{k} = {self._fmt(val)}" for k, val in v.items())
            return "{ " + inner + " }"
        if isinstance(v, (list, tuple)):
            inner = ", ".join(self._fmt(x) for x in v)
            return "[" + inner + "]"
        raise TypeError(f"Cannot serialize value of type {type(v).__name__}: {v!r}")

    @staticmethod
    def _fmt_str(s: str) -> str:
        # Use TOML basic strings with escaping; switch to literal if simpler.
        esc = (s.replace("\\", "\\\\")
                .replace('"', '\\"')
                .replace("\b", "\\b")
                .replace("\t", "\\t")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\f", "\\f"))
        return f'"{esc}"'


# --- Wizard state ----------------------------------------------------------

@dataclass
class State:
    args: argparse.Namespace
    descriptions: bool = False
    config_dir: Optional[Path] = None
    # Auth
    auth_mode: str = ""               # 'existing' | 'new' | 'inline' | 'defer'
    auth_path: Optional[Path] = None  # picked existing OR target for [New]; None for inline/defer
    auth_data: dict = field(default_factory=lambda: {"labelstudio": [], "storage": [], "ml_backend": []})
    # Host / project
    host: Optional[str] = None
    token: Optional[str] = None       # in-memory; landing place depends on auth_mode
    token_works: Optional[bool] = None
    project_name: Optional[str] = None
    shortname: Optional[str] = None
    outfile: Optional[Path] = None
    # Sections being built
    label_config: Optional[str] = None
    description: str = ""
    sampling: str = "sequential"
    color: str = "#FF4C25"
    color_comment: Optional[str] = None
    pk: str = "image"
    cache: str = "RAM"
    storages: list = field(default_factory=list)        # each: {'data': dict, 'comments': dict}
    ml_backends: list = field(default_factory=list)
    annotations: dict = field(default_factory=lambda: {"instructions": "", "show_before_labeling": False})
    # Per-session caches
    _cache_other_storages: Optional[list] = None
    _cache_other_ml: Optional[list] = None
    _cache_ls_storages: Optional[list] = None
    _cache_ls_ml: Optional[list] = None
    _ls_client: Any = None  # lazy LabelStudio client


def describe(state: State, key: str) -> None:
    if state.descriptions and key in DESCRIPTIONS:
        questionary.print(f"  → {DESCRIPTIONS[key]}", style="italic fg:#888888")


# --- Validators ------------------------------------------------------------

def validate_ls_token(host: str, token: str, timeout: int = 10) -> tuple[bool, str]:
    auth_type = "Token" if len(token) <= 40 else "Bearer"
    try:
        r = requests.get(urljoin(host, "/api/version"),
                         headers={"Authorization": f"{auth_type} {token}"},
                         timeout=timeout, allow_redirects=True)
        return r.status_code == 200, f"HTTP {r.status_code}"
    except Exception as e:
        return False, str(e)


def validate_storage(s: dict) -> tuple[bool, str]:
    if not s.get("aws_access_key_id") or not s.get("aws_secret_access_key"):
        return False, "credentials not available (deferred)"
    try:
        import boto3
        client = boto3.client(
            "s3",
            endpoint_url=s.get("endpoint_url"),
            aws_access_key_id=s["aws_access_key_id"],
            aws_secret_access_key=s["aws_secret_access_key"],
        )
        client.head_bucket(Bucket=s["bucket"])
        return True, "head_bucket OK"
    except Exception as e:
        return False, str(e).splitlines()[0]


def validate_ml_backend(url: str, user: Optional[str] = None,
                        password: Optional[str] = None,
                        timeout: int = 10) -> tuple[bool, str]:
    auth = (user, password) if user else None
    try:
        r = requests.get(url, auth=auth, timeout=timeout, allow_redirects=True)
        return r.status_code == 200, f"HTTP {r.status_code}"
    except Exception as e:
        return False, str(e)


# --- Color helpers ---------------------------------------------------------

_HEX6 = re.compile(r"^#[0-9a-fA-F]{6}$")
_HEX3 = re.compile(r"^#[0-9a-fA-F]{3}$")


def parse_color(s: str) -> tuple[str, Optional[str]]:
    """Return (hex, comment-name-or-None). Raises ValueError on bad input."""
    s = s.strip()
    if _HEX6.match(s):
        return s.upper(), None
    if _HEX3.match(s):
        return "#" + "".join(c * 2 for c in s[1:]).upper(), None
    name = s.lower().replace(" ", "")
    if name in ImageColor.colormap:
        rgb = ImageColor.getrgb(name)
        return "#{:02X}{:02X}{:02X}".format(*rgb), s.lower()
    raise ValueError(f"not a hex (#RRGGBB) or recognized CSS color name: {s!r}")


# --- Path / host helpers ---------------------------------------------------

def host_subdomain(host: str) -> str:
    p = urlparse(host)
    netloc = p.netloc or p.path
    netloc = netloc.split(":")[0]
    parts = netloc.split(".")
    return parts[0] if parts and parts[0] else "host"


def list_toml_files(d: Path, prefix_first: str = "") -> list[Path]:
    """List *.toml in dir; sort with `prefix_first`-prefixed entries first."""
    if not d.is_dir():
        return []
    files = sorted(d.glob("*.toml"))
    if prefix_first:
        pri = [f for f in files if f.name.startswith(prefix_first)]
        rest = [f for f in files if not f.name.startswith(prefix_first)]
        return pri + rest
    return files


def auth_path_for_project(auth_path: Optional[Path], project_dir: Path) -> str:
    """Render auth path string for the project config: bare name if same dir, else absolute."""
    if auth_path is None:
        return ""
    auth_path = auth_path.resolve()
    project_dir = project_dir.resolve()
    if auth_path.parent == project_dir:
        return auth_path.name
    return str(auth_path)


# --- Prompt helpers --------------------------------------------------------

def ask_yn(msg: str, default: bool = True) -> bool:
    return questionary.confirm(msg, default=default).unsafe_ask()


def ask_text(msg: str, default: str = "", validate=None) -> str:
    return questionary.text(msg, default=default, validate=validate).unsafe_ask()


def ask_password(msg: str, default: str = "") -> str:
    """Masked text input. Falls back to displaying default after entry if blank."""
    val = questionary.password(msg).unsafe_ask()
    if val == "" and default:
        return default
    return val


def ask_select(msg: str, choices: list, default=None) -> Any:
    return questionary.select(msg, choices=choices, default=default).unsafe_ask()


class Spinner:
    """Tiny threaded spinner: with Spinner('Validating'): ..."""

    FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]

    def __init__(self, msg: str):
        self.msg = msg
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def __enter__(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        if self._thread:
            self._thread.join()
        sys.stdout.write("\r" + " " * (len(self.msg) + 4) + "\r")
        sys.stdout.flush()

    def _run(self):
        for ch in itertools.cycle(self.FRAMES):
            if self._stop.is_set():
                return
            sys.stdout.write(f"\r{ch} {self.msg}")
            sys.stdout.flush()
            time.sleep(0.1)


# --- Step: descriptions toggle ---------------------------------------------

def step_descriptions(state: State) -> None:
    if state.args.verbose:
        state.descriptions = True
        questionary.print("(verbose: descriptions ON)", style="italic fg:#888888")
        return
    state.descriptions = ask_yn("Include config descriptions?", default=False)


# --- Step: config directory ------------------------------------------------

def step_config_dir(state: State) -> None:
    describe(state, "config_dir")
    if state.args.dir:
        state.config_dir = Path(state.args.dir).resolve()
    else:
        d = ask_text("Config directory:", default="./configs/")
        state.config_dir = Path(d).resolve()
    state.config_dir.mkdir(parents=True, exist_ok=True)


# --- Step: auth file -------------------------------------------------------

def _load_auth_file(path: Path) -> dict:
    with open(path, "rb") as f:
        d = tomllib.load(f)
    # Normalize singular [storage]/[labelstudio]/[ml_backend] to lists.
    for k in ("labelstudio", "storage", "ml_backend"):
        v = d.get(k)
        if isinstance(v, dict):
            d[k] = [v]
        elif v is None:
            d[k] = []
    return d


def step_auth_file(state: State) -> None:
    describe(state, "auth_file")
    NEW = "[New auth file]"
    INLINE = "[inline — embed secrets in project config]"
    DEFER = "[defer — leave auth blank, skip secret prompts]"

    if state.args.auth:
        state.auth_path = Path(state.args.auth).resolve()
        if not state.auth_path.is_file():
            questionary.print(f"WARNING: --auth file does not exist: {state.auth_path}",
                              style="fg:#ff8800")
            sys.exit(1)
        state.auth_mode = "existing"
        state.auth_data = _load_auth_file(state.auth_path)
        return

    existing = list_toml_files(state.config_dir, prefix_first="ls_auth")
    choices = [str(p.name) for p in existing] + [NEW, INLINE, DEFER]
    pick = ask_select("Auth file:", choices=choices)
    if pick == DEFER:
        state.auth_mode = "defer"
        return
    if pick == INLINE:
        state.auth_mode = "inline"
        return
    if pick == NEW:
        state.auth_mode = "new"
        default = (f"ls_auth.{host_subdomain(state.host)}.toml"
                   if state.host else "ls_auth.toml")
        while True:
            v = ask_text("New auth filename:", default=default)
            p = _resolve_outfile(v, state.config_dir)
            if p.exists():
                if not ask_yn(f"Overwrite existing {p}?", default=False):
                    continue
            state.auth_path = p
            return
    # existing
    state.auth_mode = "existing"
    state.auth_path = state.config_dir / pick
    state.auth_data = _load_auth_file(state.auth_path)


# --- Step: host ------------------------------------------------------------

def _ping_host(host: str, timeout: int = 10) -> tuple[bool, str]:
    try:
        with Spinner(f"pinging {host}"):
            r = requests.get(host, timeout=timeout, allow_redirects=True)
        return True, f"HTTP {r.status_code}"
    except Exception as e:
        return False, str(e).splitlines()[0]


def step_host(state: State) -> None:
    describe(state, "host")
    host = ask_text("Host URL:", default=state.host or "https://",
                    validate=lambda v: bool(urlparse(v).netloc) or "must be a URL")
    while True:
        ok, msg = _ping_host(host)
        if ok:
            questionary.print(f"✓ reachable ({msg})", style="fg:#00aa00")
            state.host = host
            return
        questionary.print(f"✗ ping failed: {msg}", style="fg:#cc4400")
        action = ask_select("Action:", choices=["edit", "retry", "continue (skip ping)"])
        if action == "edit":
            host = ask_text("Host URL:", default=host,
                            validate=lambda v: bool(urlparse(v).netloc) or "must be a URL")
            continue
        if action == "retry":
            continue  # re-ping same host
        state.host = host
        return


# --- Step: token + validation ----------------------------------------------

def _existing_token_for_host(state: State) -> Optional[str]:
    if state.auth_mode != "existing":
        return None
    for e in state.auth_data.get("labelstudio", []):
        if e.get("host") == state.host:
            return e.get("token")
    return None


def step_token(state: State) -> None:
    describe(state, "token")
    if state.auth_mode == "defer":
        questionary.print("(auth deferred — skipping token)", style="italic fg:#888888")
        return
    existing = _existing_token_for_host(state)
    if existing:
        state.token = existing
    else:
        token = questionary.password("API token (blank → defer):").unsafe_ask()
        if not token:
            questionary.print("(blank token — proceeding with auth deferred for LS)",
                              style="italic fg:#888888")
            state.token = None
            return
        state.token = token
        # New auth file: stash labelstudio entry. Inline: kept as state.token (written inline later).
        if state.auth_mode == "new":
            state.auth_data["labelstudio"].append({"host": state.host, "token": token})
    if state.token:
        with Spinner("validating token"):
            ok, msg = validate_ls_token(state.host, state.token)
        state.token_works = ok
        if ok:
            questionary.print(f"✓ token works ({msg})", style="fg:#00aa00")
        else:
            questionary.print(f"✗ token check failed ({msg})", style="fg:#cc4400")
            if not ask_yn("Continue anyway?", default=True):
                sys.exit(1)


# --- Step: project name (with dup check) -----------------------------------

def _ls_client(state: State):
    if state._ls_client is not None:
        return state._ls_client
    if not state.token_works:
        return None
    from label_studio_sdk import LabelStudio
    state._ls_client = LabelStudio(base_url=state.host, api_key=state.token)
    return state._ls_client


def step_project_name(state: State) -> None:
    describe(state, "project_name")
    existing_titles = set()
    if state.token_works:
        try:
            client = _ls_client(state)
            existing_titles = {p.title for p in client.projects.list()}
        except Exception as e:
            questionary.print(f"(could not list existing projects: {e})",
                              style="italic fg:#888888")
    while True:
        name = ask_text("Project name:")
        if not name.strip():
            questionary.print("Project name cannot be empty.", style="fg:#cc4400")
            continue
        if name in existing_titles:
            questionary.print(f"WARNING: a project named {name!r} already exists on {state.host}.",
                              style="fg:#ff8800")
            if not ask_yn("Use this name anyway?", default=False):
                continue
        state.project_name = name
        return


# --- Step: shortname -------------------------------------------------------

_SHORTNAME_BAD = re.compile(r"[\s/\\]")


def step_shortname(state: State) -> None:
    describe(state, "shortname")
    default = re.sub(r"[\s/\\]+", "-", state.project_name).strip("-").lower() if state.project_name else ""
    while True:
        v = ask_text("Project shortname (no spaces/slashes):", default=default)
        if not v:
            questionary.print("Shortname cannot be empty.", style="fg:#cc4400")
            continue
        if _SHORTNAME_BAD.search(v):
            questionary.print("Shortname cannot contain spaces or path separators.",
                              style="fg:#cc4400")
            continue
        state.shortname = v
        return


# --- Step: outfile ---------------------------------------------------------

def _resolve_outfile(value: str, config_dir: Path) -> Path:
    """Bare filename → in config_dir; anything with directory → relative to cwd."""
    p = Path(value)
    if p.is_absolute():
        return p.resolve()
    if p.parent == Path("."):  # bare filename
        return (config_dir / p.name).resolve()
    return p.resolve()


def step_outfile(state: State) -> None:
    describe(state, "outfile")
    default = f"ls_project.{state.shortname}.toml"
    if state.args.outfile:
        # CLI override: bare name → config_dir; with a path → cwd-relative.
        # If --dir was also given, bare names go in --dir.
        state.outfile = _resolve_outfile(state.args.outfile, state.config_dir)
    else:
        v = ask_text("Output filename:", default=default)
        state.outfile = _resolve_outfile(v, state.config_dir)


# --- Step: label_config ----------------------------------------------------

def step_label_config(state: State) -> None:
    describe(state, "label_config")
    if not state.descriptions:
        questionary.print("(this wizard does not validate XML — out of scope)",
                          style="italic fg:#888888")
    xmls = sorted(state.config_dir.glob("*.xml"))
    MANUAL = "[enter .xml filename]"
    if xmls:
        choices = [p.name for p in xmls] + [MANUAL]
        pick = ask_select("Label config XML:", choices=choices)
        if pick != MANUAL:
            state.label_config = pick
            return
    state.label_config = ask_text("Label config XML path:", default="config/label_ui.xml")


# --- Step: description / sampling / color ----------------------------------

def step_general(state: State) -> None:
    describe(state, "description")
    state.description = ask_text("Description:", default="")

    describe(state, "task_sampling")
    pick = ask_select("Task sampling:",
                      choices=["sequential", "random (uniform)", "uncertainty"],
                      default="sequential")
    state.sampling = "sequential" if pick.startswith("sequential") else (
        "uniform" if pick.startswith("random") else "uncertainty")

    describe(state, "color")
    CUSTOM = "[Custom: hex or CSS color name]"
    color_choices = [f"{name}  {hexv}" for name, hexv in CURATED_COLORS] + [CUSTOM]
    pick = ask_select("Color:", choices=color_choices)
    if pick == CUSTOM:
        while True:
            raw = ask_text("Hex (#RRGGBB) or CSS color name:")
            try:
                hex_v, name_comment = parse_color(raw)
                state.color = hex_v
                state.color_comment = name_comment
                break
            except ValueError as e:
                questionary.print(f"Invalid: {e}", style="fg:#cc4400")
    else:
        idx = color_choices.index(pick)
        name, hexv = CURATED_COLORS[idx]
        state.color = hexv
        state.color_comment = name


# --- Step: labelstudio-tools ----------------------------------------------

def step_lstools(state: State) -> None:
    describe(state, "pk")
    state.pk = ask_text("Primary key field (pk):", default="image")
    state.cache = "RAM"


# --- Storage: defaults + sources -------------------------------------------

STORAGE_DEFAULTS = {
    "type": "s3",
    "mode": "source",
    "presigned_urls": True,
    "presigned_urls_expiry": 15,
    "bucket_prefix": "",
    "import_method": "tasks",
    "file_name_filter": "(?!)",
    "scan_all_subfolders": True,
}


def _ls_storage_to_cfg(s, mode: str) -> dict:
    """Convert SDK storage object → wizard storage dict."""
    return {
        "type": "s3",
        "mode": mode,
        "title": getattr(s, "title", None) or "",
        "bucket": getattr(s, "bucket", None) or "",
        "endpoint_url": getattr(s, "s3endpoint", None) or "",
        "bucket_prefix": getattr(s, "prefix", None) or "",
        "presigned_urls": bool(getattr(s, "presign", False)),
        "presigned_urls_expiry": int(getattr(s, "presign_ttl", 15) or 15),
        "file_name_filter": getattr(s, "regex_filter", None) or "(?!)",
        "scan_all_subfolders": bool(getattr(s, "recursive_scan", True)),
        "import_method": "blobs" if getattr(s, "use_blob_urls", False) else "tasks",
    }


def collect_other_storages(state: State) -> list[dict]:
    if state._cache_other_storages is not None:
        return state._cache_other_storages
    out = []
    for path in list_toml_files(state.config_dir):
        if state.outfile and path.resolve() == state.outfile.resolve():
            continue
        try:
            with open(path, "rb") as f:
                data = tomllib.load(f)
        except Exception:
            continue
        storages = data.get("storage", [])
        if isinstance(storages, dict):
            storages = [storages]
        for s in storages:
            s = dict(s)
            if not _is_complete_storage_option(s):
                continue
            out.append({"data": s, "source": path.name})
    state._cache_other_storages = out
    return out


def collect_ls_storages(state: State) -> list[dict]:
    if state._cache_ls_storages is not None:
        return state._cache_ls_storages
    if not state.token_works:
        state._cache_ls_storages = []
        return []
    out = []
    try:
        client = _ls_client(state)
        for proj in client.projects.list():
            for s in client.import_storage.s3.list(project=proj.id) or []:
                out.append({"data": _ls_storage_to_cfg(s, "source"),
                            "source": f"LS:{proj.title}"})
            for s in client.export_storage.s3.list(project=proj.id) or []:
                out.append({"data": _ls_storage_to_cfg(s, "target"),
                            "source": f"LS:{proj.title}"})
    except Exception as e:
        questionary.print(f"(could not list LS storages: {e})", style="italic fg:#888888")
    state._cache_ls_storages = out
    return out


def _storage_key(d: dict) -> tuple:
    return (d.get("type", "s3"), d.get("endpoint_url", ""), d.get("bucket", ""),
            d.get("bucket_prefix", ""), d.get("mode", ""))


def _is_complete_storage_option(d: dict) -> bool:
    """Return true for project-storage entries usable as preload options."""
    return (
        _normalize_mode(str(d.get("mode", ""))) in ("source", "target")
        and bool(d.get("bucket"))
        and bool(d.get("endpoint_url"))
    )


def _dedupe_storage_options(items: list[dict]) -> list[dict]:
    """Group by connection signature; merge sources comma-delimited."""
    by_key: dict[tuple, dict] = {}
    for it in items:
        k = _storage_key(it["data"])
        if k in by_key:
            by_key[k]["sources"].append(it["source"])
        else:
            by_key[k] = {"data": it["data"], "sources": [it["source"]]}
    return list(by_key.values())


def _storage_auth_available(state: State, storage_cfg: dict) -> bool:
    """Whether credentials are available inline or in the current auth data."""
    if storage_cfg.get("aws_access_key_id") and storage_cfg.get("aws_secret_access_key"):
        return True
    for e in state.auth_data.get("storage", []):
        if (e.get("type", "s3") == storage_cfg.get("type", "s3")
                and e.get("bucket") == storage_cfg.get("bucket")
                and e.get("endpoint_url") == storage_cfg.get("endpoint_url")
                and e.get("aws_access_key_id")
                and e.get("aws_secret_access_key")):
            return True
    return False


def _format_storage_choices(state: State, items: list[dict]) -> list:
    """Build a table-like questionary choice list with a header row."""
    auth_marker = "__OK__"
    rows = []
    for idx, item in enumerate(items):
        d = item["data"]
        rows.append({
            "idx": idx,
            "mode": d.get("mode", "?"),
            "endpoint": d.get("endpoint_url", ""),
            "bucket": d.get("bucket", ""),
            "prefix": d.get("bucket_prefix", ""),
            "auth": auth_marker if _storage_auth_available(state, d) else "",
            "source": ", ".join(item["sources"]),
        })
    table = tabulate(
        [[r["mode"], r["endpoint"], r["bucket"], r["prefix"], r["auth"], r["source"]]
         for r in rows],
        headers=["Mode", "Endpoint", "Bucket", "Prefix", "Auth", "Source"],
        tablefmt="plain",
    )
    lines = table.splitlines()
    if not lines:
        return []
    choices = [
        questionary.Separator(lines[0]),
        questionary.Separator("-" * len(lines[0])),
    ]
    for row, line in zip(rows, lines[1:]):
        if row["auth"]:
            before, _, after = line.partition(row["auth"])
            title = [
                ("", before),
                ("fg:#00aa00 bold", "✓"),
                ("", " " * (len(auth_marker) - 1)),
                ("", after),
            ]
        else:
            title = line
        choices.append(questionary.Choice(title=title, value=row["idx"]))
    return choices


def _print_kv(k: str, v: Any) -> None:
    print_formatted_text(FormattedText([
        ("fg:#00aa00", f"  {k}"),
        ("", f" = {v!r}"),
    ]))


# --- Storage: prompts (manual / edit) --------------------------------------

def _normalize_mode(s: str) -> str:
    s = s.strip().lower()
    if s in ("source", "import"):
        return "source"
    if s in ("target", "export"):
        return "target"
    return s


def prompt_storage(state: State, preload: Optional[dict] = None) -> Optional[dict]:
    """Run the storage entry prompts. Returns the storage dict, or None on cancel."""
    p = preload or {}
    s = dict(STORAGE_DEFAULTS)
    s.update(p)

    s["type"] = ask_text("type:", default=s.get("type", "s3"))
    if s["type"] != "s3":
        questionary.print("Only 's3' is implemented; reverting.", style="fg:#cc4400")
        s["type"] = "s3"
    s["endpoint_url"] = ask_text("endpoint_url:", default=s.get("endpoint_url", ""))
    s["bucket"] = ask_text("bucket:", default=s.get("bucket", ""))
    # Keys: only prompt if auth deferred OR no auth-side credentials present
    need_keys = state.auth_mode in ("inline", "new") or (
        state.auth_mode == "existing" and not _auth_has_storage_creds(state, s))
    if state.auth_mode == "defer":
        questionary.print("(auth deferred — skipping access/secret keys)",
                          style="italic fg:#888888")
    elif need_keys:
        ak = ask_text("aws_access_key_id:", default=s.get("aws_access_key_id", ""))
        sk = ask_password("aws_secret_access_key (hidden):",
                          default=s.get("aws_secret_access_key", ""))
        if ak:
            s["aws_access_key_id"] = ak
        if sk:
            s["aws_secret_access_key"] = sk
    s["bucket_prefix"] = ask_text("bucket_prefix:", default=s.get("bucket_prefix", ""))
    while True:
        m = ask_text("mode (source/import or target/export):", default=s.get("mode", "source"))
        m = _normalize_mode(m)
        if m in ("source", "target"):
            s["mode"] = m
            break
        questionary.print("Invalid mode. Try: source, import, target, export.",
                          style="fg:#cc4400")
    title_default = s.get("title") or (
        f"{s['bucket']}: {s['bucket_prefix']} ({s['mode']})"
        if s.get("bucket") else "")
    s["title"] = ask_text("title:", default=title_default)
    s["presigned_urls"] = ask_yn("presigned_urls?", default=bool(s.get("presigned_urls", True)))
    s["presigned_urls_expiry"] = int(ask_text(
        "presigned_urls_expiry (minutes):",
        default=str(s.get("presigned_urls_expiry", 15)),
        validate=lambda v: v.isdigit() or "must be a positive integer"))
    import_method_choices = {
        "tasks": "tasks (tasks from json, jsonl, parquet files)",
        "blobs": "blobs (tasks from media-files)",
    }
    import_method_pick = ask_select(
        "import_method:",
        choices=list(import_method_choices.values()),
        default=import_method_choices.get(s.get("import_method", "tasks"),
                                          import_method_choices["tasks"]))
    s["import_method"] = next(
        key for key, label in import_method_choices.items()
        if label == import_method_pick)
    s["file_name_filter"] = ask_text(
        "file_name_filter (regex; default '(?!)' = none):",
        default=s.get("file_name_filter", "(?!)"))
    s["scan_all_subfolders"] = ask_yn(
        "scan_all_subfolders?", default=bool(s.get("scan_all_subfolders", True)))
    return s


def _auth_has_storage_creds(state: State, storage_cfg: dict) -> bool:
    """Does state.auth_data already contain a matching s3 entry with creds?"""
    if state.auth_mode != "existing":
        return False
    for e in state.auth_data.get("storage", []):
        if (e.get("type", "s3") == storage_cfg.get("type", "s3")
                and e.get("bucket") == storage_cfg.get("bucket")
                and e.get("endpoint_url") == storage_cfg.get("endpoint_url")
                and e.get("aws_access_key_id")
                and e.get("aws_secret_access_key")):
            return True
    return False


def _show_storage(s: dict) -> None:
    questionary.print("--- storage ---", style="bold")
    for k in ("type", "mode", "title", "bucket", "endpoint_url", "bucket_prefix",
              "presigned_urls", "presigned_urls_expiry", "import_method",
              "file_name_filter", "scan_all_subfolders",
              "aws_access_key_id", "aws_secret_access_key"):
        if k in s:
            v = s[k]
            if k in ("aws_access_key_id", "aws_secret_access_key") and v:
                v = v[:4] + "…" + v[-2:] if len(v) > 6 else "***"
            _print_kv(k, v)


# --- Storage loop ----------------------------------------------------------

def step_storage_loop(state: State) -> None:
    describe(state, "storage")
    while True:
        more = ask_yn(
            ("Add another storage?" if state.storages else "Add storage?"),
            default=not state.storages,
        )
        if not more:
            return
        s = _add_one_storage(state)
        if s is None:
            continue
        state.storages.append(s)


def _add_one_storage(state: State) -> Optional[dict]:
    """Run one storage-entry sub-flow. Returns the dict or None on cancel."""
    load = ask_yn("Load from other config / labelstudio?", default=False)
    s: Optional[dict] = None
    if load:
        with Spinner("loading storage options"):
            opts = _dedupe_storage_options(collect_other_storages(state))
            ls_opts = (_dedupe_storage_options(collect_ls_storages(state))
                       if state.token_works else [])
        all_opts = opts + ls_opts
        if not all_opts:
            questionary.print("(no other storage entries available)", style="italic fg:#888888")
        else:
            CANCEL = "[cancel]"
            choices = _format_storage_choices(state, all_opts) + [CANCEL]
            pick = ask_select("Pick a storage to preload:", choices=choices)
            if pick == CANCEL:
                return None
            s = dict(STORAGE_DEFAULTS)
            s.update(all_opts[pick]["data"])
            # Warn if no creds available (neither inline in source nor in selected auth).
            if not _storage_auth_available(state, s):
                questionary.print(
                    "WARNING: this storage has no credentials in the source config "
                    "or the selected auth file.", style="fg:#ff8800")
                if not ask_yn("Continue with this selection?", default=True):
                    return None
    if s is None:
        s = prompt_storage(state)
        if s is None:
            return None

    while True:
        _show_storage(s)
        action = ask_select(
            "Action:",
            choices=["accept", "validate (head_bucket)", "edit", "cancel"],
        )
        if action == "accept":
            return _split_storage_secrets(state, s)
        if action == "cancel":
            return None
        if action.startswith("validate"):
            if state.auth_mode == "defer":
                questionary.print("(validate not possible — auth deferred)",
                                  style="italic fg:#888888")
            else:
                with Spinner(f"head_bucket {s.get('bucket','?')}"):
                    ok, msg = validate_storage(s)
                if ok:
                    questionary.print(f"✓ {msg}", style="fg:#00aa00")
                else:
                    questionary.print(f"✗ {msg}  (that's too bad — continue)",
                                      style="fg:#cc4400")
            continue
        if action == "edit":
            s_new = prompt_storage(state, preload=s)
            if s_new is not None:
                s = s_new
            continue


def _split_storage_secrets(state: State, s: dict) -> dict:
    """Move S3 secrets out of `s` per auth_mode; return the dict to put in project config."""
    ak = s.pop("aws_access_key_id", None)
    sk = s.pop("aws_secret_access_key", None)
    if ak is None and sk is None:
        return s  # no secrets to handle (defer / pre-existing in auth)
    if state.auth_mode == "new":
        entry = {"type": s.get("type", "s3"),
                 "bucket": s.get("bucket", ""),
                 "endpoint_url": s.get("endpoint_url", "")}
        if ak: entry["aws_access_key_id"] = ak
        if sk: entry["aws_secret_access_key"] = sk
        # Replace existing matching entry if present.
        same = [i for i, e in enumerate(state.auth_data["storage"])
                if (e.get("bucket") == entry["bucket"]
                    and e.get("endpoint_url") == entry["endpoint_url"])]
        if same:
            state.auth_data["storage"][same[0]] = entry
        else:
            state.auth_data["storage"].append(entry)
        return s
    # inline OR existing-unmatched OR defer-but-entered — write back inline.
    if ak: s["aws_access_key_id"] = ak
    if sk: s["aws_secret_access_key"] = sk
    return s


# --- ML backend: defaults + sources ----------------------------------------

ML_DEFAULTS = {
    "interactive": True,
    "extra_params": {},
    "start_training_on_annotation_update": False,
    "annotation_prelabeling": False,
}


def collect_other_ml(state: State) -> list[dict]:
    if state._cache_other_ml is not None:
        return state._cache_other_ml
    out = []
    for path in list_toml_files(state.config_dir):
        if state.outfile and path.resolve() == state.outfile.resolve():
            continue
        try:
            with open(path, "rb") as f:
                data = tomllib.load(f)
        except Exception:
            continue
        ml = data.get("ml_backend")
        if isinstance(ml, dict):
            ml = dict(ml)
            if _is_complete_ml_option(ml):
                out.append({"data": ml, "source": path.name})
        elif isinstance(ml, list):
            for entry in ml:
                entry = dict(entry)
                if _is_complete_ml_option(entry):
                    out.append({"data": entry, "source": path.name})
    state._cache_other_ml = out
    return out


def collect_ls_ml(state: State) -> list[dict]:
    if state._cache_ls_ml is not None:
        return state._cache_ls_ml
    if not state.token_works:
        state._cache_ls_ml = []
        return []
    out = []
    try:
        client = _ls_client(state)
        for proj in client.projects.list():
            for m in client.ml.list(project=proj.id) or []:
                d = {
                    "name": getattr(m, "title", None) or "",
                    "backend_url": getattr(m, "url", None) or "",
                    "interactive": bool(getattr(m, "is_interactive", False)),
                    "extra_params": _parse_extra_params(getattr(m, "extra_params", None)),
                }
                out.append({"data": d, "source": f"LS:{proj.title}"})
    except Exception as e:
        questionary.print(f"(could not list LS ml backends: {e})", style="italic fg:#888888")
    state._cache_ls_ml = out
    return out


def _parse_extra_params(v) -> dict:
    if not v:
        return {}
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        try:
            import json
            parsed = json.loads(v)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _ml_key(d: dict) -> tuple:
    return (d.get("backend_url", ""), d.get("name", ""))


def _is_complete_ml_option(d: dict) -> bool:
    return bool(d.get("backend_url"))


def _dedupe_ml_options(items: list[dict]) -> list[dict]:
    by_key: dict[tuple, dict] = {}
    for it in items:
        k = _ml_key(it["data"])
        if k in by_key:
            by_key[k]["sources"].append(it["source"])
        else:
            by_key[k] = {"data": it["data"], "sources": [it["source"]]}
    return list(by_key.values())


def _ml_auth_available(state: State, ml_cfg: dict) -> bool:
    if ml_cfg.get("user") and ml_cfg.get("pass"):
        return True
    name = ml_cfg.get("name")
    backend_url = ml_cfg.get("backend_url")
    for e in state.auth_data.get("ml_backend", []):
        if (name and e.get("name") == name) or (
                backend_url and e.get("backend_url") == backend_url):
            return bool(e.get("user") and e.get("pass"))
    return False


def _format_ml_choices(state: State, items: list[dict]) -> list:
    auth_marker = "__OK__"
    rows = []
    for idx, item in enumerate(items):
        d = item["data"]
        rows.append({
            "idx": idx,
            "name": d.get("name") or "(unnamed)",
            "backend_url": d.get("backend_url", ""),
            "auth": auth_marker if _ml_auth_available(state, d) else "",
            "source": ", ".join(item["sources"]),
        })
    table = tabulate(
        [[r["name"], r["backend_url"], r["auth"], r["source"]]
         for r in rows],
        headers=["Name", "Backend URL", "Auth", "Source"],
        tablefmt="plain",
    )
    lines = table.splitlines()
    if not lines:
        return []
    choices = [
        questionary.Separator(lines[0]),
        questionary.Separator("-" * len(lines[0])),
    ]
    for row, line in zip(rows, lines[1:]):
        if row["auth"]:
            before, _, after = line.partition(row["auth"])
            title = [
                ("", before),
                ("fg:#00aa00 bold", "✓"),
                ("", " " * (len(auth_marker) - 1)),
                ("", after),
            ]
        else:
            title = line
        choices.append(questionary.Choice(title=title, value=row["idx"]))
    return choices


def prompt_ml_backend(state: State, preload: Optional[dict] = None) -> Optional[dict]:
    p = preload or {}
    m = dict(ML_DEFAULTS)
    m.update(p)
    m["backend_url"] = ask_text("backend_url:", default=m.get("backend_url", ""))
    m["name"] = ask_text("name:", default=m.get("name", ""))
    m["interactive"] = ask_yn("interactive?", default=bool(m.get("interactive", True)))
    m["start_training_on_annotation_update"] = ask_yn(
        "start_training_on_annotation_update?",
        default=bool(m.get("start_training_on_annotation_update", False)))
    m["annotation_prelabeling"] = ask_yn(
        "annotation_prelabeling?",
        default=bool(m.get("annotation_prelabeling", False)))
    while True:
        ep = m.get("extra_params") or {}
        if isinstance(ep, dict):
            import json
            default_str = json.dumps(ep) if ep else "{}"
        else:
            default_str = str(ep)
        raw = ask_text("extra_params (JSON):", default=default_str)
        try:
            import json
            parsed = json.loads(raw) if raw.strip() else {}
            if not isinstance(parsed, dict):
                raise ValueError("must be a JSON object")
            m["extra_params"] = parsed
            break
        except Exception as e:
            questionary.print(f"Invalid JSON: {e}", style="fg:#cc4400")
    return m


def _show_ml(m: dict) -> None:
    questionary.print("--- ml_backend ---", style="bold")
    for k in ("name", "backend_url", "interactive", "extra_params",
              "start_training_on_annotation_update", "annotation_prelabeling"):
        if k in m:
            _print_kv(k, m[k])


def step_ml_loop(state: State) -> None:
    describe(state, "ml_backend")
    if not ask_yn("Add ml-backend?", default=True):
        return
    m = _add_one_ml(state)
    if m is not None:
        state.ml_backends = [m]


def _add_one_ml(state: State) -> Optional[dict]:
    load = ask_yn("Load from other config / labelstudio?", default=True)
    m: Optional[dict] = None
    if load:
        with Spinner("loading ml-backend options"):
            opts = _dedupe_ml_options(collect_other_ml(state))
            ls_opts = (_dedupe_ml_options(collect_ls_ml(state))
                       if state.token_works else [])
        all_opts = opts + ls_opts
        if not all_opts:
            questionary.print("(no other ml backends available)", style="italic fg:#888888")
        else:
            CANCEL = "[cancel]"
            choices = _format_ml_choices(state, all_opts) + [CANCEL]
            pick = ask_select("Pick an ml-backend to preload:", choices=choices)
            if pick == CANCEL:
                return None
            m = dict(ML_DEFAULTS)
            m.update(all_opts[pick]["data"])
    if m is None:
        m = prompt_ml_backend(state)
        if m is None:
            return None

    while True:
        _show_ml(m)
        action = ask_select(
            "Action:",
            choices=["accept", "validate (GET backend_url)", "edit", "cancel"],
        )
        if action == "accept":
            return m
        if action == "cancel":
            return None
        if action.startswith("validate"):
            if state.auth_mode == "defer":
                questionary.print("(validate not possible — auth deferred)",
                                  style="italic fg:#888888")
            else:
                with Spinner(f"GET {m.get('backend_url','?')}"):
                    ok, msg = validate_ml_backend(m["backend_url"])
                if ok:
                    questionary.print(f"✓ {msg}", style="fg:#00aa00")
                else:
                    questionary.print(f"✗ {msg}  (that's too bad — continue)",
                                      style="fg:#cc4400")
            continue
        if action == "edit":
            m_new = prompt_ml_backend(state, preload=m)
            if m_new is not None:
                m = m_new
            continue


# --- Annotations -----------------------------------------------------------

def step_annotations(state: State) -> None:
    describe(state, "annotations")
    state.annotations["instructions"] = ask_text("annotation instructions text:", default="")
    state.annotations["show_before_labeling"] = ask_yn(
        "annotations: show_before_labeling?", default=False)


# --- Write outputs ---------------------------------------------------------

def write_project_config(state: State) -> None:
    b = TomlBuilder()
    if state.auth_mode == "inline":
        b.header_comment(
            "WARNING: this file contains inline secrets — keep out of version control.")
    b.kv("host", state.host or "")
    b.kv("project", state.project_name or "")
    b.kv("label_config", state.label_config or "")
    if state.auth_mode == "existing":
        b.kv("auth", auth_path_for_project(state.auth_path, state.outfile.parent))
    elif state.auth_mode == "new":
        b.kv("auth", auth_path_for_project(state.auth_path, state.outfile.parent))
    elif state.auth_mode == "defer":
        b.kv("auth", "")
    # inline: omit the field

    # Inline LS token if applicable
    if state.auth_mode == "inline" and state.token:
        b.kv("token", state.token)

    b.section("labelstudio-tools")
    b.kv("pk", state.pk)
    b.kv("cache", state.cache)

    b.section("general")
    b.kv("description", state.description)
    b.kv("task_sampling", state.sampling)
    b.kv("color", state.color, comment=state.color_comment)

    for s in state.storages:
        b.array_section("storage")
        # Stable order for readability; secrets last.
        order = ["type", "mode", "title", "bucket", "endpoint_url", "bucket_prefix",
                 "presigned_urls", "presigned_urls_expiry", "import_method",
                 "file_name_filter", "scan_all_subfolders",
                 "aws_access_key_id", "aws_secret_access_key"]
        seen = set()
        for k in order:
            if k in s:
                b.kv(k, s[k])
                seen.add(k)
        for k, v in s.items():
            if k not in seen:
                b.kv(k, v)

    for m in state.ml_backends[:1]:
        b.section("ml_backend")
        order = ["name", "backend_url", "interactive", "extra_params",
                 "start_training_on_annotation_update", "annotation_prelabeling"]
        seen = set()
        for k in order:
            if k in m:
                b.kv(k, m[k])
                seen.add(k)
        for k, v in m.items():
            if k not in seen:
                b.kv(k, v)

    b.section("annotations")
    b.kv("instructions", state.annotations.get("instructions", ""))
    b.kv("show_before_labeling", bool(state.annotations.get("show_before_labeling", False)))

    out = b.render()
    if state.outfile.exists():
        if not ask_yn(f"Output file exists: {state.outfile}\n  Overwrite?",
                      default=False):
            questionary.print("Aborted.", style="fg:#cc4400")
            sys.exit(1)
    state.outfile.parent.mkdir(parents=True, exist_ok=True)
    state.outfile.write_text(out)
    questionary.print(f"\n✓ wrote {state.outfile}", style="fg:#00aa00 bold")


def write_new_auth_file(state: State) -> None:
    if state.auth_mode != "new" or state.auth_path is None:
        return
    b = TomlBuilder()
    b.header_comment("WARNING: this file contains secrets — keep out of version control.")
    for entry in state.auth_data.get("labelstudio", []):
        b.array_section("labelstudio")
        for k in ("host", "token"):
            if k in entry:
                b.kv(k, entry[k])
    for entry in state.auth_data.get("storage", []):
        b.array_section("storage")
        for k in ("type", "bucket", "endpoint_url",
                  "aws_access_key_id", "aws_secret_access_key"):
            if k in entry:
                b.kv(k, entry[k])
    for entry in state.auth_data.get("ml_backend", []):
        b.array_section("ml_backend")
        for k in ("name", "backend_url", "user", "pass"):
            if k in entry:
                b.kv(k, entry[k])
    out = b.render()
    if state.auth_path.exists():
        if not ask_yn(f"Auth file exists: {state.auth_path}\n  Overwrite?",
                      default=False):
            questionary.print("Auth file write aborted.", style="fg:#cc4400")
            return
    state.auth_path.parent.mkdir(parents=True, exist_ok=True)
    state.auth_path.write_text(out)
    questionary.print(f"✓ wrote {state.auth_path}", style="fg:#00aa00 bold")


# --- --default mode -------------------------------------------------------

def _maybe_comment(verbose: bool, key: str) -> Optional[str]:
    return FIELD_COMMENTS.get(key) if verbose else None


def write_default_project(path: Path, auth_rel: str, verbose: bool) -> None:
    b = TomlBuilder()
    b.header_comment(
        "Stub Label Studio project config.\n"
        "Strictly required: host, label_config, and a valid token (in the auth\n"
        "file or inline). Anything else can be removed if not needed —\n"
        "including the entire [[storage]], [ml_backend], and [annotations]\n"
        "sections.")
    b.kv("host", "", _maybe_comment(verbose, "host"))
    b.kv("project", "", _maybe_comment(verbose, "project"))
    b.kv("label_config", "", _maybe_comment(verbose, "label_config"))
    b.kv("auth", auth_rel, _maybe_comment(verbose, "auth"))

    b.section("labelstudio-tools")
    b.kv("pk", "image", _maybe_comment(verbose, "pk"))
    b.kv("cache", "RAM", _maybe_comment(verbose, "cache"))

    b.section("general")
    b.kv("description", "", _maybe_comment(verbose, "description"))
    b.kv("task_sampling", "sequential", _maybe_comment(verbose, "task_sampling"))
    b.kv("color", "#FF4C25", "Red" if not verbose else f"Red — {FIELD_COMMENTS['color']}")

    b.array_section("storage")
    b.kv("type", "s3", _maybe_comment(verbose, "type"))
    b.kv("mode", "source", _maybe_comment(verbose, "mode"))
    b.kv("title", "", _maybe_comment(verbose, "title"))
    b.kv("bucket", "", _maybe_comment(verbose, "bucket"))
    b.kv("endpoint_url", "", _maybe_comment(verbose, "endpoint_url"))
    b.kv("bucket_prefix", "", _maybe_comment(verbose, "bucket_prefix"))
    b.kv("presigned_urls", True, _maybe_comment(verbose, "presigned_urls"))
    b.kv("presigned_urls_expiry", 15, _maybe_comment(verbose, "presigned_urls_expiry"))
    b.kv("import_method", "tasks", _maybe_comment(verbose, "import_method"))
    b.kv("file_name_filter", "(?!)", _maybe_comment(verbose, "file_name_filter"))
    b.kv("scan_all_subfolders", True, _maybe_comment(verbose, "scan_all_subfolders"))

    b.section("ml_backend")
    b.kv("name", "", _maybe_comment(verbose, "name"))
    b.kv("backend_url", "", _maybe_comment(verbose, "backend_url"))
    b.kv("interactive", True, _maybe_comment(verbose, "interactive"))
    b.kv("extra_params", {}, _maybe_comment(verbose, "extra_params"))
    b.kv("start_training_on_annotation_update", False,
         _maybe_comment(verbose, "start_training_on_annotation_update"))
    b.kv("annotation_prelabeling", False,
         _maybe_comment(verbose, "annotation_prelabeling"))

    b.section("annotations")
    b.kv("instructions", "", _maybe_comment(verbose, "instructions"))
    b.kv("show_before_labeling", False, _maybe_comment(verbose, "show_before_labeling"))

    path.write_text(b.render())


def write_default_auth(path: Path, verbose: bool) -> None:
    b = TomlBuilder()
    b.header_comment(
        "Auth file — KEEP OUT OF VERSION CONTROL.\n"
        "Required: a [[labelstudio]] entry with host + token matching the\n"
        "project config's host. [[storage]] and [[ml_backend]] entries are\n"
        "optional and can be removed.")
    b.array_section("labelstudio")
    b.kv("host", "", _maybe_comment(verbose, "host"))
    b.kv("token", "", _maybe_comment(verbose, "token"))

    b.array_section("storage")
    b.kv("type", "s3", _maybe_comment(verbose, "type"))
    b.kv("bucket", "", _maybe_comment(verbose, "bucket"))
    b.kv("endpoint_url", "", _maybe_comment(verbose, "endpoint_url"))
    b.kv("aws_access_key_id", "", _maybe_comment(verbose, "aws_access_key_id"))
    b.kv("aws_secret_access_key", "", _maybe_comment(verbose, "aws_secret_access_key"))

    b.array_section("ml_backend")
    b.kv("name", "", _maybe_comment(verbose, "name"))
    b.kv("backend_url", "", _maybe_comment(verbose, "backend_url"))
    b.comment('user = ""'
              + (f"  # {FIELD_COMMENTS['user']}" if verbose else ""))
    b.comment('pass = ""'
              + (f"  # {FIELD_COMMENTS['pass']}" if verbose else ""))

    path.write_text(b.render())


def run_default_mode(args: argparse.Namespace) -> None:
    config_dir = Path(args.dir).resolve() if args.dir else Path("./configs").resolve()
    config_dir.mkdir(parents=True, exist_ok=True)

    proj_name = args.outfile or "ls_project.toml"
    auth_name = args.auth or "ls_auth.toml"
    proj_path = _resolve_outfile(proj_name, config_dir)
    auth_path = _resolve_outfile(auth_name, config_dir)

    for p in (proj_path, auth_path):
        if p.exists():
            print(f"ERROR: {p} already exists; refusing to overwrite.", file=sys.stderr)
            sys.exit(1)

    auth_rel = auth_path_for_project(auth_path, proj_path.parent)
    write_default_project(proj_path, auth_rel, verbose=args.verbose)
    write_default_auth(auth_path, verbose=args.verbose)
    print(f"Wrote {proj_path}")
    print(f"Wrote {auth_path}")


# --- Main ------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="labelstudio-tools config-wizard",
                                description="Interactive wizard for a new LS project config (TOML).")
    p.add_argument("-v", "--verbose", action="store_true",
                   help="Show field descriptions (skips the y/n prompt; "
                        "with --default, adds inline field-description comments).")
    p.add_argument("--dir", help="Pre-select config directory (skips prompt). "
                                  "Default for --default: ./configs/.")
    p.add_argument("--auth", help="Pre-select auth file (skips prompt). "
                                   "With --default: overrides default auth filename.")
    p.add_argument("-o", "--outfile",
                   help="Pre-set output filename. Bare name → goes in --dir; "
                        "with path → cwd-relative.")
    p.add_argument("--default", action="store_true",
                   help="Skip the wizard; write a stub project config (ls_project.toml) "
                        "and auth file (ls_auth.toml) into --dir.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.default:
        run_default_mode(args)
        return
    state = State(args=args)
    try:
        step_descriptions(state)
        step_config_dir(state)
        step_host(state)
        step_auth_file(state)
        step_token(state)
        step_project_name(state)
        step_shortname(state)
        step_outfile(state)
        step_label_config(state)
        step_general(state)
        step_lstools(state)
        step_storage_loop(state)
        step_ml_loop(state)
        step_annotations(state)
        write_project_config(state)
        write_new_auth_file(state)
    except KeyboardInterrupt:
        questionary.print("\nAborted.", style="fg:#cc4400")
        sys.exit(130)


if __name__ == "__main__":
    main()
