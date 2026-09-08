"""Declarative, spec-driven REST cloud connector.

A provider whose API is "JSON over REST with conventional auth" can be added by
writing a spec (JSON) instead of Python.  ``DeclarativeRestConnector`` reads that
spec and issues templated HTTP requests, mapping the responses onto the two
shapes the rest of Cloud Offload understands: the offer dict and the ``Instance``
dataclass.

Spec shape
----------

::

    {
      "spec_version": 1,
      "name": "acme",
      "aliases": ["acme-gpu"],
      "display_name": "Acme GPU",
      "base_url": "https://api.acme.dev/v1",
      "base_url_config_field": "acme_api_url",
      "auth": {"type": "bearer"},
      "headers": {"User-Agent": "cloud-offload/0.1"},
      "timeout": 30,
      "client_filter": true,
      "include_raw": true,
      "status_map": {"provisioning": "pending", "active": "running"},
      "default_status": "unknown",
      "endpoints": {
        "offers":    {"method": "GET",  "path": "offers", "items": "$.data",
                      "map": {"id": "$.id", "gpu_ram_gb": {"path": "$.vram_mb",
                                                           "unit": "MB->GB"}}},
        "launch":    {"method": "POST", "path": "instances",
                      "body": {"offer": "{{offer_id}}", "image": "{{docker_image}}"},
                      "map": {"id": "$.instance.id"},
                      "wait_for": {"status": "running", "timeout_seconds": 300,
                                   "interval_seconds": 5}},
        "get":       {"method": "GET",  "path": "instances", "items": "$.data",
                      "select": {"where": "$.id", "equals": "{{instance_id}}"},
                      "map": {...}},
        "list":      {"method": "GET",  "path": "instances", "items": "$.data",
                      "select": {"where": "$.state", "in": ["active", "booting"]},
                      "map": {...}},
        "terminate": {"method": "DELETE", "path": "instances/{{instance_id}}"},
        "balance":   {"method": "GET",  "path": "account",
                      "map": {"balance": {"path": "$.credit", "type": "float"}}}
      }
    }

The operation keys may also sit at the top level (as in the original design
sketch); ``endpoints`` is preferred because it keeps operations from colliding
with metadata keys.

Template variables
~~~~~~~~~~~~~~~~~~

``{{offer_id}}``, ``{{instance_id}}``, ``{{docker_image}}``, ``{{env_vars}}``,
``{{startup_script}}``, ``{{gpu_type}}``, ``{{min_gpu_ram}}``,
``{{max_hourly_rate}}`` and ``{{provider}}`` may appear in ``path``, ``query``
and ``body``.  A string that is *exactly* one placeholder keeps the value's
type; a placeholder embedded in a longer string is stringified.  The credential
is never a template variable — it is supplied by ``auth`` alone, so a spec file
can be shared without leaking secrets.

Three directives shape structured request values:

``{"$json": {...}}``
    Render the inner value, then JSON-encode it.  This is how Vast.ai's filter
    DSL is passed as ``?q=<json>``.
``{"$when": "min_gpu_ram", ...}``
    Drop the whole containing object unless that variable is truthy.
``{"$value": "min_gpu_ram", "scale": 1024, "type": "int", "omit_if": "empty"}``
    Emit a typed (non-string) value from a variable, optionally scaled.
    ``omit_if`` is ``null`` (default), ``empty`` (any falsy value) or ``never``.

Response mapping
~~~~~~~~~~~~~~~~

Paths are a small JSONPath subset implemented here — no new dependencies:
``$.data.items``, ``$.gpu.vram_gb``, ``$.offers[0].id``, or plain ``data.items``.
A map entry is either a path string or an object::

    {"path": "$.gpu_ram", "scale": 0.0009765625, "type": "float",
     "default": 0, "required": false}
    {"path": "$.gpu_ram", "unit": "MB->GB"}
    {"const": "on-demand"}

``unit`` is sugar for a known ``scale`` (see ``UNIT_SCALES``).  A missing value
(or JSON ``null``) falls back to ``default``; with no default the key is still
present and set to ``None``, so mapped shapes stay stable.  ``required`` turns a
missing value into a clear error instead of silent ``None``.

Two collection primitives cover the awkward real-world cases:

``select``
    ``{"where": "$.id", "equals": "{{instance_id}}"}`` picks one entry out of a
    listing when the provider has no fetch-by-id route (Vast.ai does not).  The
    same primitive filters ``list`` results with ``in`` / ``not_in`` / ``equals``.
``wait_for``
    ``{"status": "running", "timeout_seconds": 300, "interval_seconds": 5}`` on
    the ``launch`` endpoint polls ``get`` until the instance reaches a normalized
    status, or raises ``TimeoutError``.  Providers that report "provisioning"
    before "running" need this.

Errors are loud by default: a failed request, a non-JSON body or a missing
required field raises rather than mapping to silent ``None``.  A read endpoint
may opt into degrading instead — ``"on_error": "empty"`` on ``offers``/``list``
and ``"on_error": "null"`` on ``get`` return an empty result and log a warning,
which is what most hand-written connectors do so that one unreachable provider
does not abort routing across all of them.

Limits (read this before writing a spec)
----------------------------------------

This connector deliberately covers only the common case:

* **JSON over REST only.**  No GraphQL — that is why RunPod has a coded
  connector, and it always will.
* **No multi-step provisioning.**  One request per operation, plus the optional
  ``wait_for`` poll loop.  Providers that need "reserve, then configure, then
  start" need a coded connector.
* **No custom request signing.**  Bearer / custom header / query parameter /
  HTTP Basic only.  AWS SigV4 and friends need a coded connector.
* **No pagination**, no retry/backoff policy, no websocket or streaming APIs.
* **No response post-processing** beyond scale/type/default — arbitrary
  transformations need code.

When a provider does not fit, write a ``CloudConnector`` subclass and register
it; the declarative path is a convenience, not a replacement.
"""

from __future__ import annotations

import base64
import copy
import json
import logging
import re
import time
from functools import lru_cache
from pathlib import Path
from typing import Any

from cloud_offload import config as _config
from cloud_offload.providers.base import CloudConnector, Instance, PlacementConstraints

logger = logging.getLogger(__name__)

__all__ = [
    "DeclarativeRestConnector",
    "DeclarativeRequestError",
    "DeclarativeSpecError",
    "builtin_provider_spec",
    "builtin_provider_specs",
    "credential_like_keys",
    "describe_spec_files",
    "dry_run_spec",
    "load_provider_specs",
    "register_declarative_providers",
    "shadow_conflict",
    "spec_directory",
    "spec_file_path",
    "validate_spec",
]

#: Operations a spec may define.  ``offers`` is the only mandatory one.
OPERATIONS = ("offers", "launch", "get", "list", "terminate", "balance")

#: Normalized instance states the rest of Cloud Offload understands.
NORMALIZED_STATUSES = ("pending", "running", "stopped", "terminated", "unknown")

AUTH_TYPES = ("bearer", "header", "query", "basic", "none")

HTTP_METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD")

#: Template variables a spec may reference.  The API credential is intentionally
#: absent: it is injected by ``auth`` and never templated into a spec.
TEMPLATE_VARIABLES = (
    "offer_id",
    "instance_id",
    "docker_image",
    "env_vars",
    "startup_script",
    "gpu_type",
    "min_gpu_ram",
    "max_hourly_rate",
    "disk_gb",
    "provider",
)

#: ``unit`` sugar for common conversions.  Values are exact where possible so
#: that, e.g., MB->GB matches a hand-written ``value / 1024``.
UNIT_SCALES = {
    "mb->gb": 1.0 / 1024,
    "gb->mb": 1024,
    "kb->gb": 1.0 / (1024 * 1024),
    "gb->kb": 1024 * 1024,
    "mib->gib": 1.0 / 1024,
    "gib->mib": 1024,
    "bytes->gb": 1.0 / (1024 * 1024 * 1024),
    "gb->bytes": 1024 * 1024 * 1024,
    "cents->usd": 0.01,
    "usd->cents": 100,
    "per_minute->per_hour": 60,
    "per_second->per_hour": 3600,
    "per_month->per_hour": 1.0 / 730,
}

VALUE_TYPES = ("str", "int", "float", "bool")

#: What a read endpoint does when the provider errors.  ``raise`` (the default)
#: surfaces a clear exception; ``empty``/``null`` degrade to an empty result and
#: a logged warning, which is how several hand-written connectors behave.
ERROR_POLICIES = {
    "offers": ("raise", "empty"),
    "list": ("raise", "empty"),
    "get": ("raise", "null"),
}

_FIELD_SPEC_KEYS = {"path", "const", "scale", "unit", "type", "default", "required"}

_PLACEHOLDER = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")
_INDEX = re.compile(r"-?\d+")
_SEGMENT = re.compile(r"[^.\[\]]+")

_MISSING = object()
_OMIT = object()


class DeclarativeSpecError(ValueError):
    """The provider spec itself is wrong (bad syntax, missing section, ...)."""


class DeclarativeRequestError(RuntimeError):
    """A request failed, or a response did not match what the spec promised."""


# ---------------------------------------------------------------------------
# Path accessor (a deliberately small JSONPath subset)
# ---------------------------------------------------------------------------


def parse_path(path: str) -> list:
    """Parse ``$.a.b[0].c`` into ``['a', 'b', 0, 'c']``.

    Raises ``DeclarativeSpecError`` on malformed syntax so a bad spec fails
    loudly at validation time rather than silently mapping to ``None``.
    """
    if not isinstance(path, str):
        raise DeclarativeSpecError(f"path must be a string, got {type(path).__name__}")
    body = path.strip()
    if not body:
        raise DeclarativeSpecError("path is empty")
    if body.startswith("$"):
        body = body[1:]
    if body in ("", "."):
        return []

    tokens: list = []
    position = 0
    expect_separator = False
    length = len(body)
    while position < length:
        character = body[position]
        if character == ".":
            if position + 1 >= length or body[position + 1] in ".[":
                raise DeclarativeSpecError(f"empty path segment in {path!r}")
            position += 1
            expect_separator = False
            continue
        if character == "]":
            raise DeclarativeSpecError(f"unbalanced ']' in {path!r}")
        if character == "[":
            end = body.find("]", position)
            if end == -1:
                raise DeclarativeSpecError(f"unclosed '[' in {path!r}")
            index = body[position + 1 : end]
            if not _INDEX.fullmatch(index):
                raise DeclarativeSpecError(f"invalid list index '{index}' in {path!r}")
            tokens.append(int(index))
            position = end + 1
            expect_separator = False
            continue
        if expect_separator:
            raise DeclarativeSpecError(f"missing '.' between segments in {path!r}")
        match = _SEGMENT.match(body, position)
        tokens.append(match.group(0))
        position = match.end()
        expect_separator = True

    if not tokens:
        raise DeclarativeSpecError(f"path {path!r} selects nothing")
    return tokens


def resolve_path(data: Any, path: str, default: Any = None) -> Any:
    """Resolve ``path`` against ``data``, returning ``default`` when absent."""
    value = data
    for token in parse_path(path):
        if isinstance(token, int):
            if not isinstance(value, (list, tuple)):
                return default
            try:
                value = value[token]
            except IndexError:
                return default
        else:
            if not isinstance(value, dict) or token not in value:
                return default
            value = value[token]
    return value


# ---------------------------------------------------------------------------
# Request templating
# ---------------------------------------------------------------------------


def _coerce(value: Any, type_name: str | None, *, where: str) -> Any:
    if type_name is None or value is None:
        return value
    try:
        if type_name == "str":
            return str(value)
        if type_name == "int":
            return int(value)
        if type_name == "float":
            return float(value)
        if type_name == "bool":
            if isinstance(value, str):
                return value.strip().lower() in {"1", "true", "yes", "on"}
            return bool(value)
    except (TypeError, ValueError) as exc:
        raise DeclarativeRequestError(
            f"{where}: cannot convert {value!r} to {type_name}: {exc}"
        ) from exc
    raise DeclarativeSpecError(f"{where}: unknown type {type_name!r}")


def _scale_of(spec: dict, *, where: str) -> float | int | None:
    unit = spec.get("unit")
    scale = spec.get("scale")
    if unit is not None:
        if not isinstance(unit, str) or unit.strip().lower() not in UNIT_SCALES:
            raise DeclarativeSpecError(f"{where}: unknown unit {unit!r}")
        unit_scale = UNIT_SCALES[unit.strip().lower()]
        scale = unit_scale if scale is None else unit_scale * scale
    if scale is None:
        return None
    if isinstance(scale, bool) or not isinstance(scale, (int, float)):
        raise DeclarativeSpecError(f"{where}: scale must be a number, got {scale!r}")
    return scale


def _variable(name: str) -> str:
    """Accept both ``min_gpu_ram`` and ``{{min_gpu_ram}}`` in directives."""
    if not isinstance(name, str):
        raise DeclarativeSpecError(f"variable name must be a string, got {name!r}")
    stripped = name.strip()
    match = _PLACEHOLDER.fullmatch(stripped)
    return match.group(1) if match else stripped


def render_template(value: Any, variables: dict, *, where: str = "template") -> Any:
    """Render a spec fragment against the current call's variables.

    Returns ``_OMIT`` when a directive asks for the fragment to be dropped;
    callers inside containers filter that out.
    """
    if isinstance(value, str):
        match = _PLACEHOLDER.fullmatch(value.strip())
        if match:
            return _lookup(match.group(1), variables, where=where)

        def substitute(hit):
            resolved = _lookup(hit.group(1), variables, where=where)
            if resolved is None:
                return ""
            if isinstance(resolved, (dict, list)):
                return json.dumps(resolved, sort_keys=True)
            return str(resolved)

        return _PLACEHOLDER.sub(substitute, value)

    if isinstance(value, dict):
        if "$value" in value:
            return _render_value_directive(value, variables, where=where)
        if "$json" in value:
            rendered = render_template(value["$json"], variables, where=f"{where}.$json")
            if rendered is _OMIT:
                return _OMIT
            return json.dumps(rendered)
        guard = value.get("$when")
        if guard is not None:
            names = [guard] if isinstance(guard, str) else guard
            if not isinstance(names, list):
                raise DeclarativeSpecError(
                    f"{where}: $when must be a variable name or list of names"
                )
            for name in names:
                if not _lookup(_variable(name), variables, where=where):
                    return _OMIT
        rendered_dict = {}
        for key, item in value.items():
            if key == "$when":
                continue
            child = render_template(item, variables, where=f"{where}.{key}")
            if child is not _OMIT:
                rendered_dict[key] = child
        return rendered_dict

    if isinstance(value, list):
        rendered_list = []
        for index, item in enumerate(value):
            child = render_template(item, variables, where=f"{where}[{index}]")
            if child is not _OMIT:
                rendered_list.append(child)
        return rendered_list

    return value


def _lookup(name: str, variables: dict, *, where: str) -> Any:
    if name not in variables:
        raise DeclarativeSpecError(
            f"{where}: unknown template variable {{{{{name}}}}}; "
            f"known variables: {', '.join(TEMPLATE_VARIABLES)}"
        )
    return variables[name]


def _render_value_directive(directive: dict, variables: dict, *, where: str) -> Any:
    name = _variable(directive["$value"])
    value = _lookup(name, variables, where=where)
    omit_if = str(directive.get("omit_if", "null")).lower()
    if omit_if not in {"null", "empty", "never"}:
        raise DeclarativeSpecError(
            f"{where}: omit_if must be null, empty or never (got {omit_if!r})"
        )
    if (value is None and omit_if in {"null", "empty"}) or (
        omit_if == "empty" and not value
    ):
        return directive.get("default", _OMIT)

    scale = _scale_of(directive, where=where)
    if scale is not None and value is not None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise DeclarativeRequestError(
                f"{where}: cannot scale non-numeric value {value!r}"
            )
        value = value * scale
    return _coerce(value, directive.get("type"), where=where)


# ---------------------------------------------------------------------------
# Response mapping
# ---------------------------------------------------------------------------


def _is_field_spec(value: Any) -> bool:
    return isinstance(value, dict) and bool(_FIELD_SPEC_KEYS & set(value))


def map_field(item: Any, field_spec: Any, *, where: str) -> Any:
    """Map one response field according to a map entry."""
    if isinstance(field_spec, str):
        field_spec = {"path": field_spec}
    if not isinstance(field_spec, dict):
        raise DeclarativeSpecError(
            f"{where}: map entry must be a path string or object, got "
            f"{type(field_spec).__name__}"
        )
    if "const" in field_spec:
        return field_spec["const"]
    if "path" not in field_spec:
        raise DeclarativeSpecError(f"{where}: map entry needs a 'path' or 'const'")

    value = resolve_path(item, field_spec["path"], _MISSING)
    if value is _MISSING or value is None:
        if field_spec.get("required"):
            raise DeclarativeRequestError(
                f"{where}: required field {field_spec['path']!r} is missing from the "
                f"provider response"
            )
        return field_spec.get("default")

    scale = _scale_of(field_spec, where=where)
    if scale is not None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise DeclarativeRequestError(
                f"{where}: cannot scale non-numeric value {value!r} from "
                f"{field_spec['path']!r}"
            )
        value = value * scale
    return _coerce(value, field_spec.get("type"), where=where)


def apply_map(item: Any, mapping: dict, *, where: str) -> dict:
    """Map a response object through a spec map, honouring nested sub-maps."""
    if not isinstance(mapping, dict):
        raise DeclarativeSpecError(f"{where}: map must be an object")
    result: dict = {}
    for key, field_spec in mapping.items():
        child = f"{where}.{key}"
        if isinstance(field_spec, dict) and not _is_field_spec(field_spec):
            result[key] = apply_map(item, field_spec, where=child)
        else:
            result[key] = map_field(item, field_spec, where=child)
    return result


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


def spec_endpoints(spec: dict) -> dict:
    """Return the operation table, accepting nested or top-level layouts."""
    endpoints = spec.get("endpoints")
    if isinstance(endpoints, dict):
        return endpoints
    return {name: spec[name] for name in OPERATIONS if name in spec}


class DeclarativeRestConnector(CloudConnector):
    """A ``CloudConnector`` driven entirely by a provider spec dict."""

    def __init__(
        self,
        spec: dict,
        api_key: str | None = None,
        *,
        http: Any | None = None,
        base_url: str | None = None,
        sleep: Any | None = None,
        monotonic: Any | None = None,
    ):
        problems = validate_spec(spec)
        if problems:
            raise DeclarativeSpecError("Invalid provider spec: " + "; ".join(problems))

        self.spec = copy.deepcopy(spec)
        self.endpoints = spec_endpoints(self.spec)
        self._name = str(self.spec["name"]).strip().lower()
        self.base_url = str(base_url or self.spec["base_url"]).rstrip("/")
        self.auth = dict(self.spec.get("auth") or {"type": "bearer"})
        self.timeout = self.spec.get("timeout", 30)
        self.status_map = {
            str(key).lower(): str(value)
            for key, value in (self.spec.get("status_map") or {}).items()
        }
        self.default_status = str(self.spec.get("default_status", "unknown"))
        self.client_filter = bool(self.spec.get("client_filter", True))
        self.include_raw = bool(self.spec.get("include_raw", True))

        self.api_key = (api_key or "").strip()
        if not self.api_key and self.auth.get("type", "bearer") != "none":
            raise ValueError(
                f"API key required for declarative provider {self._name!r}; "
                f"set {_config.provider_env_var(self._name)} or store a credential"
            )

        if http is None:
            try:
                import requests
            except ImportError as exc:  # pragma: no cover - dependency is declared
                raise ImportError("requests required: pip install requests") from exc
            http = requests
        self.http = http
        self._sleep = sleep or time.sleep
        self._monotonic = monotonic or time.monotonic

    # -- plumbing ---------------------------------------------------------

    @property
    def name(self) -> str:
        return self._name

    @property
    def display_name(self) -> str:
        return str(self.spec.get("display_name") or self._name)

    def _endpoint(self, operation: str) -> dict:
        endpoint = self.endpoints.get(operation)
        if not isinstance(endpoint, dict):
            raise DeclarativeSpecError(
                f"{self._name}: spec defines no {operation!r} endpoint"
            )
        return endpoint

    def _auth_parts(self) -> tuple[dict, dict]:
        """Return ``(headers, query_params)`` carrying the credential."""
        auth_type = str(self.auth.get("type", "bearer")).lower()
        if auth_type == "none":
            return {}, {}
        if auth_type == "bearer":
            prefix = self.auth.get("prefix", "Bearer ")
            header = self.auth.get("name", "Authorization")
            return {header: f"{prefix}{self.api_key}"}, {}
        if auth_type == "header":
            prefix = self.auth.get("prefix", "")
            return {str(self.auth["name"]): f"{prefix}{self.api_key}"}, {}
        if auth_type == "query":
            return {}, {str(self.auth["name"]): self.api_key}
        if auth_type == "basic":
            slot = str(self.auth.get("in", "username")).lower()
            username = str(self.auth.get("username", ""))
            if slot == "username":
                username, password = self.api_key, ""
            else:
                password = self.api_key
            token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode(
                "ascii"
            )
            return {"Authorization": f"Basic {token}"}, {}
        raise DeclarativeSpecError(f"{self._name}: unknown auth type {auth_type!r}")

    def _variables(self, **overrides) -> dict:
        variables = {name: None for name in TEMPLATE_VARIABLES}
        variables["provider"] = self._name
        variables.update(overrides)
        return variables

    def _request(self, operation: str, variables: dict) -> Any:
        endpoint = self._endpoint(operation)
        where = f"{self._name}.{operation}"
        method = str(endpoint.get("method", "GET")).upper()
        path = render_template(endpoint.get("path", ""), variables, where=f"{where}.path")
        url = f"{self.base_url}/{str(path).lstrip('/')}"

        auth_headers, auth_params = self._auth_parts()
        headers = {**(self.spec.get("headers") or {}), **auth_headers}

        params = dict(auth_params)
        rendered_query = render_template(
            endpoint.get("query") or {}, variables, where=f"{where}.query"
        )
        if isinstance(rendered_query, dict):
            params.update(rendered_query)

        kwargs: dict = {"headers": headers, "timeout": self.timeout}
        if params:
            kwargs["params"] = params
        if "body" in endpoint:
            body = render_template(endpoint["body"], variables, where=f"{where}.body")
            if body is not _OMIT:
                kwargs["json"] = body

        try:
            response = self.http.request(method, url, **kwargs)
        except DeclarativeRequestError:
            raise
        except Exception as exc:
            raise DeclarativeRequestError(
                f"{where}: {method} {url} could not be sent: {exc}"
            ) from exc

        return self._payload(response, where=where, method=method, url=url)

    def _payload(self, response: Any, *, where: str, method: str, url: str) -> Any:
        status_code = getattr(response, "status_code", None)
        raise_for_status = getattr(response, "raise_for_status", None)
        if raise_for_status is not None:
            try:
                raise_for_status()
            except Exception as exc:
                error = DeclarativeRequestError(
                    f"{where}: {method} {url} returned HTTP {status_code}: {exc}"
                )
                error.status_code = status_code
                raise error from exc
        elif status_code is not None and status_code >= 400:
            error = DeclarativeRequestError(
                f"{where}: {method} {url} returned HTTP {status_code}"
            )
            error.status_code = status_code
            raise error

        if status_code == 204:
            return {}
        content = getattr(response, "content", None)
        text = getattr(response, "text", None)
        if not content and not text:
            return {}
        try:
            return response.json()
        except Exception as exc:
            preview = str(text or "")[:120]
            raise DeclarativeRequestError(
                f"{where}: {method} {url} returned a non-JSON body ({preview!r}): {exc}"
            ) from exc

    def _items(self, endpoint: dict, payload: Any, *, where: str) -> list:
        items_path = endpoint.get("items")
        fallback = _MISSING
        if isinstance(items_path, dict):
            fallback = items_path.get("default", _MISSING)
            items_path = items_path.get("path")
        if items_path is None:
            items = payload
        else:
            items = resolve_path(payload, items_path, _MISSING)
            if items is _MISSING or items is None:
                if fallback is not _MISSING:
                    return list(fallback)
                raise DeclarativeRequestError(
                    f"{where}: response has no {items_path!r} collection"
                )
        if not isinstance(items, list):
            raise DeclarativeRequestError(
                f"{where}: expected a list at {items_path or '$'}, got "
                f"{type(items).__name__}"
            )
        for item in items:
            if not isinstance(item, dict):
                raise DeclarativeRequestError(
                    f"{where}: list entries must be objects, got {type(item).__name__}"
                )
        return items

    def _selects(
        self, item: dict, select: Any, variables: dict, *, where: str
    ) -> bool:
        """Evaluate a ``select`` clause against one collection entry."""
        if not select:
            return True
        if not isinstance(select, dict) or "where" not in select:
            raise DeclarativeSpecError(
                f"{where}: select needs a 'where' path plus 'equals', 'in' or 'not_in'"
            )
        value = resolve_path(item, select["where"], None)
        type_name = select.get("type")
        if type_name is not None:
            value = _coerce(value, type_name, where=where)
        if "equals" in select:
            expected = render_template(
                select["equals"], variables, where=f"{where}.equals"
            )
            if type_name is not None:
                expected = _coerce(expected, type_name, where=where)
            return value == expected
        for key, negate in (("in", False), ("not_in", True)):
            if key in select:
                allowed = render_template(
                    select[key], variables, where=f"{where}.{key}"
                )
                if not isinstance(allowed, list):
                    raise DeclarativeSpecError(f"{where}: '{key}' must be a list")
                return (value not in allowed) if negate else (value in allowed)
        return value is not None

    # -- offers -----------------------------------------------------------

    def list_available(
        self,
        gpu_type: str | None = None,
        min_gpu_ram: int | None = None,
        max_hourly_rate: float | None = None,
    ) -> list[dict]:
        """List offers, mapped into the normalized offer dict."""
        endpoint = self._endpoint("offers")
        where = f"{self._name}.offers"
        variables = self._variables(
            gpu_type=gpu_type,
            min_gpu_ram=min_gpu_ram,
            max_hourly_rate=max_hourly_rate,
        )
        try:
            payload = self._request("offers", variables)
        except DeclarativeRequestError as exc:
            if str(endpoint.get("on_error", "raise")).lower() != "empty":
                raise
            logger.warning("%s: returning no offers: %s", where, exc)
            return []
        offers = [
            self._map_offer(item, endpoint, where=where)
            for item in self._items(endpoint, payload, where=where)
            if self._selects(item, endpoint.get("select"), variables, where=where)
        ]
        if not self.client_filter:
            return offers
        return [
            offer
            for offer in offers
            if self._offer_matches(offer, gpu_type, min_gpu_ram, max_hourly_rate)
        ]

    def _map_offer(self, item: dict, endpoint: dict, *, where: str) -> dict:
        offer = apply_map(item, endpoint.get("map") or {"id": "$.id"}, where=where)
        offer["id"] = "" if offer.get("id") is None else str(offer["id"])
        offer["provider"] = self._name
        if self.include_raw:
            offer["raw"] = item
        return offer

    @staticmethod
    def _normalize_gpu(value: str) -> str:
        return " ".join(str(value).replace("_", " ").replace("-", " ").lower().split())

    def _offer_matches(
        self,
        offer: dict,
        gpu_type: str | None,
        min_gpu_ram: int | None,
        max_hourly_rate: float | None,
    ) -> bool:
        """Post-filter offers so caps are honoured even without server filters."""
        if gpu_type:
            needle = self._normalize_gpu(gpu_type)
            haystack = self._normalize_gpu(offer.get("gpu_type") or "")
            if needle != haystack and needle not in haystack:
                return False
        if min_gpu_ram is not None:
            ram = offer.get("gpu_ram_gb")
            if ram is None or ram < min_gpu_ram:
                return False
        if max_hourly_rate is not None:
            rate = offer.get("hourly_rate")
            if rate is None or rate > max_hourly_rate:
                return False
        return True

    # -- instances --------------------------------------------------------

    def _map_instance(self, item: dict, endpoint: dict, *, where: str) -> Instance:
        mapped = apply_map(item, endpoint.get("map") or {"id": "$.id"}, where=where)
        metadata = mapped.pop("metadata", None)
        raw_status = mapped.pop("status", None)
        status = self.status_map.get(
            str(raw_status).lower() if raw_status is not None else "",
            self.default_status,
        )
        identifier = mapped.pop("id", None)
        if identifier is None:
            raise DeclarativeRequestError(f"{where}: instance response has no id")
        gpu_count = mapped.pop("gpu_count", None)
        hourly_rate = mapped.pop("hourly_rate", None)
        extra = {
            key: value
            for key, value in mapped.items()
            if key not in {"gpu_type", "ip_address", "ssh_port"}
        }
        if not isinstance(metadata, dict):
            metadata = {} if metadata is None else {"value": metadata}
        metadata.update(extra)
        return Instance(
            id=str(identifier),
            provider=self._name,
            gpu_type=mapped.get("gpu_type") or "unknown",
            gpu_count=1 if gpu_count is None else gpu_count,
            hourly_rate=0 if hourly_rate is None else hourly_rate,
            status=status,
            ip_address=mapped.get("ip_address"),
            ssh_port=mapped.get("ssh_port"),
            metadata=metadata,
        )

    def launch(
        self,
        offer_id: str,
        docker_image: str,
        env_vars: dict | None = None,
        startup_script: str | None = None,
        disk_gb: int | None = None,
        placement: PlacementConstraints | None = None,
        resource_name: str | None = None,
        min_cuda_version: str | None = None,
    ) -> Instance:
        """Launch an instance, optionally polling until it reports ready.

        ``disk_gb`` is offered to the spec as a template variable and is
        otherwise ignored: a provider whose launch payload has no place for a
        container disk keeps rendering exactly the request it rendered before.
        """
        if min_cuda_version:
            raise ValueError(f"{self._name} cannot enforce a minimum CUDA version")
        endpoint = self._endpoint("launch")
        where = f"{self._name}.launch"
        variables = self._variables(
            offer_id=offer_id,
            docker_image=docker_image,
            env_vars=env_vars or {},
            startup_script=startup_script,
            disk_gb=disk_gb,
            resource_name=resource_name,
        )
        payload = self._request("launch", variables)
        if not isinstance(payload, dict):
            raise DeclarativeRequestError(
                f"{where}: expected an object response, got {type(payload).__name__}"
            )
        instance = self._map_instance(payload, endpoint, where=where)

        wait = endpoint.get("wait_for")
        if not wait:
            return instance
        settings = wait if isinstance(wait, dict) else {}
        return self._wait_for(
            instance.id,
            wanted=str(settings.get("status", "running")),
            timeout=float(settings.get("timeout_seconds", 300)),
            interval=float(settings.get("interval_seconds", 5)),
        )

    def _wait_for(
        self, instance_id: str, *, wanted: str, timeout: float, interval: float
    ) -> Instance:
        """Poll ``get`` until the instance reaches ``wanted`` or time runs out."""
        deadline = self._monotonic() + timeout
        while True:
            instance = self.get_instance(instance_id)
            if instance is not None and instance.status == wanted:
                return instance
            if self._monotonic() >= deadline:
                raise TimeoutError(
                    f"{self._name} instance {instance_id} did not reach "
                    f"{wanted!r} within {timeout:g}s"
                )
            self._sleep(interval)

    def get_instance(self, instance_id: str) -> Instance | None:
        """Get one instance by ID, scanning a listing when there is no by-id route."""
        endpoint = self._endpoint("get")
        where = f"{self._name}.get"
        variables = self._variables(instance_id=instance_id)
        try:
            payload = self._request("get", variables)
        except DeclarativeRequestError as exc:
            if getattr(exc, "status_code", None) == 404:
                return None
            if str(endpoint.get("on_error", "raise")).lower() != "null":
                raise
            logger.warning("%s: treating %s as not found: %s", where, instance_id, exc)
            return None

        if "items" not in endpoint:
            if not payload:
                return None
            if not isinstance(payload, dict):
                raise DeclarativeRequestError(
                    f"{where}: expected an object response, got {type(payload).__name__}"
                )
            return self._map_instance(payload, endpoint, where=where)

        select = endpoint.get("select") or {
            "where": "$.id",
            "equals": "{{instance_id}}",
            "type": "str",
        }
        for item in self._items(endpoint, payload, where=where):
            if self._selects(item, select, variables, where=f"{where}.select"):
                return self._map_instance(item, endpoint, where=where)
        return None

    def list_instances(self) -> list[Instance]:
        """List instances, applying the spec's ``select`` filter."""
        endpoint = self._endpoint("list")
        where = f"{self._name}.list"
        variables = self._variables()
        try:
            payload = self._request("list", variables)
        except DeclarativeRequestError as exc:
            if str(endpoint.get("on_error", "raise")).lower() != "empty":
                raise
            logger.warning("%s: returning no instances: %s", where, exc)
            return []
        select = endpoint.get("select")
        status_in = endpoint.get("status_in")
        instances = []
        for item in self._items(endpoint, payload, where=where):
            if not self._selects(item, select, variables, where=f"{where}.select"):
                continue
            instance = self._map_instance(item, endpoint, where=where)
            if status_in and instance.status not in status_in:
                continue
            instances.append(instance)
        return instances

    def terminate(self, instance_id: str) -> bool:
        """Terminate an instance; ``False`` when the provider refuses."""
        try:
            payload = self._request(
                "terminate", self._variables(instance_id=instance_id)
            )
        except (DeclarativeRequestError, DeclarativeSpecError):
            return False
        success = self._endpoint("terminate").get("success")
        if not success:
            return True
        value = resolve_path(payload, success.get("path", "$"), None)
        return value == success.get("equals", True)

    def account_balance(self) -> dict:
        """Return normalized account credit, when the spec declares it."""
        if "balance" not in self.endpoints:
            return {"available": False, "currency": "USD"}
        endpoint = self._endpoint("balance")
        where = f"{self._name}.balance"
        payload = self._request("balance", self._variables())
        if not isinstance(payload, dict):
            raise DeclarativeRequestError(
                f"{where}: expected an object response, got {type(payload).__name__}"
            )
        mapped = apply_map(payload, endpoint.get("map") or {}, where=where)
        return {
            "available": True,
            "currency": str(endpoint.get("currency", "USD")),
            **mapped,
        }


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _validate_template(value: Any, where: str, problems: list[str]) -> None:
    """Check placeholders and directives without executing a request."""
    if isinstance(value, str):
        for name in _PLACEHOLDER.findall(value):
            if name not in TEMPLATE_VARIABLES:
                problems.append(
                    f"{where}: unknown template variable {{{{{name}}}}} "
                    f"(known: {', '.join(TEMPLATE_VARIABLES)})"
                )
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_template(item, f"{where}[{index}]", problems)
        return
    if not isinstance(value, dict):
        return

    if "$value" in value:
        name = value["$value"]
        if not isinstance(name, str):
            problems.append(f"{where}: $value must name a template variable")
        elif _variable(name) not in TEMPLATE_VARIABLES:
            problems.append(f"{where}: $value references unknown variable {name!r}")
        if str(value.get("omit_if", "null")).lower() not in {"null", "empty", "never"}:
            problems.append(f"{where}: omit_if must be null, empty or never")
        try:
            _scale_of(value, where=where)
        except DeclarativeSpecError as exc:
            problems.append(str(exc))
        if value.get("type") is not None and value["type"] not in VALUE_TYPES:
            problems.append(f"{where}: type must be one of {', '.join(VALUE_TYPES)}")
        return

    guard = value.get("$when")
    if guard is not None:
        names = [guard] if isinstance(guard, str) else guard
        if not isinstance(names, list) or not all(isinstance(n, str) for n in names):
            problems.append(f"{where}: $when must be a name or list of names")
        else:
            for name in names:
                if _variable(name) not in TEMPLATE_VARIABLES:
                    problems.append(
                        f"{where}: $when references unknown variable {name!r}"
                    )
    for key, item in value.items():
        if key == "$when":
            continue
        _validate_template(item, f"{where}.{key}", problems)


def _validate_map(mapping: Any, where: str, problems: list[str]) -> None:
    if not isinstance(mapping, dict):
        problems.append(f"{where}: map must be an object")
        return
    if not mapping:
        problems.append(f"{where}: map is empty")
        return
    for key, field_spec in mapping.items():
        child = f"{where}.{key}"
        if isinstance(field_spec, str):
            try:
                parse_path(field_spec)
            except DeclarativeSpecError as exc:
                problems.append(f"{child}: {exc}")
            continue
        if isinstance(field_spec, dict) and not _is_field_spec(field_spec):
            _validate_map(field_spec, child, problems)
            continue
        if not isinstance(field_spec, dict):
            problems.append(
                f"{child}: map entry must be a path string or object, got "
                f"{type(field_spec).__name__}"
            )
            continue
        if "const" not in field_spec and "path" not in field_spec:
            problems.append(f"{child}: map entry needs a 'path' or 'const'")
        if "path" in field_spec:
            try:
                parse_path(field_spec["path"])
            except DeclarativeSpecError as exc:
                problems.append(f"{child}: {exc}")
        try:
            _scale_of(field_spec, where=child)
        except DeclarativeSpecError as exc:
            problems.append(str(exc))
        if field_spec.get("type") is not None and field_spec["type"] not in VALUE_TYPES:
            problems.append(f"{child}: type must be one of {', '.join(VALUE_TYPES)}")


def _validate_select(select: Any, where: str, problems: list[str]) -> None:
    if not isinstance(select, dict) or "where" not in select:
        problems.append(
            f"{where}: select needs a 'where' path plus 'equals', 'in' or 'not_in'"
        )
        return
    try:
        parse_path(select["where"])
    except DeclarativeSpecError as exc:
        problems.append(f"{where}: {exc}")
    if not {"equals", "in", "not_in"} & set(select):
        problems.append(f"{where}: select needs one of 'equals', 'in' or 'not_in'")
    for key in ("in", "not_in"):
        if key in select and not isinstance(select[key], list):
            problems.append(f"{where}: '{key}' must be a list")
    if select.get("type") is not None and select["type"] not in VALUE_TYPES:
        problems.append(f"{where}: type must be one of {', '.join(VALUE_TYPES)}")
    for key in ("equals", "in", "not_in"):
        if key in select:
            _validate_template(select[key], f"{where}.{key}", problems)


#: Key names that would mean a spec is carrying a secret it has no business
#: carrying.  A spec is shareable *because* it holds none: the connector takes
#: its credential from ``config.api_key_for()`` and no template variable exposes
#: it, so a key shaped like one is a spec error, not a style preference.
CREDENTIAL_KEY_HINTS = (
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "passwd",
    "password",
    "secret",
    "token",
)


def credential_like_keys(value: Any, where: str = "spec") -> list[str]:
    """Return the paths of any keys in a spec that look like a credential."""
    found: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            child = f"{where}.{key}"
            if isinstance(key, str) and any(
                hint in key.lower() for hint in CREDENTIAL_KEY_HINTS
            ):
                found.append(child)
            found.extend(credential_like_keys(item, child))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            found.extend(credential_like_keys(item, f"{where}[{index}]"))
    return found


def validate_spec(spec: Any) -> list[str]:
    """Return human-readable problems with a provider spec (empty = valid)."""
    problems: list[str] = []
    if not isinstance(spec, dict):
        return [f"spec must be a JSON object, got {type(spec).__name__}"]

    problems.extend(
        f"{key} looks like a credential; specs hold none — store the API key "
        f"through POST /api/providers/{{provider}}/credentials or "
        f"{_config.provider_env_var(str(spec.get('name') or 'provider'))}"
        for key in credential_like_keys(spec)
    )

    name = spec.get("name")
    if not isinstance(name, str) or not name.strip():
        problems.append("'name' is required and must be a non-empty string")

    aliases = spec.get("aliases")
    if aliases is not None:
        if not isinstance(aliases, list) or not all(
            isinstance(alias, str) and alias.strip() for alias in aliases
        ):
            problems.append("'aliases' must be a list of non-empty strings")

    base_url = spec.get("base_url")
    if not isinstance(base_url, str) or not base_url.strip():
        problems.append("'base_url' is required and must be a non-empty string")
    elif not base_url.strip().lower().startswith(("http://", "https://")):
        problems.append("'base_url' must start with http:// or https://")

    field_name = spec.get("base_url_config_field")
    if field_name is not None and (
        not isinstance(field_name, str) or not field_name.strip()
    ):
        problems.append("'base_url_config_field' must be a non-empty string")

    auth = spec.get("auth", {"type": "bearer"})
    if not isinstance(auth, dict):
        problems.append("'auth' must be an object")
    else:
        auth_type = str(auth.get("type", "bearer")).lower()
        if auth_type not in AUTH_TYPES:
            problems.append(
                f"unknown auth type {auth.get('type')!r} "
                f"(supported: {', '.join(AUTH_TYPES)})"
            )
        elif auth_type in {"header", "query"} and not str(auth.get("name", "")).strip():
            problems.append(f"auth type {auth_type!r} requires a 'name'")
        elif auth_type == "basic" and str(auth.get("in", "username")).lower() not in {
            "username",
            "password",
        }:
            problems.append("basic auth 'in' must be 'username' or 'password'")

    timeout = spec.get("timeout", 30)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0:
        problems.append("'timeout' must be a positive number")

    headers = spec.get("headers")
    if headers is not None and not isinstance(headers, dict):
        problems.append("'headers' must be an object")

    status_map = spec.get("status_map")
    if status_map is not None:
        if not isinstance(status_map, dict):
            problems.append("'status_map' must be an object")
        else:
            for key, value in status_map.items():
                if value not in NORMALIZED_STATUSES:
                    problems.append(
                        f"status_map[{key!r}] = {value!r} is not one of "
                        f"{', '.join(NORMALIZED_STATUSES)}"
                    )

    default_status = spec.get("default_status", "unknown")
    if default_status not in NORMALIZED_STATUSES:
        problems.append(
            f"'default_status' must be one of {', '.join(NORMALIZED_STATUSES)}"
        )

    if spec.get("endpoints") is not None and not isinstance(spec["endpoints"], dict):
        problems.append("'endpoints' must be an object")
    table = spec_endpoints(spec) if isinstance(spec.get("endpoints", {}), dict) else {}
    if not table:
        problems.append(
            "no endpoints defined; at minimum an 'offers' endpoint is required"
        )
    elif "offers" not in table:
        problems.append("missing required 'offers' endpoint")
    for operation in table:
        if operation not in OPERATIONS:
            problems.append(
                f"unknown endpoint {operation!r} (supported: {', '.join(OPERATIONS)})"
            )

    for operation, endpoint in table.items():
        where = f"endpoints.{operation}"
        if not isinstance(endpoint, dict):
            problems.append(f"{where}: endpoint must be an object")
            continue
        method = str(endpoint.get("method", "GET")).upper()
        if method not in HTTP_METHODS:
            problems.append(
                f"{where}: method {endpoint.get('method')!r} is not one of "
                f"{', '.join(HTTP_METHODS)}"
            )
        path = endpoint.get("path", "")
        if not isinstance(path, str):
            problems.append(f"{where}: 'path' must be a string")
        else:
            _validate_template(path, f"{where}.path", problems)
        for section in ("query", "body"):
            if section in endpoint:
                _validate_template(endpoint[section], f"{where}.{section}", problems)
        if "items" in endpoint:
            items = endpoint["items"]
            if isinstance(items, dict):
                if "path" not in items:
                    problems.append(f"{where}.items: needs a 'path'")
                if "default" in items and not isinstance(items["default"], list):
                    problems.append(f"{where}.items: 'default' must be a list")
                items = items.get("path")
            try:
                parse_path(items)
            except DeclarativeSpecError as exc:
                problems.append(f"{where}.items: {exc}")
        if operation in {"offers", "launch", "get", "list"}:
            if "map" not in endpoint:
                problems.append(f"{where}: a 'map' is required")
            else:
                _validate_map(endpoint["map"], f"{where}.map", problems)
        elif "map" in endpoint:
            _validate_map(endpoint["map"], f"{where}.map", problems)
        if "select" in endpoint:
            _validate_select(endpoint["select"], f"{where}.select", problems)
        if "on_error" in endpoint:
            allowed = ERROR_POLICIES.get(operation)
            policy = str(endpoint["on_error"]).lower()
            if allowed is None:
                problems.append(
                    f"{where}.on_error: only "
                    f"{', '.join(sorted(ERROR_POLICIES))} may set an error policy"
                )
            elif policy not in allowed:
                problems.append(
                    f"{where}.on_error: must be one of {', '.join(allowed)}"
                )
        if "wait_for" in endpoint:
            wait = endpoint["wait_for"]
            if not isinstance(wait, dict):
                problems.append(f"{where}.wait_for: must be an object")
            else:
                if operation != "launch":
                    problems.append(
                        f"{where}.wait_for: only the 'launch' endpoint may wait"
                    )
                if str(wait.get("status", "running")) not in NORMALIZED_STATUSES:
                    problems.append(
                        f"{where}.wait_for.status must be one of "
                        f"{', '.join(NORMALIZED_STATUSES)}"
                    )
                for key in ("timeout_seconds", "interval_seconds"):
                    value = wait.get(key, 1)
                    if (
                        isinstance(value, bool)
                        or not isinstance(value, (int, float))
                        or value <= 0
                    ):
                        problems.append(f"{where}.wait_for.{key} must be positive")
                if "get" not in table:
                    problems.append(
                        f"{where}.wait_for: requires a 'get' endpoint to poll"
                    )

    settings_schema = spec.get("settings_schema")
    if settings_schema is not None and not isinstance(settings_schema, list):
        problems.append("'settings_schema' must be a list")

    return problems


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------


def dry_run_spec(spec: Any, api_key: str | None = None, *, http: Any = None) -> dict:
    """Exercise only the offers call so a spec can be debugged safely.

    Nothing is provisioned and no money is spent: this issues exactly one
    read-only request.  Returns
    ``{"ok", "offer_count", "sample", "error", "problems"}``.
    """
    result: dict = {
        "ok": False,
        "offer_count": 0,
        "sample": None,
        "error": None,
        "problems": [],
    }
    problems = validate_spec(spec)
    if problems:
        result["problems"] = problems
        result["error"] = "Invalid provider spec: " + "; ".join(problems)
        return result

    result["provider"] = str(spec["name"]).strip().lower()
    try:
        connector = DeclarativeRestConnector(spec, api_key=api_key, http=http)
        offers = connector.list_available()
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    result["ok"] = True
    result["offer_count"] = len(offers)
    result["sample"] = offers[0] if offers else None
    return result


# ---------------------------------------------------------------------------
# Spec files and registration
# ---------------------------------------------------------------------------

#: Specs shipped inside the package.  These register by default so that a
#: provider served declaratively is never lost just because the user has no
#: ``~/.cloud-offload/providers`` directory.
BUILTIN_SPEC_DIR = Path(__file__).resolve().parent / "specs"


#: A spec name doubles as a file stem, so it is restricted to characters that
#: cannot escape the spec directory or mean something to a shell.
SPEC_NAME_PATTERN = re.compile(r"[a-z0-9][a-z0-9._-]*")

#: Longest accepted spec name, so a name can never exceed a filesystem limit.
MAX_SPEC_NAME_LENGTH = 64


def spec_directory(directory: str | Path | None = None) -> Path:
    """Return the user directory provider specs are read from."""
    if directory is not None:
        return Path(directory)
    return _config.CONFIG_DIR / "providers"


def spec_file_path(name: str, directory: str | Path | None = None) -> Path:
    """Return the file a user spec named ``name`` is stored in.

    Raises ``ValueError`` for any name that is not a bare file stem, so a name
    arriving from an HTTP route can never traverse out of the spec directory or
    address a file the caller did not mean to name.
    """
    candidate = str(name or "").strip().lower()
    if not candidate:
        raise ValueError("A provider spec name is required")
    if len(candidate) > MAX_SPEC_NAME_LENGTH:
        raise ValueError(
            f"Provider spec name is too long "
            f"({MAX_SPEC_NAME_LENGTH} characters maximum)"
        )
    if any(separator in candidate for separator in ("/", "\\", ":")):
        raise ValueError(
            f"Invalid provider spec name {name!r}: path separators are not allowed"
        )
    if not SPEC_NAME_PATTERN.fullmatch(candidate):
        # The pattern is what actually contains the name: it admits no separator,
        # no leading dot and no space, so the result cannot leave the directory.
        raise ValueError(
            f"Invalid provider spec name {name!r}: use lowercase letters, digits, "
            "'.', '-' and '_', starting with a letter or digit"
        )
    return spec_directory(directory) / f"{candidate}.json"


def describe_spec_files(directory: str | Path | None = None) -> list[dict]:
    """Describe every spec file in a directory, valid or not.

    Returns ``{"name", "display_name", "source", "valid", "problems"}`` per file.
    Specs never carry credentials — they cannot, because the connector takes its
    API key from ``config.api_key_for()`` and no template variable exposes it —
    so a description is safe to hand to a UI verbatim.
    """
    specs, failures = _read_spec_files(spec_directory(directory))
    described = [
        {
            "name": str(spec["name"]).strip().lower(),
            "display_name": str(spec.get("display_name") or spec["name"]),
            "source": spec.get("_source"),
            "valid": True,
            "problems": [],
        }
        for spec in specs
    ]
    described.extend(
        {
            "name": failure.get("name"),
            "display_name": failure.get("name"),
            "source": failure.get("source"),
            "valid": False,
            "problems": list(failure.get("errors") or []),
        }
        for failure in failures
    )
    return sorted(described, key=lambda entry: (entry["name"] or "", entry["source"] or ""))


def _read_spec_files(directory: str | Path) -> tuple[list[dict], list[dict]]:
    """Return ``(specs, failures)`` for every ``*.json`` in a directory."""
    path = Path(directory)
    specs: list[dict] = []
    failures: list[dict] = []
    try:
        candidates = sorted(path.glob("*.json"))
    except OSError as exc:
        return specs, [{"source": str(path), "name": None, "errors": [str(exc)]}]

    for candidate in candidates:
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            failures.append(
                {
                    "source": str(candidate),
                    "name": candidate.stem,
                    "errors": [f"could not be read as JSON: {exc}"],
                }
            )
            continue
        problems = validate_spec(payload)
        if problems:
            failures.append(
                {
                    "source": str(candidate),
                    "name": (payload.get("name") if isinstance(payload, dict) else None)
                    or candidate.stem,
                    "errors": problems,
                }
            )
            continue
        payload["_source"] = str(candidate)
        specs.append(payload)
    return specs, failures


@lru_cache(maxsize=1)
def _builtin_spec_cache() -> tuple[dict, ...]:
    """Parse the in-package specs once. They are package data: they cannot change."""
    return tuple(_read_spec_files(BUILTIN_SPEC_DIR)[0])


def builtin_provider_specs() -> list[dict]:
    """Return the validated specs shipped inside the package.

    Copies, so a caller that edits what it gets back cannot corrupt the cache.
    """
    return [copy.deepcopy(spec) for spec in _builtin_spec_cache()]


def builtin_provider_spec(name: str) -> dict | None:
    """Return the in-package spec registered under ``name``, if there is one."""
    wanted = str(name or "").strip().lower()
    for spec in _builtin_spec_cache():
        if str(spec["name"]).strip().lower() == wanted:
            return copy.deepcopy(spec)
    return None


def load_provider_specs(directory: str | Path | None = None) -> list[dict]:
    """Read ``CONFIG_DIR/providers/*.json``, skipping invalid specs.

    Each returned spec carries a ``_source`` key naming the file it came from.
    Invalid files are skipped; use ``register_declarative_providers`` for a
    report of what failed and why.
    """
    return _read_spec_files(spec_directory(directory))[0]


def connector_factory(spec: dict):
    """Build a registry factory that constructs this spec's connector."""
    frozen = copy.deepcopy(spec)
    name = str(frozen["name"]).strip().lower()
    config_field = frozen.get("base_url_config_field")

    def factory(config):
        settings = config.settings_for(name)
        base_url = settings.get("base_url")
        if not base_url and config_field:
            base_url = str(getattr(config, config_field, "") or "").strip()
        return DeclarativeRestConnector(
            frozen,
            api_key=config.api_key_for(name),
            base_url=base_url or frozen["base_url"],
        )

    return factory


def _settings_schema_for(spec: dict) -> list[dict]:
    schema = spec.get("settings_schema")
    if isinstance(schema, list):
        return schema
    return [
        {
            "key": "base_url",
            "label": "API base URL",
            "type": "string",
            "default": spec["base_url"],
            "help": f"Overrides the base URL declared by the {spec['name']} spec.",
        }
    ]


def register_declarative_providers(
    directory: str | Path | None = None,
    *,
    include_builtin: bool = True,
    include_user: bool = True,
) -> dict:
    """Validate and register every declarative provider spec.

    Returns ``{"loaded": [name, ...], "skipped": [...], "failed": [...]}`` where
    ``skipped`` and ``failed`` entries are ``{"source", "name", "errors"}``.
    Built-in specs (shipped in ``cloud_offload/providers/specs``) load first, then
    user specs from ``CONFIG_DIR/providers``, which may override them.

    A spec is refused rather than allowed to shadow a coded connector of the same
    name: that keeps a stray ``runpod.json`` from silently replacing the real
    RunPod connector, and keeps a built-in spec from taking over a name while the
    coded connector for it still exists.

    Idempotent, and never raises: one broken spec file must not stop the
    coordinator from starting.
    """
    from cloud_offload.providers import connector_metadata, register_connector

    loaded: list[str] = []
    skipped: list[dict] = []
    failed: list[dict] = []
    try:
        specs: list[tuple[dict, bool]] = []
        if include_builtin:
            builtin, builtin_failures = _read_spec_files(BUILTIN_SPEC_DIR)
            failed.extend(builtin_failures)
            specs.extend((spec, True) for spec in builtin)
        if include_user:
            user, user_failures = _read_spec_files(spec_directory(directory))
            failed.extend(user_failures)
            specs.extend((spec, False) for spec in user)
    except Exception as exc:  # pragma: no cover - defensive
        return {
            "loaded": [],
            "skipped": [],
            "failed": [{"source": None, "name": None, "errors": [str(exc)]}],
        }

    for spec, is_builtin in specs:
        source = spec.get("_source")
        name = str(spec["name"]).strip().lower()
        aliases = tuple(
            str(alias).strip().lower()
            for alias in (spec.get("aliases") or [])
            if str(alias).strip()
        )
        try:
            conflict = _shadow_conflict(connector_metadata, name, aliases)
            if conflict is not None:
                report = {
                    "source": source,
                    "name": name,
                    "errors": [
                        f"{conflict[0]!r} is already served by a "
                        f"{conflict[1]} connector"
                        + ("" if is_builtin else "; rename the spec")
                    ],
                }
                (skipped if is_builtin else failed).append(report)
                continue
            register_connector(
                name,
                connector_factory(spec),
                aliases=aliases,
                replace=True,
                kind="declarative",
                display_name=str(spec.get("display_name") or name),
                settings_schema=_settings_schema_for(spec),
            )
        except Exception as exc:  # pragma: no cover - defensive
            failed.append(
                {"source": source, "name": name, "errors": [f"{type(exc).__name__}: {exc}"]}
            )
            continue
        loaded.append(name)

    return {"loaded": loaded, "skipped": skipped, "failed": failed}


def _shadow_conflict(metadata_of, name: str, aliases: tuple[str, ...]):
    """Return ``(name, kind)`` when registering would shadow a coded connector."""
    for candidate in (name, *aliases):
        metadata = metadata_of(candidate)
        if metadata.get("registered") and metadata.get("kind") != "declarative":
            return candidate, metadata.get("kind")
    return None


def shadow_conflict(name: str, aliases: tuple[str, ...] = ()) -> tuple | None:
    """Return ``(name, kind)`` when a spec would shadow a coded connector.

    This is the rule ``register_declarative_providers`` already applies at load
    time, exposed so a caller that wants to refuse a spec *before* writing it —
    an HTTP route, say — gets the identical answer rather than a second copy of
    the rule that can drift from this one.
    """
    from cloud_offload.providers import connector_metadata

    return _shadow_conflict(
        connector_metadata,
        str(name or "").strip().lower(),
        tuple(str(alias).strip().lower() for alias in aliases if str(alias).strip()),
    )
