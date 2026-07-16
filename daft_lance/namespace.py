from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any
from urllib.parse import urlparse

import lance

_NAMESPACE_CACHE_SIZE = int(os.environ.get("DAFT_LANCE_NAMESPACE_CACHE_SIZE", "16"))


def has_namespace_params(namespace_impl: str | None, table_id: list[str] | None) -> bool:
    return namespace_impl is not None and table_id is not None


def validate_uri_or_namespace(
    uri: str | os.PathLike[str] | None,
    namespace_impl: str | None,
    table_id: list[str] | None,
) -> None:
    has_uri = uri is not None
    has_ns = has_namespace_params(namespace_impl, table_id)

    if namespace_impl is not None and table_id is None:
        raise ValueError("'table_id' must be provided when 'namespace_impl' is provided.")
    if table_id is not None and namespace_impl is None:
        raise ValueError("'namespace_impl' must be provided when 'table_id' is provided.")
    if has_uri and has_ns:
        raise ValueError(
            "Cannot provide both 'uri' and namespace parameters. Use either 'uri' OR ('namespace_impl' + 'table_id')."
        )
    if not has_uri and not has_ns:
        raise ValueError("Must provide either 'uri' OR ('namespace_impl' + 'table_id').")


def _normalize_file_uri(location: str) -> str:
    parsed = urlparse(location)
    if parsed.scheme == "file":
        return parsed.path
    return location


@lru_cache(maxsize=_NAMESPACE_CACHE_SIZE)
def _get_cached_namespace(namespace_impl: str, namespace_properties_tuple: tuple[tuple[str, str], ...] | None) -> Any:
    import lance_namespace as ln

    namespace_properties = dict(namespace_properties_tuple) if namespace_properties_tuple else {}
    return ln.connect(namespace_impl, namespace_properties)


def get_or_create_namespace(namespace_impl: str | None, namespace_properties: dict[str, str] | None) -> Any | None:
    if namespace_impl is None:
        return None
    namespace_properties_tuple = tuple(sorted(namespace_properties.items())) if namespace_properties else None
    return _get_cached_namespace(namespace_impl, namespace_properties_tuple)


def get_namespace_kwargs(
    namespace_impl: str | None,
    namespace_properties: dict[str, str] | None,
    table_id: list[str] | None,
) -> dict[str, Any]:
    """Kwargs wiring a namespace client into pylance APIs (``lance.dataset``, ``commit``, ...).

    Requires pylance >= 7 which accepts ``namespace_client`` + ``table_id`` natively.
    """
    if not has_namespace_params(namespace_impl, table_id):
        return {}

    namespace = get_or_create_namespace(namespace_impl, namespace_properties)
    if namespace is None:
        return {}
    return {"namespace_client": namespace, "table_id": table_id}


# `lance.fragment.write_fragments` accepts the same namespace kwargs as `lance.dataset`.
get_write_fragments_kwargs = get_namespace_kwargs


def _storage_options(response: Any) -> dict[str, str] | None:
    storage_options = getattr(response, "storage_options", None)
    if storage_options is None:
        return None
    return dict(storage_options)


def _response_location(response: Any) -> str:
    location = getattr(response, "location", None) or getattr(response, "table_uri", None)
    if not location:
        raise ValueError("Namespace response did not include a table location.")
    return _normalize_file_uri(str(location))


@dataclass(frozen=True)
class ResolvedNamespaceTable:
    """A namespace table resolved to a physical location.

    ``is_declared_placeholder`` is True when the location comes from a
    ``declare_table`` call (fresh or pre-existing declared-only stub): some
    namespace implementations materialize a stub dataset at declare time, and a
    ``mode="create"`` write must overwrite that stub instead of failing.
    """

    uri: str
    storage_options: dict[str, str] | None = None
    is_declared_placeholder: bool = False


def _resolved_from_response(response: Any, *, is_declared_placeholder: bool = False) -> ResolvedNamespaceTable:
    return ResolvedNamespaceTable(
        uri=_response_location(response),
        storage_options=_storage_options(response),
        is_declared_placeholder=is_declared_placeholder,
    )


def _describe_table(namespace: Any, table_id: list[str], *, check_declared: bool = False) -> Any:
    from lance_namespace import DescribeTableRequest

    request = (
        DescribeTableRequest(id=table_id, check_declared=True) if check_declared else DescribeTableRequest(id=table_id)
    )
    return namespace.describe_table(request)


def _declare_table(namespace: Any, table_id: list[str]) -> ResolvedNamespaceTable:
    from lance_namespace import DeclareTableRequest

    response = namespace.declare_table(DeclareTableRequest(id=table_id, location=None))
    return _resolved_from_response(response, is_declared_placeholder=True)


def is_declared_placeholder_response(response: Any) -> bool:
    """Whether a ``describe_table`` response describes a declared-only stub table.

    ``is_only_declared`` is the standard signal (only reliable when the request
    was made with ``check_declared=True``); the ``lance.declared`` property is a
    compatibility fallback for services that expose the state that way.
    """
    if getattr(response, "is_only_declared", None) is True:
        return True
    for attr in ("properties", "metadata"):
        mapping = getattr(response, attr, None)
        if mapping and str(dict(mapping).get("lance.declared", "")).strip().lower() == "true":
            return True
    return False


def is_table_not_found(error: Exception) -> bool:
    """Whether an exception from ``describe_table`` means the table does not exist.

    Native implementations raise the typed ``TableNotFoundError``; the Rust-backed
    REST client may surface untyped errors, so only fall back when the error is
    clearly a 404 for a table resource.
    """
    try:
        from lance_namespace.errors import TableNotFoundError

        if isinstance(error, TableNotFoundError):
            return True
    except ImportError:
        pass

    status_code = _error_status_code(error)
    if status_code != 404:
        return False

    message = str(error).lower()
    return "no such table" in message or (
        "table" in message and ("not found" in message or "does not exist" in message)
    )


def is_table_already_exists(error: Exception) -> bool:
    """Whether an exception from ``declare_table`` means the table already exists.

    Mirrors :func:`is_table_not_found`: prefer the typed error, fall back to a
    clearly-classified conflict from untyped REST errors.
    """
    try:
        from lance_namespace.errors import TableAlreadyExistsError

        if isinstance(error, TableAlreadyExistsError):
            return True
    except ImportError:
        pass

    status_code = _error_status_code(error)
    if status_code != 409:
        return False

    message = str(error).lower()
    return "table" in message and "already exists" in message


def _error_status_code(error: Exception) -> int | None:
    """Best-effort HTTP status extraction for untyped namespace REST errors."""
    for attr in ("status", "status_code", "code"):
        value = getattr(error, attr, None)
        status_code = _coerce_status_code(value)
        if status_code is not None:
            return status_code

    response = getattr(error, "response", None)
    if response is not None:
        for attr in ("status", "status_code"):
            value = getattr(response, attr, None)
            status_code = _coerce_status_code(value)
            if status_code is not None:
                return status_code

    return None


def _coerce_status_code(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def resolve_namespace_table(
    *,
    namespace_impl: str | None,
    namespace_properties: dict[str, str] | None,
    table_id: list[str] | None,
    mode: str = "read",
) -> ResolvedNamespaceTable | None:
    """Resolve a namespace table to a :class:`ResolvedNamespaceTable`.

    ``mode="create"`` requires the table to not exist (declared-only stubs are
    reused as placeholders); ``mode="overwrite"`` declares the table when it does
    not exist yet; other modes require the table to exist. Only ``mode="create"``
    and ``mode="overwrite"`` have side effects (a ``declare_table`` call).
    """
    namespace = get_or_create_namespace(namespace_impl, namespace_properties)
    if namespace is None or table_id is None:
        return None

    if mode == "create":
        return _resolve_for_create(namespace, table_id)

    try:
        return _resolved_from_response(_describe_table(namespace, table_id))
    except Exception as error:
        if mode == "overwrite" and is_table_not_found(error):
            return _declare_or_recover(namespace, table_id, check_declared=False)
        raise


def _resolve_for_create(namespace: Any, table_id: list[str]) -> ResolvedNamespaceTable:
    """Describe-first create resolution.

    ``declare_table`` fails with a conflict on any existing table (declared-only
    or real), so classify via ``describe_table`` before declaring, and again if
    a concurrent declare wins the race in between.
    """
    try:
        response = _describe_table(namespace, table_id, check_declared=True)
    except Exception as error:
        if not is_table_not_found(error):
            raise
        return _declare_or_recover(namespace, table_id, check_declared=True)
    return _classify_existing_for_create(response, table_id)


def _declare_or_recover(namespace: Any, table_id: list[str], *, check_declared: bool) -> ResolvedNamespaceTable:
    try:
        return _declare_table(namespace, table_id)
    except Exception as error:
        if not is_table_already_exists(error):
            raise
        # Lost a declare race; re-describe and classify what won.
        response = _describe_table(namespace, table_id, check_declared=check_declared)
        if not check_declared:
            return _resolved_from_response(response)
        return _classify_existing_for_create(response, table_id)


def _classify_existing_for_create(response: Any, table_id: list[str]) -> ResolvedNamespaceTable:
    if is_declared_placeholder_response(response):
        return _resolved_from_response(response, is_declared_placeholder=True)
    raise ValueError(
        f"Table {table_id!r} already exists in the namespace and cannot be created. "
        'Use mode="overwrite" to replace it or mode="append" to add to it.'
    )


def merge_storage_options(*layers: dict[str, Any] | None) -> dict[str, Any] | None:
    """Merge storage-option layers; later layers take precedence.

    Callers order layers from lowest to highest priority, conventionally:
    io_config-derived < user-provided ``storage_options`` < namespace-vended.
    """
    merged: dict[str, Any] = {}
    for layer in layers:
        if layer:
            merged.update(layer)
    return merged or None


def pop_namespace_params(open_kwargs: dict[str, Any]) -> tuple[str | None, dict[str, str] | None, list[str] | None]:
    """Remove and return the namespace triple from a ``_lance_open_kwargs`` dict."""
    return (
        open_kwargs.pop("namespace_impl", None),
        open_kwargs.pop("namespace_properties", None),
        open_kwargs.pop("table_id", None),
    )


def namespace_kwargs_for_dataset(ds: Any) -> dict[str, Any]:
    """Namespace kwargs for commit-style calls, recovered from ``ds._lance_open_kwargs``.

    ``lance.LanceDataset.commit`` is a static method, so a dataset opened through a
    namespace does not carry its client into commits; re-derive it from the open
    kwargs stashed by ``construct_lance_dataset``.
    """
    open_kwargs = getattr(ds, "_lance_open_kwargs", None) or {}
    return get_namespace_kwargs(
        open_kwargs.get("namespace_impl"),
        open_kwargs.get("namespace_properties"),
        open_kwargs.get("table_id"),
    )


def open_dataset_from_open_kwargs(ds_uri: str | None, open_kwargs: dict[str, Any] | None) -> lance.LanceDataset:
    """Re-open a dataset on a worker from serialized ``_lance_open_kwargs``.

    The namespace client is not picklable, so only the (impl, properties, table_id)
    triple travels with the task; the client is re-created here via the lru cache.
    """
    open_kwargs = dict(open_kwargs or {})
    namespace_impl, namespace_properties, table_id = pop_namespace_params(open_kwargs)
    return lance.dataset(
        None if has_namespace_params(namespace_impl, table_id) else ds_uri,
        **get_namespace_kwargs(namespace_impl, namespace_properties, table_id),
        **open_kwargs,
    )
