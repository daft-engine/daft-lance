from __future__ import annotations

import os
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


def _declare_table(namespace: Any, table_id: list[str]) -> tuple[str, dict[str, str] | None]:
    from lance_namespace import DeclareTableRequest

    response = namespace.declare_table(DeclareTableRequest(id=table_id, location=None))
    return _response_location(response), _storage_options(response)


def is_table_not_found(error: Exception) -> bool:
    """Whether an exception from ``describe_table`` means the table does not exist.

    Native implementations raise the typed ``TableNotFoundError``; the Rust-backed
    REST client surfaces untyped errors, so fall back to message matching.
    """
    try:
        from lance_namespace.errors import TableNotFoundError

        if isinstance(error, TableNotFoundError):
            return True
    except ImportError:
        pass
    message = str(error).lower()
    return "not found" in message or "does not exist" in message or "no such table" in message


def resolve_namespace_table(
    *,
    namespace_impl: str | None,
    namespace_properties: dict[str, str] | None,
    table_id: list[str] | None,
    mode: str = "read",
) -> tuple[str | None, dict[str, str] | None]:
    """Resolve a namespace table to ``(location, storage_options)``.

    ``mode="create"`` declares the table; ``mode="overwrite"`` declares it only when
    it does not exist yet; other modes require the table to exist.
    """
    namespace = get_or_create_namespace(namespace_impl, namespace_properties)
    if namespace is None or table_id is None:
        return None, None

    if mode == "create":
        return _declare_table(namespace, table_id)

    from lance_namespace import DescribeTableRequest

    try:
        response = namespace.describe_table(DescribeTableRequest(id=table_id))
        return _response_location(response), _storage_options(response)
    except Exception as error:
        if mode == "overwrite" and is_table_not_found(error):
            return _declare_table(namespace, table_id)
        raise


def merge_storage_options(
    storage_options: dict[str, Any] | None,
    namespace_storage_options: dict[str, str] | None,
) -> dict[str, Any] | None:
    merged: dict[str, Any] = {}
    if storage_options:
        merged.update(storage_options)
    if namespace_storage_options:
        merged.update(namespace_storage_options)
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
