from __future__ import annotations

import os
from functools import lru_cache
from typing import Any
from urllib.parse import urlparse

import lance

_NAMESPACE_CACHE_SIZE = int(os.environ.get("DAFT_LANCE_NAMESPACE_CACHE_SIZE", "16"))
_PYLANCE_5 = (5, 0, 0)


@lru_cache(maxsize=1)
def _pylance_version() -> tuple[int, ...]:
    version = getattr(lance, "__version__", "0.0.0")
    parts = []
    for part in version.split(".")[:3]:
        digits = "".join(ch for ch in part if ch.isdigit())
        parts.append(int(digits or "0"))
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts)


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


def _create_storage_options_provider(
    namespace_impl: str | None,
    namespace_properties: dict[str, str] | None,
    table_id: list[str] | None,
) -> Any | None:
    if not has_namespace_params(namespace_impl, table_id):
        return None
    namespace = get_or_create_namespace(namespace_impl, namespace_properties)
    if namespace is None or not hasattr(lance, "LanceNamespaceStorageOptionsProvider"):
        return None
    return lance.LanceNamespaceStorageOptionsProvider(namespace=namespace, table_id=table_id)


def get_namespace_kwargs(
    namespace_impl: str | None,
    namespace_properties: dict[str, str] | None,
    table_id: list[str] | None,
) -> dict[str, Any]:
    if not has_namespace_params(namespace_impl, table_id):
        return {}

    namespace = get_or_create_namespace(namespace_impl, namespace_properties)
    if namespace is None:
        return {}

    kwargs: dict[str, Any] = {"table_id": table_id}
    if _pylance_version() >= _PYLANCE_5:
        kwargs["namespace_client"] = namespace
    else:
        kwargs["namespace"] = namespace
        provider = _create_storage_options_provider(namespace_impl, namespace_properties, table_id)
        if provider is not None:
            kwargs["storage_options_provider"] = provider
    return kwargs


def get_write_fragments_kwargs(
    namespace_impl: str | None,
    namespace_properties: dict[str, str] | None,
    table_id: list[str] | None,
) -> dict[str, Any]:
    if not has_namespace_params(namespace_impl, table_id):
        return {}
    if _pylance_version() >= _PYLANCE_5:
        namespace = get_or_create_namespace(namespace_impl, namespace_properties)
        if namespace is None:
            return {}
        return {"namespace_client": namespace, "table_id": table_id}
    provider = _create_storage_options_provider(namespace_impl, namespace_properties, table_id)
    return {"storage_options_provider": provider} if provider is not None else {}


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
    try:
        from lance_namespace import DeclareTableRequest

        response = namespace.declare_table(DeclareTableRequest(id=table_id, location=None))
        return _response_location(response), _storage_options(response)
    except (AttributeError, NotImplementedError):
        from lance_namespace import CreateEmptyTableRequest

        response = namespace.create_empty_table(CreateEmptyTableRequest(id=table_id))
        return _response_location(response), _storage_options(response)


def resolve_namespace_table(
    *,
    namespace_impl: str | None,
    namespace_properties: dict[str, str] | None,
    table_id: list[str] | None,
    mode: str = "read",
) -> tuple[str | None, dict[str, str] | None]:
    namespace = get_or_create_namespace(namespace_impl, namespace_properties)
    if namespace is None or table_id is None:
        return None, None

    if mode == "create":
        return _declare_table(namespace, table_id)

    from lance_namespace import DescribeTableRequest

    try:
        response = namespace.describe_table(DescribeTableRequest(id=table_id))
        return _response_location(response), _storage_options(response)
    except Exception:
        if mode == "overwrite":
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


def open_lance_dataset(
    uri: str | os.PathLike[str] | None,
    *,
    storage_options: dict[str, Any] | None = None,
    namespace_impl: str | None = None,
    namespace_properties: dict[str, str] | None = None,
    table_id: list[str] | None = None,
    mode: str = "read",
    **kwargs: Any,
) -> lance.LanceDataset:
    validate_uri_or_namespace(uri, namespace_impl, table_id)
    resolved_uri = str(uri) if uri is not None else None
    namespace_storage_options = None
    if resolved_uri is None:
        resolved_uri, namespace_storage_options = resolve_namespace_table(
            namespace_impl=namespace_impl,
            namespace_properties=namespace_properties,
            table_id=table_id,
            mode=mode,
        )
    if resolved_uri is None:
        raise ValueError("Unable to resolve Lance dataset URI.")

    merged_storage_options = merge_storage_options(storage_options, namespace_storage_options)
    return lance.dataset(
        None if has_namespace_params(namespace_impl, table_id) else resolved_uri,
        storage_options=merged_storage_options,
        **get_namespace_kwargs(namespace_impl, namespace_properties, table_id),
        **kwargs,
    )
