"""Lance Namespace (catalog) integration layer.

Tables can be addressed by a ``(namespace_impl, namespace_properties, table_id)``
triple instead of a raw uri; this module owns everything between that triple and
a pylance call. Three design decisions shape the code:

1. **Only the triple is serialized, never the client.** Namespace clients are
   not picklable, so distributed tasks carry the triple and every process
   rebuilds its client through a per-process ``lru_cache``
   (:func:`get_or_create_namespace`). This is why entry points thread the three
   raw parameters around instead of a client object.

2. **Table creation delegates to the namespace.** ``mode="create"`` maps to
   the atomic ``declare_table`` operation, while overwrite describes first and
   declares only when the namespace raises its typed ``TableNotFoundError``.
   Other namespace failures propagate unchanged.

3. **Distributed workers reopen, they never receive an open dataset.**
   ``LanceDataset.__reduce__`` only carries (uri, storage_options, version,
   manifest, ...); ``_namespace_client``, ``_table_id`` and
   ``_namespace_client_managed_versioning`` are assigned after construction and
   are therefore dropped by pickle. A worker that received a pickled dataset
   would silently lose its namespace identity and its managed-versioning commit
   handler. :class:`DatasetOpenContext` carries the serializable facts instead
   and reopens on the worker.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any
from urllib.parse import unquote, urlparse

import lance
from lance_namespace import DeclareTableResponse, DescribeTableResponse, LanceNamespace

_NAMESPACE_CACHE_SIZE = int(os.environ.get("DAFT_LANCE_NAMESPACE_CACHE_SIZE", "16"))


def has_namespace_params(namespace_impl: str | None, table_id: list[str] | None) -> bool:
    """Whether namespace addressing is in effect (``namespace_properties`` stays optional)."""
    return namespace_impl is not None and table_id is not None


def validate_uri_or_namespace(
    uri: str | os.PathLike[str] | None,
    namespace_impl: str | None,
    table_id: list[str] | None,
    namespace_properties: dict[str, str] | None = None,
) -> None:
    """Enforce that exactly one addressing style is used: ``uri`` XOR the namespace triple."""
    has_uri = uri is not None
    has_ns = has_namespace_params(namespace_impl, table_id)

    if namespace_properties is not None and namespace_impl is None:
        raise ValueError("'namespace_impl' must be provided when 'namespace_properties' is provided.")
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
    """Strip a ``file://`` scheme to a plain filesystem path.

    Namespace impls return locations as URIs (dir namespace vends
    ``file:///...``), but the location is later used where a plain path is
    required: ``write_fragments(dataset_uri=...)``, ``LanceDataset.commit``,
    and ``pathlib`` checks in the sink. Object-store URIs pass through as-is.

    The path is percent-decoded: namespaces vend URI-encoded locations (a table
    under ``daft lance/`` comes back as ``daft%20lance/``), and writing to the
    literal encoded form would create a different directory than the one
    ``describe_table`` later resolves to.
    """
    parsed = urlparse(location)
    if parsed.scheme == "file":
        return unquote(parsed.path)
    return location


@lru_cache(maxsize=_NAMESPACE_CACHE_SIZE)
def _get_cached_namespace(
    namespace_impl: str, namespace_properties_tuple: tuple[tuple[str, str], ...] | None
) -> LanceNamespace:
    import lance_namespace as ln

    namespace_properties = dict(namespace_properties_tuple) if namespace_properties_tuple else {}
    return ln.connect(namespace_impl, namespace_properties)


def get_or_create_namespace(
    namespace_impl: str | None, namespace_properties: dict[str, str] | None
) -> LanceNamespace | None:
    """Per-process namespace client pool.

    ``ln.connect`` may build HTTP clients / perform auth, and workers re-derive
    the client for every scan task and fragment write, so connections are cached
    per (impl, properties). Properties are canonicalized to a sorted tuple
    because ``lru_cache`` keys must be hashable and dict ordering must not
    create duplicate connections.
    """
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


def get_namespace_commit_kwargs(
    namespace_impl: str | None,
    namespace_properties: dict[str, str] | None,
    table_id: list[str] | None,
    managed_versioning: bool,
) -> dict[str, Any]:
    """Namespace kwargs for commit APIs, including catalog-managed versioning."""
    kwargs = get_namespace_kwargs(namespace_impl, namespace_properties, table_id)
    if kwargs:
        kwargs["namespace_client_managed_versioning"] = managed_versioning
    return kwargs


# `lance.fragment.write_fragments` accepts the same namespace kwargs as `lance.dataset`.
get_write_fragments_kwargs = get_namespace_kwargs


def _storage_options(response: DescribeTableResponse | DeclareTableResponse) -> dict[str, str] | None:
    storage_options = getattr(response, "storage_options", None)
    if storage_options is None:
        return None
    return dict(storage_options)


def _response_location(response: DescribeTableResponse | DeclareTableResponse) -> str:
    location = getattr(response, "location", None) or getattr(response, "table_uri", None)
    if not location:
        raise ValueError("Namespace response did not include a table location.")
    return _normalize_file_uri(str(location))


@dataclass(frozen=True)
class ResolvedNamespaceTable:
    """A namespace table resolved to its physical access and versioning policy."""

    uri: str
    storage_options: dict[str, str] | None = None
    managed_versioning: bool = False


def _resolved_from_response(response: DescribeTableResponse | DeclareTableResponse) -> ResolvedNamespaceTable:
    return ResolvedNamespaceTable(
        uri=_response_location(response),
        storage_options=_storage_options(response),
        managed_versioning=getattr(response, "managed_versioning", None) is True,
    )


def _describe_table(namespace: LanceNamespace, table_id: list[str]) -> DescribeTableResponse:
    from lance_namespace import DescribeTableRequest

    # vend_credentials is explicit: when unset, whether the namespace returns
    # storage credentials is implementation-defined.
    return namespace.describe_table(DescribeTableRequest(id=table_id, vend_credentials=True))


def _declare_table(namespace: LanceNamespace, table_id: list[str]) -> ResolvedNamespaceTable:
    from lance_namespace import DeclareTableRequest

    response = namespace.declare_table(DeclareTableRequest(id=table_id, location=None, vend_credentials=True))
    return _resolved_from_response(response)


def resolve_namespace_table(
    *,
    namespace_impl: str | None,
    namespace_properties: dict[str, str] | None,
    table_id: list[str] | None,
    mode: str = "read",
) -> ResolvedNamespaceTable | None:
    """Resolve a namespace table to a :class:`ResolvedNamespaceTable`.

    ``mode="create"`` atomically declares a new table. ``mode="overwrite"``
    declares the table only when a typed ``TableNotFoundError`` is raised, and
    falls back to a second describe if it loses the declare race; other modes
    require the table to exist.
    """
    namespace = get_or_create_namespace(namespace_impl, namespace_properties)
    if namespace is None or table_id is None:
        return None

    if mode == "create":
        return _declare_table(namespace, table_id)

    from lance_namespace.errors import TableAlreadyExistsError, TableNotFoundError

    try:
        return _resolved_from_response(_describe_table(namespace, table_id))
    except TableNotFoundError:
        if mode != "overwrite":
            raise
        try:
            return _declare_table(namespace, table_id)
        except TableAlreadyExistsError:
            # Lost a declare race: another writer declared the table between our
            # describe and our declare. Overwrite targets whatever exists now, so
            # re-describing is the correct recovery. ``create`` deliberately does
            # not reach here -- for it the conflict is the answer.
            return _resolved_from_response(_describe_table(namespace, table_id))


def merge_storage_options(*layers: dict[str, Any] | None) -> dict[str, Any] | None:
    """Merge storage-option layers; later layers take precedence.

    Callers order layers from lowest to highest priority, conventionally:
    io_config-derived < user-provided ``storage_options`` < namespace-vended.

    Returning ``None`` rather than ``{}`` for an empty result is load-bearing:
    pylance only installs the namespace credential-refresh provider when the
    initial storage options are ``Some`` (``python/src/dataset.rs``). An empty
    dict would install a provider whose initial options carry no
    ``expires_at_millis``, i.e. one that is treated as never expiring and so
    never refreshes.
    """
    merged: dict[str, Any] = {}
    for layer in layers:
        if layer:
            merged.update(layer)
    return merged or None


def pop_namespace_params(open_kwargs: dict[str, Any]) -> tuple[str | None, dict[str, str] | None, list[str] | None]:
    """Remove and return the namespace triple from serialized open arguments."""
    return (
        open_kwargs.pop("namespace_impl", None),
        open_kwargs.pop("namespace_properties", None),
        open_kwargs.pop("table_id", None),
    )


@dataclass(frozen=True)
class DatasetOpenContext:
    """Everything a distributed worker needs to reopen a table, and nothing else.

    Maintenance operations (compaction, scalar index, merge columns) plan on the
    driver and execute on workers. The driver's live ``LanceDataset`` cannot be
    shipped: pickling it drops the namespace client, the table id and the
    managed-versioning flag (see this module's docstring), so a worker would
    commit as if the table were an unmanaged uri table.

    This context is the serializable substitute. It deliberately excludes the
    live dataset, the namespace client, ``serialized_manifest``, ``session``,
    ``commit_lock`` and ``asof``:

    * the client is rebuilt per process from the triple via the ``lru_cache``;
    * the manifest is omitted so the payload stays independent of fragment
      count -- workers pay one pinned-manifest read instead;
    * ``asof``/tags are already resolved by the driver into :attr:`version`, so
      workers cannot drift onto a different snapshot.
    """

    uri: str
    version: int
    storage_options: dict[str, Any] | None = field(default=None, repr=False)

    namespace_impl: str | None = None
    namespace_properties: dict[str, str] | None = None
    table_id: list[str] | None = None
    managed_versioning: bool = False

    block_size: int | None = None
    index_cache_size: int | None = None
    metadata_cache_size_bytes: int | None = None
    default_scan_options: dict[str, Any] | None = field(default=None, repr=False)

    @classmethod
    def from_dataset(
        cls,
        dataset: lance.LanceDataset,
        uri: str,
        *,
        storage_options: dict[str, Any] | None = None,
        namespace_impl: str | None = None,
        namespace_properties: dict[str, str] | None = None,
        table_id: list[str] | None = None,
        managed_versioning: bool = False,
        block_size: int | None = None,
        index_cache_size: int | None = None,
        metadata_cache_size_bytes: int | None = None,
        default_scan_options: dict[str, Any] | None = None,
    ) -> DatasetOpenContext:
        """Derive a context from the snapshot the driver actually opened."""
        return cls(
            uri=uri,
            version=dataset.version,
            storage_options=storage_options,
            namespace_impl=namespace_impl,
            namespace_properties=namespace_properties,
            table_id=table_id,
            managed_versioning=managed_versioning,
            block_size=block_size,
            index_cache_size=index_cache_size,
            metadata_cache_size_bytes=metadata_cache_size_bytes,
            default_scan_options=default_scan_options,
        )

    @property
    def namespace_kwargs(self) -> dict[str, Any]:
        return get_namespace_kwargs(self.namespace_impl, self.namespace_properties, self.table_id)

    @property
    def commit_kwargs(self) -> dict[str, Any]:
        return get_namespace_commit_kwargs(
            self.namespace_impl,
            self.namespace_properties,
            self.table_id,
            self.managed_versioning,
        )

    def _open(self, version: int | None) -> lance.LanceDataset:
        """Open through the low-level constructor, passing the physical uri.

        ``lance.dataset(None, namespace_client=..., table_id=...)`` resolves the
        location with a Python-side ``describe_table`` on *every* call, which
        would put one namespace round-trip on every task. The driver already
        resolved the location, so workers pass the uri directly and the
        constructor still installs the namespace wiring (credential provider and,
        when the catalog manages versioning, the commit handler).
        """
        return lance.LanceDataset(
            self.uri,
            version=version,
            storage_options=self.storage_options,
            block_size=self.block_size,
            index_cache_size=self.index_cache_size,
            metadata_cache_size_bytes=self.metadata_cache_size_bytes,
            default_scan_options=self.default_scan_options,
            namespace_client=get_or_create_namespace(self.namespace_impl, self.namespace_properties),
            table_id=self.table_id,
            namespace_client_managed_versioning=self.managed_versioning,
        )

    def open_pinned(self) -> lance.LanceDataset:
        """Open the exact snapshot the driver planned against.

        Every worker must see the same fragment ids and schema the coordinator
        used to build its plan, so this is the default for worker code.
        """
        return self._open(self.version)

    def open_latest(self) -> lance.LanceDataset:
        """Open the newest version; only for coordinator steps that must observe worker output."""
        return self._open(None)


def open_dataset_from_open_kwargs(ds_uri: str | None, open_kwargs: dict[str, Any] | None) -> lance.LanceDataset:
    """Re-open a dataset on a worker from serialized open arguments.

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
