from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import lance

from daft.dependencies import pa
from daft.io.object_store_options import io_config_to_storage_options
from daft.logical.schema import Schema as DaftSchema
from daft_lance.namespace import (
    get_namespace_commit_kwargs,
    get_namespace_kwargs,
    has_namespace_params,
    merge_storage_options,
    resolve_namespace_table,
    validate_uri_or_namespace,
)

if TYPE_CHECKING:
    import pathlib

    from daft.daft import IOConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LanceDatasetHandle:
    """An opened Lance dataset together with the context needed to reuse it.

    ``lance.LanceDataset`` does not expose all arguments used to open it.  Keep
    those arguments in this Daft-owned value object instead of attaching
    private attributes to the third-party dataset instance.
    """

    dataset: lance.LanceDataset
    uri: str
    open_kwargs: dict[str, Any] = field(repr=False)
    managed_versioning: bool = False
    default_scan_options: dict[str, Any] | None = field(default=None, repr=False)

    @property
    def storage_options(self) -> dict[str, Any] | None:
        options = self.open_kwargs.get("storage_options")
        return options if isinstance(options, dict) else None

    @property
    def namespace_kwargs(self) -> dict[str, Any]:
        return get_namespace_kwargs(
            self.open_kwargs.get("namespace_impl"),
            self.open_kwargs.get("namespace_properties"),
            self.open_kwargs.get("table_id"),
        )

    @property
    def commit_kwargs(self) -> dict[str, Any]:
        return get_namespace_commit_kwargs(
            self.open_kwargs.get("namespace_impl"),
            self.open_kwargs.get("namespace_properties"),
            self.open_kwargs.get("table_id"),
            self.managed_versioning,
        )


def distribute_fragments_balanced(fragments: list[Any], fragment_group_size: int) -> list[dict[str, list[int]]]:
    """Distribute fragments across workers using a balanced algorithm considering fragment sizes."""
    if fragment_group_size <= 0:
        raise ValueError("fragment_group_size must be a positive integer")

    if not fragments:
        return []

    num_groups = max(1, (len(fragments) + fragment_group_size - 1) // fragment_group_size)
    # Get fragment information (ID and size)
    fragment_info = []
    for fragment in fragments:
        try:
            row_count = fragment.count_rows()
            fragment_info.append({"id": fragment.fragment_id, "size": row_count})
        except Exception as e:
            # If we can't get size info, use fragment_id as a fallback
            logger.warning(
                "Could not get size for fragment %s: %s. Using fragment_id as size estimate.",
                fragment.fragment_id,
                e,
            )
            fragment_info.append({"id": fragment.fragment_id, "size": fragment.fragment_id})

    # Sort fragments by size in descending order (largest first)
    # This helps with better load balancing using the greedy algorithm
    fragment_info.sort(key=lambda x: x["size"], reverse=True)

    # Initialize fragment groups for each worker
    fragment_group_list: list[list[int]] = [[] for _ in range(num_groups)]
    group_size_list = [0] * num_groups

    # Greedy assignment: assign each fragment to the worker with minimum workload
    for frag_info in fragment_info:
        # Find the worker with the minimum current workload
        min_workload_idx = min(range(num_groups), key=lambda i: group_size_list[i])

        # Assign fragment to this worker
        fragment_group_list[min_workload_idx].append(frag_info["id"])
        group_size_list[min_workload_idx] += frag_info["size"]

    # Log distribution statistics for debugging
    total_size = sum(frag_info["size"] for frag_info in fragment_info)
    logger.info(
        "Fragment distribution statistics: Total fragments=%d, Total size=%d, Fragment group size=%d, the num of Fragment group=%d",
        len(fragment_info),
        total_size,
        fragment_group_size,
        num_groups,
    )

    for i, (batch, workload) in enumerate(zip(fragment_group_list, group_size_list)):
        percentage = (workload / total_size * 100) if total_size > 0 else 0
        logger.info(
            "Worker %d: %d fragments, workload: %d (%d%%)",
            i,
            len(batch),
            workload,
            percentage,
        )

    # Filter out empty batches (shouldn't happen with proper input validation)
    non_empty_batches = [{"fragment_ids": batch} for batch in fragment_group_list if batch]

    return non_empty_batches


def construct_lance_dataset_handle(
    uri: str | pathlib.Path | None,
    version: int | str | None = None,
    storage_options: dict[str, Any] | None = None,
    io_config: IOConfig | None = None,
    namespace_impl: str | None = None,
    namespace_properties: dict[str, str] | None = None,
    table_id: list[str] | None = None,
    **kwargs: Any,
) -> LanceDatasetHandle:
    """Construct a Lance dataset and retain its reusable open context.

    Storage options are layered from lowest to highest priority:
    io_config-derived < user-provided ``storage_options`` < namespace-vended.
    For a plain ``uri``, user-provided ``storage_options`` replace the
    io_config-derived ones entirely (historical behavior).
    """
    validate_uri_or_namespace(uri, namespace_impl, table_id, namespace_properties)
    resolved_uri = str(uri) if uri is not None else None
    namespace_storage_options = None
    managed_versioning = False
    if resolved_uri is None:
        resolved = resolve_namespace_table(
            namespace_impl=namespace_impl,
            namespace_properties=namespace_properties,
            table_id=table_id,
            mode="read",
        )
        if resolved is not None:
            resolved_uri = resolved.uri
            namespace_storage_options = resolved.storage_options
            managed_versioning = resolved.managed_versioning
    if resolved_uri is None:
        raise ValueError("Unable to resolve Lance dataset URI.")

    io_derived_options = io_config_to_storage_options(io_config, resolved_uri) if io_config is not None else None
    if uri is not None:
        # Falsy check on purpose: entry points historically treated an empty dict
        # like None and fell through to the io_config-derived options.
        base_options = storage_options or io_derived_options
        merged_storage_options = merge_storage_options(base_options, namespace_storage_options)
    else:
        merged_storage_options = merge_storage_options(io_derived_options, storage_options, namespace_storage_options)

    original_default_scan_options = kwargs.pop("default_scan_options", None)
    safe_default_scan_options = None
    if isinstance(original_default_scan_options, dict):
        safe_default_scan_options = {k: v for k, v in original_default_scan_options.items() if k != "nearest"}
        if safe_default_scan_options:
            kwargs["default_scan_options"] = safe_default_scan_options
    elif original_default_scan_options is not None:
        # Non-dict defaults are forwarded as-is.
        kwargs["default_scan_options"] = original_default_scan_options

    dataset_uri = None if has_namespace_params(namespace_impl, table_id) else resolved_uri
    dataset = lance.dataset(
        dataset_uri,
        storage_options=merged_storage_options,
        version=version,
        **get_namespace_kwargs(namespace_impl, namespace_properties, table_id),
        **kwargs,
    )

    effective_kwargs = {
        "storage_options": merged_storage_options,
        "version": version,
        "namespace_impl": namespace_impl,
        "namespace_properties": namespace_properties,
        "table_id": table_id,
    }
    effective_kwargs.update(kwargs or {})
    return LanceDatasetHandle(
        dataset=dataset,
        uri=resolved_uri,
        open_kwargs=effective_kwargs,
        managed_versioning=managed_versioning,
        # Preserve the full user-provided defaults (including nearest) for
        # Daft's planning even if keys were stripped before calling Lance.
        default_scan_options=original_default_scan_options if isinstance(original_default_scan_options, dict) else None,
    )


def construct_lance_dataset(
    uri: str | pathlib.Path | None,
    version: int | str | None = None,
    storage_options: dict[str, Any] | None = None,
    io_config: IOConfig | None = None,
    namespace_impl: str | None = None,
    namespace_properties: dict[str, str] | None = None,
    table_id: list[str] | None = None,
    **kwargs: Any,
) -> lance.LanceDataset:
    """Construct a Lance dataset with common options.

    This compatibility wrapper preserves the historical return type. Internal
    callers that also need the resolved URI or reusable open arguments should
    use :func:`construct_lance_dataset_handle`.
    """
    return construct_lance_dataset_handle(
        uri,
        version=version,
        storage_options=storage_options,
        io_config=io_config,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        table_id=table_id,
        **kwargs,
    ).dataset


def combine_filters_to_arrow(predicates: list[Any] | None) -> pa.compute.Expression | None:
    """Combine a list of Daft PyExpr predicates into a single Arrow Expression.

    Returns None if predicates is empty or None.
    """
    from daft.expressions import Expression

    if not predicates:
        return None
    combined = predicates[0]
    for pred in predicates[1:]:
        combined = combined & pred
    return Expression._from_pyexpr(combined).to_arrow_expr()


def ensure_arrow_schema(obj: Any) -> pa.Schema:
    """Ensure the given object is a PyArrow Schema.

    Accepts pa.Schema directly, or objects with a `.schema` attribute (e.g., pa.Table, pa.RecordBatch).
    Falls back gracefully for Daft Schema objects.
    """
    if isinstance(obj, pa.Schema):
        return obj
    # pa.Table / pa.RecordBatch
    schema = getattr(obj, "schema", None)
    if isinstance(schema, pa.Schema):
        return schema
    # Daft Schema wrapper
    if isinstance(obj, DaftSchema):
        # Daft Schema stores underlying pyarrow schema at `_schema`
        pa_schema = getattr(obj, "_schema", None)
        if isinstance(pa_schema, pa.Schema):
            return pa_schema
    # Last resort: build from fields if present
    fields = getattr(obj, "fields", None)
    if fields is not None:
        try:
            return pa.schema(list(fields))
        except Exception:
            pass
    raise TypeError(f"Cannot ensure PyArrow Schema from object of type {type(obj).__name__}")


def select_required_columns(schema: pa.Schema, required_columns: list[str] | None) -> pa.Schema:
    """Return a new schema filtered to required columns, validating presence.

    If required_columns is None, returns the original schema unchanged.
    """
    if required_columns is None:
        return schema
    missing = [c for c in required_columns if c not in schema.names]
    if missing:
        raise KeyError(f"Required columns missing in schema: {missing}")
    fields = [schema.field(schema.get_field_index(c)) for c in required_columns]
    metadata: dict[bytes | str, bytes | str] | None = (
        {k: v for k, v in schema.metadata.items()} if schema.metadata else None
    )
    return pa.schema(fields, metadata=metadata)
