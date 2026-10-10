from __future__ import annotations

import pathlib
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, Literal

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, ScanOperatorHandle
from daft.dataframe import DataFrame
from daft.dependencies import pa
from daft.io._checkpoint import attach_checkpoint
from daft.logical.builder import LogicalPlanBuilder
from daft.schema import Schema

from .lance_compaction import compact_files_internal
from .lance_data_sink import LanceDataSink
from .lance_merge_column import merge_columns_from_df, merge_columns_internal
from .lance_scalar_index import create_scalar_index_internal, optimize_indices_internal
from .lance_scan import LanceScanOperator
from .lance_update_column import UpdateColumnsResult, update_columns_from_df, validate_update_arguments
from .lance_vector_index import create_vector_index_internal
from .namespace import validate_uri_or_namespace
from .utils import construct_lance_dataset_handle

if TYPE_CHECKING:
    from lance.dataset import LanceDataset
    from lance.udf import BatchUDF

    from daft.checkpoint import CheckpointConfig


@PublicAPI
def read_lance(
    uri: str | pathlib.Path | None = None,
    io_config: IOConfig | None = None,
    version: str | int | None = None,
    asof: str | None = None,
    block_size: int | None = None,
    commit_lock: object | None = None,
    index_cache_size: int | None = None,
    default_scan_options: dict[str, Any] | None = None,
    metadata_cache_size_bytes: int | None = None,
    fragment_group_size: int | None = None,
    include_fragment_id: bool | None = None,
    checkpoint: CheckpointConfig | None = None,
    *,
    table_id: list[str] | None = None,
    namespace_impl: str | None = None,
    namespace_properties: dict[str, str] | None = None,
) -> DataFrame:
    """Create a DataFrame from a Lance dataset.

    Args:
        uri: The URI of the Lance table to read from. Accepts a local path or an
            object-store URI like "s3://bucket/path".
        io_config: A custom IOConfig to use when accessing Lance data. Defaults to None.
        table_id: Table identifier within the namespace, e.g. ["catalog", "schema", "table"].
            Mutually exclusive with ``uri``.
        namespace_impl: Lance Namespace implementation, e.g. "dir" or "rest".
        namespace_properties: Properties for connecting to the namespace, e.g.
            {"root": "/data"} for "dir" or {"uri": "http://host:port"} for "rest".
        version : optional, int | str
            If specified, load a specific version of the Lance dataset. Else, loads the
            latest version. A version number (`int`) or a tag (`str`) can be provided.
        asof : optional, datetime or str
            If specified, find the latest version created on or earlier than the given
            argument value. If a version is already specified, this arg is ignored.
        block_size : optional, int
            Block size in bytes. Provide a hint for the size of the minimal I/O request.
        commit_lock : optional, lance.commit.CommitLock
            A custom commit lock.  Only needed if your object store does not support
            atomic commits.  See the user guide for more details.
        index_cache_size : optional, int
            Index cache size. Index cache is a LRU cache with TTL. This number specifies the
            number of index pages, for example, IVF partitions, to be cached in
            the host memory. Default value is ``256``.

            Roughly, for an ``IVF_PQ`` partition with ``n`` rows, the size of each index
            page equals the combination of the pq code (``np.array([n,pq], dtype=uint8))``
            Approximately, ``n = Total Rows / number of IVF partitions``.
            ``pq = number of PQ sub-vectors``.
        default_scan_options : optional, dict
            Default scan options that are used when scanning the dataset.  This accepts
            the same arguments described in :py:meth:`lance.LanceDataset.scanner`.  The
            arguments will be applied to any scan operation.

            This can be useful to supply defaults for common parameters such as
            ``batch_size``.

            It can also be used to create a view of the dataset that includes meta
            fields such as ``_rowid`` or ``_rowaddr``.  If ``default_scan_options`` is
            provided then the schema returned by :py:meth:`lance.LanceDataset.schema` will
            include these fields if the appropriate scan options are set.
            like this:
            default_scan_options = {"with_row_address": True, "with_row_id" : True,  "batch_size": 1024}
            more see: https://lance-format.github.io/lance-python-doc/dataset.html
        metadata_cache_size_bytes : optional, int
            Size of the metadata cache in bytes. This cache is used to store metadata
            information about the dataset, such as schema and statistics. If not specified,
            a default size will be used.
        fragment_group_size : optional, int
            Number of fragments to group together in a single scan task. If None or <= 1,
            each fragment will be processed individually (default behavior).
        include_fragment_id : Optional, bool
            Whether to display fragment_id.
            Set this to True when preparing input for ``merge_columns_df``.
        checkpoint: Optional :class:`daft.CheckpointConfig` for progress tracking across runs. Bundles the
            checkpoint store, the source key column (``on=``), and optional anti-join tuning. Rows whose key
            already exists in the store are skipped on re-run. Requires the Ray runner.

    Returns:
        DataFrame: a DataFrame with the schema converted from the specified Lance dataset

        This function reads Lance datasets via the Lance Python package.
        To ensure that this is installed with Daft, you may install: `pip install daft[lance]`

    Examples:
        Read a local Lance dataset:
        >>> df = daft.read_lance("/path/to/lance/data/")
        >>> df.show()

        Read a Lance dataset and specify a version:
        >>> df = daft.read_lance("/path/to/lance/data/", version=1)
        >>> df.show()

        Read a Lance dataset with fragment grouping:
        >>> df = daft.read_lance("/path/to/lance/data/", fragment_group_size=5)
        >>> df.show()

        Read a Lance dataset from a public S3 bucket:
        >>> from daft.io import S3Config, IOConfig
        >>> io_config = IOConfig(s3=S3Config(region="us-west-2", anonymous=True))
        >>> df = daft.read_lance("s3://daft-oss-public-data/lance/words-test-dataset", io_config=io_config)
        >>> df.show()
    """
    uri_str = str(uri) if uri is not None else None
    if uri_str is not None and uri_str.startswith("rest://"):
        raise ValueError(
            "rest:// Lance URIs are not supported. To read a table through a REST "
            "Lance Namespace, use the namespace parameters instead of a uri: "
            'read_lance(namespace_impl="rest", '
            'namespace_properties={"uri": "http://host:port"}, '
            'table_id=["catalog", "schema", "table"]). '
            "Otherwise pass a local path or object-store URI."
        )

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    dataset_handle = construct_lance_dataset_handle(
        uri_str,
        io_config=io_config,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        table_id=table_id,
        version=version,
        asof=asof,
        block_size=block_size,
        commit_lock=commit_lock,
        index_cache_size=index_cache_size,
        default_scan_options=default_scan_options,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )

    lance_operator = LanceScanOperator(
        dataset_handle.dataset,
        fragment_group_size=fragment_group_size,
        include_fragment_id=include_fragment_id,
        open_kwargs=dataset_handle.open_kwargs,
        default_scan_options=dataset_handle.default_scan_options,
    )

    handle = ScanOperatorHandle.from_python_scan_operator(lance_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    builder = attach_checkpoint(builder, checkpoint)
    return DataFrame(builder)


@PublicAPI
def merge_columns(
    uri: str | pathlib.Path | None = None,
    io_config: IOConfig | None = None,
    *,
    table_id: list[str] | None = None,
    namespace_impl: str | None = None,
    namespace_properties: dict[str, str] | None = None,
    transform: dict[str, str] | BatchUDF | Callable[[pa.lib.RecordBatch], pa.lib.RecordBatch] | None = None,
    read_columns: list[str] | None = None,
    reader_schema: pa.Schema | None = None,
    storage_options: dict[str, Any] | None = None,
    daft_remote_args: dict[str, Any] | None = None,
    concurrency: int | None = None,
    version: int | str | None = None,
    asof: str | None = None,
    block_size: int | None = None,
    commit_lock: Any | None = None,
    index_cache_size: int | None = None,
    default_scan_options: dict[str, Any] | None = None,
    metadata_cache_size_bytes: int | None = None,
) -> LanceDataset:
    """Merge new columns into a Lance dataset using a transformation function.

    This function modifies the Lance dataset in-place by adding new columns computed
    from existing data using a transformation function. It does not return a DataFrame.

    Args:
        uri: The URI of the Lance table (supports remote URLs to object stores such as `s3://` or `gs://`)
        io_config: A custom IOConfig to use when accessing Lance data. Defaults to None.
        table_id: Table identifier within the namespace, e.g. ["catalog", "schema", "table"].
            Mutually exclusive with ``uri``.
        namespace_impl: Lance Namespace implementation, e.g. "dir" or "rest".
        namespace_properties: Properties for connecting to the namespace, e.g.
            {"root": "/data"} for "dir" or {"uri": "http://host:port"} for "rest".
        transform: A transformation function or UDF to apply to the data.
        read_columns: List of column names to read for the transformation.
        reader_schema: Schema for the reader.
        storage_options: Extra options for storage connection.
        daft_remote_args: Optional arguments for remote execution.
        concurrency: Optional concurrency level for processing.
        version: If specified, load a specific version of the Lance dataset.
        asof: If specified, find the latest version created on or earlier than the given argument value.
        block_size: Block size in bytes. Provide a hint for the size of the minimal I/O request.
        commit_lock: A custom commit lock.
        index_cache_size: Index cache size.
        default_scan_options: Default scan options.
        metadata_cache_size_bytes: Size of the metadata cache in bytes.

    Returns:
        None: This function modifies the table in-place and does not return a value.

    Note:
        This function writes Lance datasets via the Lance Python package.
        To ensure that this is installed with Daft, you may install: `pip install daft[lance]`

    Examples:
        Merge new columns into a local Lance dataset:
        >>> def double_score(batch):
        ...     # Example transformation function
        ...     import pyarrow.compute as pc
        ...
        ...     return batch.append_column("new_column", pc.multiply(batch["c"], 2))
        >>> daft_lance.merge_columns("s3://my-lance-bucket/data/", transform=double_score)
    """
    if transform is None:
        raise ValueError(
            "merge_columns requires a `transform` function; prefer using merge_columns_df with a prepared DataFrame if no transform is needed."
        )

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    # Build Lance dataset handle for committing
    dataset_handle = construct_lance_dataset_handle(
        uri,
        storage_options=storage_options,
        io_config=io_config,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        table_id=table_id,
        version=version,
        asof=asof,
        block_size=block_size,
        commit_lock=commit_lock,
        index_cache_size=index_cache_size,
        default_scan_options=default_scan_options,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )

    return merge_columns_internal(
        dataset_handle.dataset,
        dataset_handle.worker_open_context(),
        transform=transform,
        read_columns=read_columns,
        reader_schema=reader_schema,
        daft_remote_args=daft_remote_args,
        concurrency=concurrency,
    )


@PublicAPI
def merge_columns_df(
    df: DataFrame,
    uri: str | pathlib.Path | None = None,
    io_config: IOConfig | None = None,
    *,
    table_id: list[str] | None = None,
    namespace_impl: str | None = None,
    namespace_properties: dict[str, str] | None = None,
    read_columns: list[str] | None = None,
    reader_schema: pa.Schema | None = None,
    storage_options: dict[str, Any] | None = None,
    daft_remote_args: dict[str, Any] | None = None,
    concurrency: int | None = None,
    version: int | str | None = None,
    asof: str | None = None,
    block_size: int | None = None,
    commit_lock: Any | None = None,
    index_cache_size: int | None = None,
    default_scan_options: dict[str, Any] | None = None,
    metadata_cache_size_bytes: int | None = None,
    batch_size: int | None = None,
    left_on: str | None = "_rowaddr",
    right_on: str | None = "_rowaddr",
) -> Any:
    """Row-level merge columns entrypoint using a DataFrame.

    This function modifies the Lance dataset in-place by merging new columns from a DataFrame
    into existing fragments using a row-level join. It does not return a DataFrame.

    Args:
        df: DataFrame containing the new columns to merge along with fragment_id and join key columns
        uri: URL to the Lance dataset (supports remote URLs to object stores such as `s3://` or `gs://`)
        io_config: A custom IOConfig to use when accessing Lance data. Defaults to None.
        table_id: Table identifier within the namespace, e.g. ["catalog", "schema", "table"].
            Mutually exclusive with ``uri``.
        namespace_impl: Lance Namespace implementation, e.g. "dir" or "rest".
        namespace_properties: Properties for connecting to the namespace, e.g.
            {"root": "/data"} for "dir" or {"uri": "http://host:port"} for "rest".
        read_columns: List of column names to read for the transformation.
        reader_schema: Schema for the reader.
        storage_options: Extra options for storage connection.
        daft_remote_args: Optional arguments for remote execution.
        concurrency: Optional concurrency level for processing.
        version: If specified, load a specific version of the Lance dataset.
        asof: If specified, find the latest version created on or earlier than the given argument value.
        block_size: Block size in bytes. Provide a hint for the size of the minimal I/O request.
        commit_lock: A custom commit lock.
        index_cache_size: Index cache size.
        default_scan_options: Default scan options.
        metadata_cache_size_bytes: Size of the metadata cache in bytes.
        batch_size: Optional batch size when building RecordBatchReader from the provided data.
        left_on: Key column on the Lance fragment. Defaults to "_rowaddr".
        right_on: Key column name present in the provided DataFrame. Defaults to the value of left_on.

    Returns:
        None: This function modifies the table in-place and does not return a value.

    Note:
        This function writes Lance datasets via the Lance Python package.
        To ensure that this is installed with Daft, you may install: `pip install daft[lance]`

    Examples:
        Merge new columns into a local Lance dataset:
        >>> import daft
        >>> # Read the existing table with row addresses
        >>> df = daft.read_lance(
        ...     "s3://my-lance-bucket/data/",
        ...     default_scan_options={"with_row_address": True},
        ...     include_fragment_id=True,
        ... )
        >>> # Add new columns based on existing data
        >>> df = df.with_column("doubled_c", df["c"] * 2)
        >>> # Merge the new columns back to the table
        >>> daft_lance.merge_columns_df(df, "s3://my-lance-bucket/data/")
    """
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    # Build Lance dataset handle for committing
    dataset_handle = construct_lance_dataset_handle(
        uri,
        storage_options=storage_options,
        io_config=io_config,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        table_id=table_id,
        version=version,
        asof=asof,
        block_size=block_size,
        commit_lock=commit_lock,
        index_cache_size=index_cache_size,
        default_scan_options=default_scan_options,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )

    # Set default value for right_on if not provided
    effective_right_on = right_on or left_on
    effective_batch_size = (
        batch_size if batch_size is not None else daft_remote_args.get("batch_size", None) if daft_remote_args else None
    )

    return merge_columns_from_df(
        df,
        lance_ds=dataset_handle.dataset,
        open_context=dataset_handle.worker_open_context(),
        read_columns=read_columns,
        reader_schema=reader_schema,
        daft_remote_args=daft_remote_args,
        concurrency=concurrency,
        left_on=left_on,
        right_on=effective_right_on,
        batch_size=effective_batch_size,
    )


@PublicAPI
def update_columns_df(
    df: DataFrame,
    uri: str | pathlib.Path | None = None,
    io_config: IOConfig | None = None,
    *,
    columns: Sequence[str],
    table_id: list[str] | None = None,
    namespace_impl: str | None = None,
    namespace_properties: dict[str, str] | None = None,
    storage_options: dict[str, Any] | None = None,
    version: int | str | None = None,
    asof: str | None = None,
    block_size: int | None = None,
    commit_lock: Any | None = None,
    index_cache_size: int | None = None,
    default_scan_options: dict[str, Any] | None = None,
    metadata_cache_size_bytes: int | None = None,
    max_concurrency: int | None = None,
) -> UpdateColumnsResult:
    """Overwrite existing Lance columns using values from a Daft DataFrame.

    The source DataFrame must contain ``_rowaddr``, ``fragment_id``, and every
    column named in ``columns``. It is grouped by fragment and rewritten in
    parallel, then all fragment metadata is committed in one atomic Lance
    ``RewriteColumns`` transaction.

    Args:
        df: Prepared update values with ``_rowaddr`` and ``fragment_id`` from the
            target Lance snapshot. Extra columns are ignored.
        uri: URI of the target Lance dataset. Mutually exclusive with namespace
            parameters.
        io_config: Daft object-store configuration.
        columns: Existing top-level columns to overwrite. Values are cast to
            the target Arrow types with ``safe=True``. Arrow does not detect
            every lossy float narrowing conversion, so produce the target float
            type explicitly when precision matters. Struct columns are rejected:
            casting a source struct to the target type fills any field the
            source omits with nulls, which would silently drop data the caller
            did not intend to overwrite.
        table_id: Namespace table identifier.
        namespace_impl: Lance Namespace implementation.
        namespace_properties: Properties used to connect to the namespace.
        storage_options: Additional object-store options.
        version: Dataset version or tag to read and commit against. Defaults to
            the current version. It must be the snapshot the source ``_rowaddr``
            values were read from. Lance rebases the commit over concurrent
            transactions that touch other fragments; a concurrent write to a
            fragment this update rewrites raises ``CommitConflictError``.
        asof: If specified, find the latest version created on or earlier than the given argument value.
        block_size: Block size in bytes. Provide a hint for the size of the minimal I/O request.
        commit_lock: Custom Lance commit lock.
        index_cache_size: Index cache size.
        default_scan_options: Default scan options.
        metadata_cache_size_bytes: Size of the metadata cache in bytes.
        max_concurrency: Maximum number of concurrent fragment-update workers.

    Returns:
        The committed dataset version, and the number of rows the source
        submitted. Row addresses are not checked against the target: the
        rewrite is a left-outer join, so a ``_rowaddr`` that is not a live row
        of the pinned snapshot updates nothing, and a ``_rowaddr`` repeated in
        the source updates its row once with an unspecified one of the
        submitted values. Both are silent and both are still counted. Keep the
        source aligned with ``version``, and unique, to keep the count
        meaningful.

    Note:
        Fragments are rewritten in parallel, so a failure raised by one
        fragment can leave data files other fragments already wrote behind.
        Nothing is committed and the dataset version does not change; the
        unreferenced files stay until Lance cleans them up (see
        ``LanceDataset.cleanup_old_versions``).

    Raises:
        NotImplementedError: If the target dataset uses stable row IDs. The
            current pylance transaction binding cannot propagate the updated
            fragment offsets required for correct CDF metadata.

    Examples:
        >>> import daft
        >>> import daft_lance
        >>> source = daft_lance.read_lance(  # doctest: +SKIP
        ...     "/tmp/events.lance",
        ...     default_scan_options={"with_row_address": True},
        ...     include_fragment_id=True,
        ... )
        >>> source = source.where(daft.col("date") >= "2026-07-01")  # doctest: +SKIP
        >>> source = source.with_column("label", daft.col("value") * 2)  # doctest: +SKIP
        >>> result = daft_lance.update_columns_df(  # doctest: +SKIP
        ...     source.select("_rowaddr", "fragment_id", "label"),
        ...     "/tmp/events.lance",
        ...     columns=["label"],
        ... )
    """
    resolved_columns = validate_update_arguments(columns, max_concurrency)
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    dataset_handle = construct_lance_dataset_handle(
        uri,
        storage_options=storage_options,
        io_config=io_config,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        table_id=table_id,
        version=version,
        asof=asof,
        block_size=block_size,
        commit_lock=commit_lock,
        index_cache_size=index_cache_size,
        default_scan_options=default_scan_options,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )
    if dataset_handle.dataset.has_stable_row_ids:
        raise NotImplementedError(
            "update_columns_df does not support datasets with stable row IDs: "
            "pylance does not yet expose updated fragment offsets, so "
            "_row_last_updated_at_version and CDF metadata cannot be updated correctly."
        )

    return update_columns_from_df(
        df,
        dataset_handle.dataset,
        dataset_handle.worker_open_context(),
        columns=resolved_columns,
        commit_lock=commit_lock,
        max_concurrency=max_concurrency,
    )


@PublicAPI
def create_scalar_index(
    uri: str | pathlib.Path | None = None,
    io_config: IOConfig | None = None,
    *,
    table_id: list[str] | None = None,
    namespace_impl: str | None = None,
    namespace_properties: dict[str, str] | None = None,
    column: str,
    index_type: str = "INVERTED",
    name: str | None = None,
    replace: bool = True,
    storage_options: dict[str, Any] | None = None,
    version: int | str | None = None,
    asof: str | None = None,
    block_size: int | None = None,
    commit_lock: Any | None = None,
    index_cache_size: int | None = None,
    default_scan_options: dict[str, Any] | None = None,
    metadata_cache_size_bytes: int | None = None,
    fragment_group_size: int | None = None,
    num_partitions: int | None = None,
    max_concurrency: int | None = None,
    fragment_ids: list[int] | None = None,
    **kwargs: Any,
) -> None:
    """Build a distributed scalar index using Daft's distributed execution.

    This function distributes the index building process across multiple Daft workers,
    with each worker building independent index segments for a subset of fragments
    that the coordinator commits atomically, recording complete index metadata.

    Args:
        uri: The URI of the Lance table (supports remote URLs to object stores such as `s3://` or `gs://`)
        io_config: A custom IOConfig to use when accessing Lance data. Defaults to None.
        table_id: Table identifier within the namespace, e.g. ["catalog", "schema", "table"].
            Mutually exclusive with ``uri``.
        namespace_impl: Lance Namespace implementation, e.g. "dir" or "rest".
        namespace_properties: Properties for connecting to the namespace, e.g.
            {"root": "/data"} for "dir" or {"uri": "http://host:port"} for "rest".
        column: Column name to index
        index_type: Type of index to build. Built distributed for
            "BITMAP", "BTREE", "INVERTED", "FTS", "ZONEMAP", "NGRAM",
            "LABEL_LIST", and "BLOOMFILTER". Other types raise ``ValueError``;
            for those call pylance directly
            (``lance.dataset(uri).create_scalar_index(...)``). Which column
            types an index accepts is Lance's rule, validated by Lance itself
            rather than restated here.
        name: Name of the index (generated if None).
        replace: Whether to replace an existing index with the same name.
            Defaults to True, matching pylance. Replacement is atomic: the
            coordinator's segment commit retires the old index's overlapped
            segments in the same transaction as the new ones (the index type
            may change on a full rebuild). With ``replace=False``
            an existing index of the same name is rejected — except when
            ``fragment_ids`` is given, which appends coverage for the
            requested fragments (backfill) and never replaces existing
            segments, so the ``replace`` flag does not apply.
        storage_options: Storage options for the dataset.
        version: Version of the dataset to use.
        asof: Timestamp to use for time travel queries.
        block_size: Block size for the index.
        commit_lock: Commit lock for the dataset.
        index_cache_size: Size of the index cache.
        default_scan_options: Default scan options for the dataset.
        metadata_cache_size_bytes: Size of the metadata cache in bytes.
        fragment_group_size: Number of fragments to group together in each fragment batch. If None,
            defaults to 10. Must be a positive integer.
        num_partitions: Optional number of partitions to use when repartitioning fragment batches before execution. Only values
            greater than 1 enable additional parallelism on distributed runners; values <= 1 or None will use the default partitioning.
        max_concurrency: Maximum number of concurrent tasks to use for processing fragment batches.
            If None, Daft will use its default concurrency setting. Must be a positive integer.
        fragment_ids: Optional subset of fragment IDs to index. Only the listed
            fragments are scheduled; fragments already covered by an existing
            same-name, same-column, same-type index are skipped and the
            remaining ones are appended as new segments, preserving committed
            segments untouched (``replace`` does not apply to this append
            path, and a fully covered request is a no-op that leaves the
            dataset version unchanged — pass ``replace=True`` without
            ``fragment_ids`` to rebuild instead). Same-name indexes must keep
            their column and, on backfill, their index type — Lance's build
            and commit APIs reject mismatches. Duplicates are ignored; unknown
            IDs and an empty list raise ``ValueError``.
        **kwargs: Additional keyword arguments forwarded to Lance's index segment creation API.

    Returns:
        None

    Raises:
        ValueError: If input parameters are invalid (e.g., empty column name, non-existent
            column, unsupported index type, or an existing index name with ``replace=False``)
        TypeError: If column type is incompatible with the chosen ``index_type``
        RuntimeError: If index building fails (e.g., version compatibility issues, commit failures)
        ImportError: If lance package is not available

    Note:
        This function writes Lance datasets via the Lance Python package.
        To ensure that this is installed with Daft, you may install: `pip install daft[lance]`

    Examples:
        Create a distributed inverted index with custom concurrency:
        >>> import daft
        >>> daft_lance.create_scalar_index(
        ...     "s3://my-bucket/dataset/", column="text_content", index_type="INVERTED", max_concurrency=8
        ... )

        Create a FTS (Full-Text Search) index:
        >>> daft_lance.create_scalar_index("s3://my-bucket/dataset/", column="document", index_type="FTS")

        Create a BTREE index over a scalar column:
        >>> daft_lance.create_scalar_index(
        ...     "s3://my-bucket/dataset/", column="price", index_type="BTREE", name="price_idx"
        ... )

        Create a distributed ZONEMAP or NGRAM index (newly distributed):
        >>> daft_lance.create_scalar_index("s3://my-bucket/dataset/", column="ts", index_type="ZONEMAP")
        >>> daft_lance.create_scalar_index("s3://my-bucket/dataset/", column="doc", index_type="NGRAM")

        Create an index with custom fragment grouping and partitioning:
        >>> daft_lance.create_scalar_index(
        ...     "s3://my-bucket/dataset/", column="description", fragment_group_size=8, num_partitions=16
        ... )

        Refuse to overwrite an existing index:
        >>> daft_lance.create_scalar_index("s3://my-bucket/dataset/", column="title", replace=False)

        Index only half the fragments now and backfill the rest later:
        >>> daft_lance.create_scalar_index("s3://my-bucket/dataset/", column="title", fragment_ids=[0, 1])
        >>> daft_lance.create_scalar_index("s3://my-bucket/dataset/", column="title", fragment_ids=[2, 3])
    """
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    dataset_handle = construct_lance_dataset_handle(
        uri,
        storage_options=storage_options,
        io_config=io_config,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        table_id=table_id,
        version=version,
        asof=asof,
        block_size=block_size,
        commit_lock=commit_lock,
        index_cache_size=index_cache_size,
        default_scan_options=default_scan_options,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )

    create_scalar_index_internal(
        lance_ds=dataset_handle.dataset,
        open_context=dataset_handle.worker_open_context(),
        column=column,
        index_type=index_type,
        name=name,
        replace=replace,
        fragment_group_size=fragment_group_size,
        num_partitions=num_partitions,
        max_concurrency=max_concurrency,
        fragment_ids=fragment_ids,
        **kwargs,
    )


@PublicAPI
def create_vector_index(
    uri: str | pathlib.Path | None = None,
    io_config: IOConfig | None = None,
    *,
    table_id: list[str] | None = None,
    namespace_impl: str | None = None,
    namespace_properties: dict[str, Any] | None = None,
    column: str,
    index_type: str = "IVF_PQ",
    name: str | None = None,
    replace: bool = False,
    metric: str = "L2",
    num_partitions: int | None = None,
    num_sub_vectors: int | None = None,
    num_bits: int = 8,
    sample_rate: int = 256,
    storage_options: dict[str, Any] | None = None,
    version: int | str | None = None,
    asof: str | None = None,
    block_size: int | None = None,
    commit_lock: Any | None = None,
    index_cache_size: int | None = None,
    default_scan_options: dict[str, Any] | None = None,
    metadata_cache_size_bytes: int | None = None,
    fragment_group_size: int | None = None,
    max_concurrency: int | None = None,
    **kwargs: Any,
) -> None:
    """Build a distributed vector index over every fragment in the opened version.

    The build runs in three phases: the driver trains the global model once
    (IVF centroids via ``IndicesBuilder.train_ivf``, and a PQ codebook via
    ``IndicesBuilder.train_pq`` for PQ variants), fragment batches are
    distributed across Daft workers where each builds one uncommitted index
    segment with Lance's ``create_index_uncommitted`` using the shared model,
    and the coordinator commits all segments atomically with
    ``commit_existing_index_segments``. Segments built against the same
    centroids and codebook commit as one logical index, so no central build
    step is needed.

    Incremental indexing of appended data belongs to ``optimize_indices``.
    This API does not accept ``fragment_ids``, ``ivf_centroids`` or
    ``pq_codebook``. Models are always trained by the driver.

    Args:
        uri: The URI of the Lance table (supports remote URLs to object stores such as `s3://` or `gs://`)
        io_config: A custom IOConfig to use when accessing LanceDB data. Defaults to None.
        table_id: Table identifier within the namespace, e.g. ["catalog", "schema", "table"].
            Mutually exclusive with ``uri``.
        namespace_impl: Lance Namespace implementation, e.g. "dir" or "rest".
        namespace_properties: Properties for connecting to the namespace, e.g.
            {"root": "/data"} for "dir" or {"uri": "http://host:port"} for "rest".
        column: Vector column name to index.
        index_type: Vector index type to build: "IVF_FLAT", "IVF_PQ", "IVF_SQ",
            "IVF_HNSW_FLAT", "IVF_HNSW_PQ", or "IVF_HNSW_SQ". Independently
            built SQ segments may have different quantization metadata;
            ``optimize_indices`` cannot merge segments with incompatible metadata.
        name: Name of the index (generated if None).
        replace: Whether to replace an existing index with the same name. Defaults
            to False, matching ``LanceDataset.create_index``; an existing name is
            refused with ``ValueError`` unless ``replace=True``. Replacement is
            atomic: all new segments commit in one transaction that retires the
            overlapped old segments, so readers see either the old or the new index.
            With replace=False, a concurrent same-name creation fails this build
            instead of being overwritten; retry against the latest version.
            Full replacement also fails if concurrent index maintenance adds
            coverage outside the planned snapshot; retry to include that data.
        metric: Distance metric ("L2", "cosine", or "dot").
        num_partitions: Number of IVF partitions. If None, Lance derives it from
            the number of rows during training. Note this is the IVF partition
            count of the trained model — unlike ``create_scalar_index``, where
            ``num_partitions`` controls Daft's fragment repartitioning; build
            parallelism here comes from ``fragment_group_size``/``max_concurrency``.
        num_sub_vectors: Number of PQ sub-vectors (PQ variants only). If None,
            Lance derives it from the vector dimension during training.
        num_bits: Bits per PQ code, default 8; Lance supports 4 and 8. Configurable
            only for ``IVF_PQ`` and ``IVF_HNSW_PQ``. Passed to shared codebook
            training and every segment build. Lance validates supported configurations.
        sample_rate: Training sampling multiplier, not a percentage. Lance samples
            up to this many rows per IVF partition or PQ centroid. For example,
            4 IVF partitions and sample_rate=32 require 128 rows; 8-bit PQ
            requires 256 * sample_rate rows. The value is passed unchanged to
            Lance, which rejects insufficient training data. Adjust the training
            parameters or provide more data if training fails.
        storage_options: Storage options for the dataset.
        version: Version of the dataset to use.
        asof: Timestamp to use for time travel queries.
        block_size: Block size for the dataset.
        commit_lock: Commit lock for the dataset.
        index_cache_size: Size of the index cache.
        default_scan_options: Default scan options.
        metadata_cache_size_bytes: Size of the metadata cache in bytes.
        fragment_group_size: Number of fragments grouped per worker task. If None,
            defaults to 10. Each group becomes one independently built index segment.
        max_concurrency: Maximum number of concurrent tasks for fragment batches.
            If None, Daft uses its default concurrency setting.
        **kwargs: Additional keyword arguments forwarded to Lance's index segment
            creation API, including HNSW ``m``, ``max_level`` and ``ef_construction``.
            ``index_uuid`` is managed per segment. ``ivf_centroids_file`` is not
            supported; the driver trains the shared models internally.
            Unknown arguments raise ``TypeError`` — Lance's segment build silently
            ignores them, so misspellings must not pass silently here.

    Returns:
        None

    Raises:
        ValueError: If input parameters are invalid (e.g. empty column name,
            non-existent column, unsupported index type, an existing index
            name with ``replace=False``, or insufficient data for the requested
            training parameters — Lance raises its training ``ValueError``)
        TypeError: If column type is incompatible with the chosen ``index_type``,
            or an unknown keyword argument is passed
        RuntimeError: If index building fails (e.g. version compatibility issues, commit failures)

    Note:
        This function writes Lance datasets via the Lance Python package.
        To ensure that this is installed with Daft, you may install: `pip install daft[lance]`

    Examples:
        Build a distributed IVF_PQ index:
        >>> daft_lance.create_vector_index(
        ...     "s3://my-bucket/dataset/", column="vector", index_type="IVF_PQ", num_partitions=16
        ... )

        Build an IVF_FLAT index with cosine distance:
        >>> daft_lance.create_vector_index(
        ...     "s3://my-bucket/dataset/", column="embedding", index_type="IVF_FLAT", metric="cosine"
        ... )

        Refuse to overwrite an existing index:
        >>> daft_lance.create_vector_index("s3://my-bucket/dataset/", column="vector", replace=False)

        Maintain coverage after appending data:
        >>> daft_lance.optimize_indices("s3://my-bucket/dataset/", indices=["vector_idx"])
    """
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    dataset_handle = construct_lance_dataset_handle(
        uri,
        storage_options=storage_options,
        io_config=io_config,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        table_id=table_id,
        version=version,
        asof=asof,
        block_size=block_size,
        commit_lock=commit_lock,
        index_cache_size=index_cache_size,
        default_scan_options=default_scan_options,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )

    create_vector_index_internal(
        lance_ds=dataset_handle.dataset,
        open_context=dataset_handle.worker_open_context(),
        column=column,
        index_type=index_type,
        name=name,
        replace=replace,
        metric=metric,
        num_partitions=num_partitions,
        num_sub_vectors=num_sub_vectors,
        num_bits=num_bits,
        sample_rate=sample_rate,
        fragment_group_size=fragment_group_size,
        max_concurrency=max_concurrency,
        **kwargs,
    )


@PublicAPI
def optimize_indices(
    uri: str | pathlib.Path | None = None,
    io_config: IOConfig | None = None,
    *,
    table_id: list[str] | None = None,
    namespace_impl: str | None = None,
    namespace_properties: dict[str, str] | None = None,
    indices: list[str] | None = None,
    num_indices_to_merge: int | None = None,
    storage_options: dict[str, Any] | None = None,
    block_size: int | None = None,
    commit_lock: Any | None = None,
    index_cache_size: int | None = None,
    default_scan_options: dict[str, Any] | None = None,
    metadata_cache_size_bytes: int | None = None,
) -> LanceDataset:
    """Incrementally optimize existing indexes.

    As data is appended it is not added to existing indexes automatically:
    queries keep working (uncovered fragments fall back to scans) but they
    get slower as the unindexed share grows. This function restores index
    health on the dataset's latest version: newly appended fragments are
    indexed, small segments are merged, and stale fragment IDs left inside
    mixed segments by deletes are healed as part of a commit that indexes
    or merges new data. It commits no new version when there is no new
    data to index and no segments to merge.

    Delegates to pylance's ``DatasetOptimizer.optimize_indices`` — Lance
    core owns the delta-index semantics and parallelizes the underlying
    scans in the coordinator process. A full distributed rebuild is
    ``create_scalar_index(..., replace=True)``.

    Args:
        uri: The URI of the Lance table (supports remote URLs to object stores such as `s3://` or `gs://`)
        io_config: A custom IOConfig to use when accessing Lance data. Defaults to None.
        table_id: Table identifier within the namespace, e.g. ["catalog", "schema", "table"].
            Mutually exclusive with ``uri``.
        namespace_impl: Lance Namespace implementation, e.g. "dir" or "rest".
        namespace_properties: Properties for connecting to the namespace, e.g.
            {"root": "/data"} for "dir" or {"uri": "http://host:port"} for "rest".
        indices: Names of the indexes to optimize. ``None`` (the default)
            optimizes every index on the dataset. Unknown names and an empty
            list raise ``ValueError``.
        num_indices_to_merge: How many segments to merge when compacting an
            index (passed to pylance). ``0`` indexes the new data into a new
            segment instead of merging; ``None`` uses pylance's default.
        storage_options: Storage options for the dataset.
        block_size: Block size for the dataset.
        commit_lock: Commit lock for the dataset.
        index_cache_size: Size of the index cache.
        default_scan_options: Default scan options for the dataset.
        metadata_cache_size_bytes: Size of the metadata cache in bytes.

    Returns:
        The updated dataset (its latest version after the call). Read
        ``.version``, ``describe_indices()`` etc. from it if needed.

    Raises:
        ValueError: If ``indices`` is empty or names indexes that do not
            exist on the dataset.

    Examples:
        >>> import daft_lance
        >>> updated = daft_lance.optimize_indices("s3://my-bucket/dataset/")  # doctest: +SKIP
        >>> updated.version  # doctest: +SKIP
        4

        Optimize one index and merge its small segments:

        >>> daft_lance.optimize_indices(  # doctest: +SKIP
        ...     "s3://my-bucket/dataset/", indices=["name_idx"], num_indices_to_merge=4
        ... )
    """
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    dataset_handle = construct_lance_dataset_handle(
        uri,
        storage_options=storage_options,
        io_config=io_config,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        table_id=table_id,
        block_size=block_size,
        commit_lock=commit_lock,
        index_cache_size=index_cache_size,
        default_scan_options=default_scan_options,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )

    return optimize_indices_internal(
        dataset_handle.dataset,
        dataset_handle.worker_open_context(),
        indices=indices,
        num_indices_to_merge=num_indices_to_merge,
    )


@PublicAPI
def compact_files(
    uri: str | pathlib.Path | None = None,
    io_config: IOConfig | None = None,
    *,
    table_id: list[str] | None = None,
    namespace_impl: str | None = None,
    namespace_properties: dict[str, str] | None = None,
    storage_options: dict[str, Any] | None = None,
    version: int | str | None = None,
    asof: str | None = None,
    block_size: int | None = None,
    commit_lock: Any | None = None,
    index_cache_size: int | None = None,
    default_scan_options: dict[str, Any] | None = None,
    metadata_cache_size_bytes: int | None = None,
    compaction_options: dict[str, Any] | None = None,
    partition_num: int | None = None,
    concurrency: int | None = None,
) -> Any:
    """Compact Lance dataset files using Daft UDF-style distributed execution.

    This function maintains compatibility with the original interface but uses Daft's UDF
    mechanism for distributed execution instead of Ray directly.

    Args:
        uri: The URI of the Lance table (supports remote URLs to object stores such as `s3://` or `gs://`)
        io_config: A custom IOConfig to use when accessing Lance data. Defaults to None.
        table_id: Table identifier within the namespace, e.g. ["catalog", "schema", "table"].
            Mutually exclusive with ``uri``.
        namespace_impl: Lance Namespace implementation, e.g. "dir" or "rest".
        namespace_properties: Properties for connecting to the namespace, e.g.
            {"root": "/data"} for "dir" or {"uri": "http://host:port"} for "rest".
        storage_options: Extra options for storage connection.
        version: If specified, load a specific version of the Lance dataset.
        asof: If specified, find the latest version created on or earlier than the given argument value.
        block_size: Block size in bytes. Provide a hint for the size of the minimal I/O request.
        commit_lock: A custom commit lock.
        index_cache_size: Index cache size.
        default_scan_options: Default scan options.
        metadata_cache_size_bytes: Size of the metadata cache in bytes.
        compaction_options: A dictionary of options to control compaction behavior. See `lance.optimize.CompactionOptions` for details. e.g.
            target_rows_per_fragment: Target number of rows per fragment.
            max_rows_per_group: Max number of rows per group.(default: 1024).
            max_bytes_per_file: Max number of bytes in a single file.
            materialize_deletions: Whether to compact fragments with soft deleted rows so they are no
                longer present in the file.(default: True).
            materialize_deletions_threshold: The fraction of original rows that are soft deleted in a fragment
                before the fragment is a candidate for compaction.(default: 0.1 = 10%).
        partition_num: Number of partitions to use for compaction. Defaults to None.
        concurrency: Number of concurrent compaction tasks to run. Defaults to None.

    Returns:
        CompactionMetrics: Compaction statistics; None if no compaction needed. The `CompactionMetrics` object contains the following fields:
            - `fragments_removed`: Number of fragments removed during compaction.
            - `fragments_added`: Number of fragments added during compaction.
            - `files_removed`: Number of files removed during compaction.
            - `files_added`: Number of files added during compaction.

    Raises:
        RuntimeError: When compaction fails or no successful results
    """
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    dataset_handle = construct_lance_dataset_handle(
        uri,
        storage_options=storage_options,
        io_config=io_config,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        table_id=table_id,
        version=version,
        asof=asof,
        block_size=block_size,
        commit_lock=commit_lock,
        index_cache_size=index_cache_size,
        default_scan_options=default_scan_options,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )

    return compact_files_internal(
        lance_ds=dataset_handle.dataset,
        open_context=dataset_handle.worker_open_context(),
        compaction_options=compaction_options,
        partition_num=partition_num,
        concurrency=concurrency,
    )


@PublicAPI
def write_lance(
    df: DataFrame,
    uri: str | pathlib.Path | None = None,
    mode: Literal["create", "append", "overwrite", "insert_overwrite"] = "create",
    io_config: IOConfig | None = None,
    schema: Schema | pa.Schema | None = None,
    *,
    overwrite_where: str | None = None,
    table_id: list[str] | None = None,
    namespace_impl: str | None = None,
    namespace_properties: dict[str, str] | None = None,
    **kwargs: Any,
) -> DataFrame:
    """Write a DataFrame to a Lance table, addressed by URI or by Lance Namespace.

    Args:
        df: The DataFrame to write.
        uri: The URI of the Lance table. Mutually exclusive with the namespace parameters.
        mode: One of "create", "append", "overwrite", or "insert_overwrite".
            ``"insert_overwrite"`` replaces just the rows matching ``overwrite_where``: one Lance
            commit deletes them from the existing table and adds this DataFrame's data, so
            readers see either the whole replacement or none of it. It requires an existing
            table and is not supported with ``use_mem_wal=True``.
        io_config: A custom IOConfig to use when accessing Lance data.
        schema: Desired schema to enforce during write; defaults to the DataFrame schema.
        overwrite_where: SQL predicate selecting the rows to replace. Required by, and only valid
            with, ``mode="insert_overwrite"``. Uses Lance's SQL filter dialect, e.g.
            ``"dt = DATE '2026-08-25'"``.
        table_id: Table identifier within the namespace, e.g. ["catalog", "schema", "table"].
        namespace_impl: Lance Namespace implementation, e.g. "dir" or "rest".
        namespace_properties: Properties for connecting to the namespace, e.g.
            {"root": "/data"} for "dir" or {"uri": "http://host:port"} for "rest".
        **kwargs: Additional arguments forwarded to the Lance writer.

    Returns:
        DataFrame: write statistics (num_fragments, num_deleted_rows, num_small_files, version).

    Note:
        Target-dependent validation is performed when the returned DataFrame is
        executed (for example, by ``collect()``), not while the logical write
        plan is constructed. This includes missing/duplicate targets, append
        schema compatibility, and storage-version conflicts.

    Examples:
        >>> import daft, daft_lance
        >>> df = daft.from_pydict({"id": [1, 2]})
        >>> daft_lance.write_lance(
        ...     df, namespace_impl="dir", namespace_properties={"root": "/tmp/tables"}, table_id=["t"]
        ... ).collect()  # doctest: +SKIP

        Replace one day's rows and add this batch in a single commit:

        >>> daft_lance.write_lance(
        ...     df, "/tmp/events", mode="insert_overwrite", overwrite_where="dt = DATE '2026-08-25'"
        ... ).collect()  # doctest: +SKIP
    """
    validate_uri_or_namespace(uri, namespace_impl, table_id, namespace_properties)

    if schema is None:
        schema = df.schema()

    sink = LanceDataSink(
        uri,
        schema,
        mode,
        io_config,
        overwrite_where=overwrite_where,
        table_id=table_id,
        namespace_impl=namespace_impl,
        namespace_properties=namespace_properties,
        **kwargs,
    )
    return df.write_sink(sink)
