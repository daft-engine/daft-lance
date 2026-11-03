from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING

import lance
import pyarrow as pa

import daft

if TYPE_CHECKING:
    from daft_lance.namespace import DatasetOpenContext

logger = logging.getLogger(__name__)

#: One row per selected fragment; ``error`` is null when the fragment is valid.
EMPTY_REPORT = pa.table(
    {
        "version": pa.array([], type=pa.int64()),
        "fragment_id": pa.array([], type=pa.int64()),
        "valid": pa.array([], type=pa.bool_()),
        "error": pa.array([], type=pa.string()),
    }
)


class FragmentValidationUDF:
    """Validate Lance fragments on a Daft worker against a pinned snapshot.

    The dataset is opened once per worker instance, not per fragment, so the
    pinned-manifest read is amortised across the fragments that worker owns.
    """

    def __init__(self, open_context: DatasetOpenContext, version: int) -> None:
        self.open_context = open_context
        self.version = version
        self._lance_ds: lance.LanceDataset | None = None

    def _dataset(self) -> lance.LanceDataset:
        if self._lance_ds is None:
            self._lance_ds = self.open_context.open_pinned()
        return self._lance_ds

    def __call__(self, fragment_id: int) -> str | None:
        try:
            dataset = self._dataset()
        except Exception as exc:
            # A table-open/authentication failure is not evidence of corruption,
            # so report it with a distinct prefix instead of a validation error.
            return f"failed to open the dataset at version {self.version}: {type(exc).__name__}: {exc}"

        try:
            fragment = dataset.get_fragment(int(fragment_id))
            if fragment is None:
                return f"fragment {fragment_id} is not present in pinned version {self.version}"
            fragment.validate()
        except Exception as exc:
            return f"{type(exc).__name__}: {exc}"
        return None


def _selected_fragment_ids(
    available: list[int],
    fragment_ids: Sequence[int] | None,
    version: int,
) -> list[int]:
    if fragment_ids is None:
        return available

    requested = list(dict.fromkeys(fragment_ids))
    if not requested:
        raise ValueError("fragment_ids must not be empty; pass None to validate every fragment")
    available_set = set(available)
    unknown = [fragment_id for fragment_id in requested if fragment_id not in available_set]
    if unknown:
        raise ValueError(
            f"Fragment IDs {unknown} do not exist in dataset version {version}. "
            f"Available fragment IDs: {sorted(available_set)}."
        )
    return requested


def validate_fragments_internal(
    lance_ds: lance.LanceDataset,
    open_context: DatasetOpenContext,
    *,
    fragment_ids: Sequence[int] | None = None,
    concurrency: int | None = None,
) -> daft.DataFrame:
    """Validate selected fragments of the pinned snapshot ``open_context`` describes.

    ``lance_ds`` is the driver's live dataset and is only used to enumerate
    fragment metadata and resolve the version; ``open_context`` is what workers
    reopen from.
    """
    if concurrency is not None and concurrency <= 0:
        raise ValueError(f"concurrency must be positive, got {concurrency}")

    version = lance_ds.version
    # ``get_fragments`` reads fragment metadata from the pinned manifest; it
    # does not scan rows, so fragments with no live rows are still listed.
    available = [fragment.fragment_id for fragment in lance_ds.get_fragments()]
    selected = _selected_fragment_ids(available, fragment_ids, version)
    logger.info("Validating %d of %d fragments at dataset version %d", len(selected), len(available), version)

    if not selected:
        return daft.from_arrow(EMPTY_REPORT)

    df = daft.from_pylist([{"fragment_id": fragment_id} for fragment_id in selected])
    validate_fragment = daft.cls(FragmentValidationUDF, max_concurrency=concurrency)
    validator = validate_fragment(open_context, version)
    df = df.select(df["fragment_id"], validator(df["fragment_id"]).alias("error"))
    return df.select(
        daft.lit(version).alias("version"),
        df["fragment_id"],
        df["error"].is_null().alias("valid"),
        df["error"],
    )
