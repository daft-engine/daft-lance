from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft_lance.namespace import DatasetOpenContext

import lance

logger = logging.getLogger(__name__)


def drop_index_internal(
    lance_ds: lance.LanceDataset,
    open_context: DatasetOpenContext,
    *,
    name: str,
    if_missing: bool = False,
) -> lance.LanceDataset:
    """Drop one index by name and return the updated dataset (its latest version).

    Delegates to pylance's ``LanceDataset.drop_index`` — dropping an index is
    a single driver-side atomic commit, so unlike index creation there is no
    distributed work. The name is pre-checked here because pylance raises a
    bare ``RuntimeError`` for unknown names: we raise ``ValueError`` listing
    the available indexes instead (same convention as ``optimize_indices``),
    and ``if_missing=True`` turns the missing case into a no-op that commits
    no new version.

    Returns the updated dataset (its latest version after the call).
    """
    known = {idx.name for idx in lance_ds.describe_indices()}
    if name not in known:
        if if_missing:
            logger.info("Index %s does not exist on %s; if_missing=True, nothing to do.", name, open_context.uri)
            return open_context.open_latest()
        raise ValueError(f"index '{name}' does not exist on the dataset. Available index names: {sorted(known)}")

    logger.info("Dropping index: uri=%s, name=%s", open_context.uri, name)
    lance_ds.drop_index(name)
    return open_context.open_latest()
