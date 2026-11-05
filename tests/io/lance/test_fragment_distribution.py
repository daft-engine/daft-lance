from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations

import pytest

from daft_lance.utils import distribute_fragments_balanced


@dataclass
class _Fragment:
    fragment_id: int
    rows: int

    def count_rows(self) -> int:
        return self.rows


def test_equal_fragments_form_contiguous_groups() -> None:
    fragments = [_Fragment(i, 100) for i in range(8)]

    assert distribute_fragments_balanced(fragments, 4) == [
        {"fragment_ids": [0, 1, 2, 3]},
        {"fragment_ids": [4, 5, 6, 7]},
    ]


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [0],
        [0] * 8,
        [0, 10, 0, 0, 20, 0],
        [100, 80, 60, 40, 20],
        [10, 1, 1, 1, 1, 10],
        [2, 3, 8, 1, 6],
        [2**60, 2**60 + 1, 0, 1],
    ],
)
@pytest.mark.parametrize("fragment_group_size", [1, 2, 3, 4, 8, 10])
def test_contiguous_groups_minimize_maximum_rows(rows: list[int], fragment_group_size: int) -> None:
    # Dataset order determines adjacency, not numerical fragment ID order.
    fragments = [_Fragment(100 - i * 3, count) for i, count in enumerate(rows)]
    groups = distribute_fragments_balanced(fragments, fragment_group_size)
    batches = [group["fragment_ids"] for group in groups]

    assert len(batches) == (len(rows) + fragment_group_size - 1) // fragment_group_size
    assert all(batches)
    assert [fid for batch in batches for fid in batch] == [fragment.fragment_id for fragment in fragments]
    assert groups == distribute_fragments_balanced(fragments, fragment_group_size)
    if not rows:
        return

    sizes = {fragment.fragment_id: fragment.rows for fragment in fragments}
    actual = max(sum(sizes[fid] for fid in batch) for batch in batches)
    # Independent oracle: enumerate every legal placement of contiguous cuts.
    optimal = min(
        max(sum(rows[start:end]) for start, end in zip((0, *cuts), (*cuts, len(rows))))
        for cuts in combinations(range(1, len(rows)), len(batches) - 1)
    )
    assert actual == optimal


def test_unknown_fragment_sizes_preserve_order(caplog: pytest.LogCaptureFixture) -> None:
    class UnknownSizeFragment:
        def __init__(self, fragment_id: int) -> None:
            self.fragment_id = fragment_id

        def count_rows(self) -> int:
            raise OSError("row count unavailable")

    fragments = [UnknownSizeFragment(i) for i in [0, 3, 10, 100]]
    assert distribute_fragments_balanced(fragments, 2) == [
        {"fragment_ids": [0, 3]},
        {"fragment_ids": [10, 100]},
    ]
    assert "Using 1 as size estimate" in caplog.text


@pytest.mark.parametrize("fragment_group_size", [0, -1])
def test_group_size_must_be_positive(fragment_group_size: int) -> None:
    with pytest.raises(ValueError, match="fragment_group_size must be a positive integer"):
        distribute_fragments_balanced([_Fragment(0, 100)], fragment_group_size)
