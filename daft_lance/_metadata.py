from __future__ import annotations

from typing import Any

import pyarrow as pa

from daft.datatype import DataType, _ensure_registered_super_ext_type
from daft.schema import Schema

LANCE_BLOB_DESCRIPTOR_TYPE = DataType.struct(
    {
        "kind": DataType.uint8(),
        "position": DataType.uint64(),
        "size": DataType.uint64(),
        "blob_id": DataType.uint32(),
        "blob_uri": DataType.string(),
    }
)


def convert_lance_schema(schema: pa.Schema) -> Schema:
    """Convert a Lance (pyarrow) schema to a Daft Schema.

    Note:
        We must convert the logical ``lance.blob.v2`` type to its
        physical descriptor struct because we never materialize
        blobs when reading. Materialization is handled by the user.
    """
    _ensure_registered_super_ext_type()
    fields: list[tuple[str, DataType]] = []
    for field in schema:
        if _is_lance_blob(field):
            fields.append((field.name, LANCE_BLOB_DESCRIPTOR_TYPE))
        else:
            fields.append((field.name, DataType.from_arrow_type(field.type)))
    return Schema.from_field_name_and_types(fields)


def _is_lance_blob(field: pa.Field[Any]) -> bool:
    md = field.metadata or {}
    if md.get(b"lance-encoding:blob") == b"true":
        return True
    if isinstance(field.type, pa.ExtensionType) and field.type.extension_name == "lance.blob.v2":
        return True
    if md.get(b"ARROW:extension:name") == b"lance.blob.v2":
        return True
    return False
