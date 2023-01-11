from typing import IO, Dict, List, Type
import pyreadstat
from tableschema import Table

from datahub.ingestion.source.schema_inference.base import SchemaInferenceBase
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    StringTypeClass,
    TimeTypeClass,
    UnionTypeClass,
)

# see https://github.com/frictionlessdata/tableschema-py/blob/main/tableschema/schema.py#L545
pyreadstat_type_map: Dict[str, Type] = {
    "duration": TimeTypeClass,
    "geojson": RecordTypeClass,
    "geopoint": RecordTypeClass,
    "object": RecordTypeClass,
    "array": ArrayTypeClass,
    "datetime": TimeTypeClass,
    "time": TimeTypeClass,
    "date": DateTypeClass,
    "integer": NumberTypeClass,
    "number": NumberTypeClass,
    "boolean": BooleanTypeClass,
    "string": StringTypeClass,
    "any": UnionTypeClass,
    "double": NumberTypeClass,
}

def test():
    df, meta= pyreadstat.read_xport('adsl.xpt', metadataonly=True)
    meta_map = meta.readstat_variable_types
    fields: List[SchemaField] = []
    for k,v in meta_map.items():
        mapped_type: Type = pyreadstat_type_map.get(v, NullTypeClass)        




def get_table_schema_fields(meta):

    meta_map = meta.readstat_variable_types
    fields: List[SchemaField] = []

    for k,v in meta_map.items():
        mapped_type: Type = pyreadstat_type_map.get(v, NullTypeClass)

        field = SchemaField(
            fieldPath=k,
            type=SchemaFieldDataType(mapped_type()),
            nativeDataType=str(v),
            recursive=False,
        )
        fields.append(field)

    return fields


class XportInferrer(SchemaInferenceBase):
    def __init__(self, max_rows: int):
        self.max_rows = max_rows

    def infer_schema(self, file: IO[bytes]) -> List[SchemaField]:
        # infer schema of a csv file without reading the whole file
        filename= file.name
        df,meta = pyreadstat.read_xport(filename, metadataonly=True)
        return get_table_schema_fields(meta)


class Sas7bdatInferrer(SchemaInferenceBase):
    def __init__(self, max_rows: int):
        self.max_rows = max_rows

    def infer_schema(self, file: IO[bytes]) -> List[SchemaField]:
        # infer schema of a csv file without reading the whole file
        filename=file.name
        df,meta = pyreadstat.read_sas7bdat(file.name, metadataonly=True)
        return get_table_schema_fields(meta)
