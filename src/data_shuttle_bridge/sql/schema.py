from typing import Iterable, Dict, Type, List, Set

from data_shuttle_bridge.sql.payloads import TableSchema


def build_schema(models: Iterable[Type]) -> Dict[str, TableSchema]:
    schema: Dict[str, TableSchema] = {}
    name_by_table: Dict[str, str] = {}
    for m in models:
        table = getattr(m, "__table__", None)
        table_name = getattr(m, "__tablename__", None) or (
            table.name if table is not None else None
        )
        if not table_name or table is None:
            raise ValueError(f"Model {m} is not mapped to a table")
        name_by_table[table_name] = table_name
    for m in models:
        table = getattr(m, "__table__", None)
        table_name = getattr(m, "__tablename__", None) or table.name
        fields: List[str] = [c.name for c in table.columns]
        parents: Set[str] = set()
        for c in table.columns:
            for fk in c.foreign_keys:
                p_table = fk.column.table.name
                parents.add(p_table)
        schema[table_name] = TableSchema(model=m, fields=fields, parents=parents)
    return schema
