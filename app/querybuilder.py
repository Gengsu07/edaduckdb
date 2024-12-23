from typing import Any, List, Union

from connection import DatabaseManager


class QueryBuilder:
    def __init__(self, table_name: str, use_limit: str = "default"):
        self.table_name = table_name
        self.conditions = []
        self.params = []
        self.use_limit = use_limit
        self.custom_limit = None
        self.DEFAULT_LIMIT = 100

    def set_custom_limit(self, limit: int):
        """Set a custom limit for the query"""
        self.custom_limit = limit
        self.use_limit = "custom"
        return self

    def add_condition(
        self,
        column_names: Union[str, List[str]],
        column_types: Union[str, List[str]],
        operators: Union[str, List[str]],
        values: Union[Any, List[Any]],
    ) -> "QueryBuilder":
        """
        Add conditions for single or multiple columns.

        Args:
            column_names: Single column name or list of column names
            column_types: Single type or list of column types
            operators: Single operator or list of operators
            values: Single value or list of values

        Example:
            # Single column
            add_condition("age", "integer", ">", 18)

            # Multiple columns
            add_condition(
                ["age", "status", "department"],
                ["integer", "string", "string"],
                [">", "=", "IN"],
                [18, "active", ["IT", "HR"]]
            )
        """
        # Convert single inputs to lists for uniform processing
        if isinstance(column_names, str):
            column_names = [column_names]
            column_types = [column_types]
            operators = [operators]
            values = [values]

        # Validate input lengths match
        if not (
            len(column_names) == len(column_types) == len(operators) == len(values)
        ):
            raise ValueError("All input lists must have the same length")

        # Process each column condition
        for col_name, col_type, operator, value in zip(
            column_names, column_types, operators, values
        ):
            if col_type.lower() == "string":
                if isinstance(value, list):
                    placeholders = ["?" for _ in value]
                    self.conditions.append(f"{col_name} IN ({','.join(placeholders)})")
                    self.params.extend(value)
                else:
                    self.conditions.append(f"{col_name} = ?")
                    self.params.append(value)

            elif col_type.lower() in ["integer", "float", "decimal"]:
                if isinstance(value, (list, tuple)):
                    if len(value) == 2:  # Range query
                        self.conditions.append(f"{col_name} BETWEEN ? AND ?")
                        self.params.extend(value)
                    else:
                        # For multiple values, create individual comparisons
                        numeric_conditions = []
                        for v in value:
                            numeric_conditions.append(f"{col_name} {operator} ?")
                            self.params.append(v)
                        self.conditions.append(f"({' OR '.join(numeric_conditions)})")
                else:
                    self.conditions.append(f"{col_name} {operator} ?")
                    self.params.append(value)

            elif col_type.lower() == "datetime":
                if isinstance(value, (list, tuple)):
                    self.conditions.append(f"{col_name} BETWEEN ? AND ?")
                    self.params.extend(value)
                else:
                    self.conditions.append(f"{col_name} {operator} ?")
                    self.params.append(value)

        return self

    def _add_limit_to_query(self, query: str) -> str:
        """Add appropriate LIMIT clause to query based on settings"""
        if self.use_limit == "default":
            query += f" LIMIT {self.DEFAULT_LIMIT}"
        elif self.use_limit == "custom" and self.custom_limit is not None:
            query += f" LIMIT {self.custom_limit}"
        return query

    def build_select(self, columns: List[str] = None) -> tuple[str, List]:
        cols = "*" if not columns else ", ".join(columns)
        query = f"SELECT {cols} FROM {self.table_name}"

        if self.conditions:
            query += " WHERE " + " AND ".join(self.conditions)

        query = self._add_limit_to_query(query)

        return query, self.params

    def build_count(self) -> tuple[str, List]:
        query = f"SELECT COUNT(*) FROM {self.table_name}"

        if self.conditions:
            query += " WHERE " + " AND ".join(self.conditions)

        return query, self.params

    def clear_conditions(self):
        self.conditions = []
        self.params = []


def db_connection():
    db_manager = DatabaseManager("config.toml")
    conn = db_manager.connection
    return conn


builder = QueryBuilder("db.public.ppmpkm")
builder.add_condition(
    column_names=["KDMAP", "TAHUNBAYAR", "KET"],
    column_types=["string", "integer", "string"],
    operators=["IN", "=", "="],
    values=[["411125", "411126"], 2024, ["MPN", "SPM"]],
)
builder.set_custom_limit(10)
query, params = builder.build_select()

conn = db_connection()

result = conn.execute(query, params).fetchall()
print("\nMultiple conditions:")
print(query, params)
print(result)
