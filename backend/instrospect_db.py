import logging
from typing import Any, Dict, List

from connection import DatabaseManager, InitiateConnection
from sqlalchemy import MetaData, create_engine, inspect


class InstrospectDB:
    """
    A class to introspect database tables and provide information about their structure.
    Supports multiple database types through SQLAlchemy.
    """

    def __init__(self, connection_string: str):
        """
        Initialize the QueryBuilder with a database connection string.

        Args:
            connection_string (str): SQLAlchemy connection string
                e.g., 'postgresql://user:password@localhost:5432/dbname'
                      'mysql://user:password@localhost:3306/dbname'
                      'sqlite:///path/to/database.db'
        """
        self.engine = create_engine(connection_string)
        self.metadata = MetaData()
        self.inspector = inspect(self.engine)
        self.logger = logging.getLogger(__name__)

    def get_all_tables(self) -> List[str]:
        """
        Get a list of all tables in the database.

        Returns:
            List[str]: List of table names
        """
        return self.inspector.get_table_names()

    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get detailed information about all columns in a table.

        Args:
            table_name (str): Name of the table to inspect

        Returns:
            List[Dict[str, Any]]: List of column information dictionaries
        """
        try:
            columns = self.inspector.get_columns(table_name)
            return [
                {
                    "name": col["name"],
                    "type": str(col["type"]),
                    "nullable": col["nullable"],
                    "default": col["default"],
                    "primary_key": col.get("primary_key", False),
                }
                for col in columns
            ]
        except Exception as e:
            self.logger.error(f"Error getting columns for table {table_name}: {str(e)}")
            raise

    def get_primary_keys(self, table_name: str) -> List[str]:
        """
        Get the primary key columns of a table.

        Args:
            table_name (str): Name of the table

        Returns:
            List[str]: List of primary key column names
        """
        try:
            pk_constraint = self.inspector.get_pk_constraint(table_name)
            return pk_constraint["constrained_columns"] if pk_constraint else []
        except Exception as e:
            self.logger.error(
                f"Error getting primary keys for table {table_name}: {str(e)}"
            )
            raise

    def get_foreign_keys(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get all foreign key constraints for a table.

        Args:
            table_name (str): Name of the table

        Returns:
            List[Dict[str, Any]]: List of foreign key information
        """
        try:
            return self.inspector.get_foreign_keys(table_name)
        except Exception as e:
            self.logger.error(
                f"Error getting foreign keys for table {table_name}: {str(e)}"
            )
            raise

    def get_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get all indexes defined on the table.

        Args:
            table_name (str): Name of the table

        Returns:
            List[Dict[str, Any]]: List of index information
        """
        try:
            return self.inspector.get_indexes(table_name)
        except Exception as e:
            self.logger.error(f"Error getting indexes for table {table_name}: {str(e)}")
            raise

    def get_table_definiton(self, table_name: str) -> Dict[str, Any]:
        """
        Get complete schema information for a table.

        Args:
            table_name (str): Name of the table

        Returns:
            Dict[str, Any]: Complete table schema information
        """
        try:
            return {
                "table_name": table_name,
                "columns": self.get_table_columns(table_name),
                "primary_keys": self.get_primary_keys(table_name),
                "foreign_keys": self.get_foreign_keys(table_name),
                "indexes": self.get_indexes(table_name),
            }
        except Exception as e:
            self.logger.error(f"Error getting schema for table {table_name}: {str(e)}")
            raise

    def close(self):
        """
        Close the database connection.
        """
        self.engine.dispose()


if __name__ == "__main__":

    db_manager = DatabaseManager("config.toml")
    connection_string = InitiateConnection(db_manager.db_config).connection_string

    db = InstrospectDB(connection_string)

    columns_data = db.get_table_columns("ppmpkm")

    columns_name = [item["name"] for item in columns_data]
    columns_text = [item["name"] for item in columns_data if item["type"] == "TEXT"]
    print(columns_text)

    db.close()
