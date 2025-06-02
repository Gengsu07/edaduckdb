from typing import Optional

import duckdb
import toml


class DatabaseConfig:
    def __init__(self, config):
        self.host = config["db"]["host"]
        self.port = config["db"]["port"]
        self.user = config["db"]["user"]
        self.database = config["db"]["database"]
        self.password = config["db"]["password"]
        self.db_flavour = config["db"]["db_flavour"]


class DBConnectionString:
    @staticmethod
    def postgres_connection_string(host, port, user, database, password):
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"


class DatabaseManager:
    _instance: Optional["DatabaseManager"] = None
    _connection = None

    def __new__(cls, config_file=None):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, config_file=None):
        if self._initialized:
            return

        if config_file is None:
            raise ValueError(
                "config_file is required for initial DatabaseManager creation"
            )

        self.config = self.load_config(config_file)
        self.db_config = DatabaseConfig(self.config)
        self._initialized = True

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self.create_connection()
        return self._connection

    @staticmethod
    def load_config(config_file):
        with open(config_file, "r") as f:
            return toml.load(f)

    def create_connection(self):
        return InitiateConnection(self.db_config).setup_connection()


class InitiateConnection:
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.connection_string = self._create_connection_string()
        self.connection_attach = self._create_connection_attach()

    def _create_connection_string(self):
        if self.db_config.db_flavour.lower() == "postgres":
            return DBConnectionString().postgres_connection_string(
                self.db_config.host,
                self.db_config.port,
                self.db_config.user,
                self.db_config.database,
                self.db_config.password,
            )
        return None

    def connection_string(self):
        return self._create_connection_string()

    def _create_connection_attach(self):
        return f"""host={self.db_config.host} port={self.db_config.port} 
            user={self.db_config.user} dbname={self.db_config.database} 
            password={self.db_config.password} sslmode=disable"""

    def setup_connection(self):
        con = duckdb.connect()
        con.install_extension(f"{self.db_config.db_flavour}")
        con.load_extension(f"{self.db_config.db_flavour}")
        con.install_extension("excel")
        con.load_extension("excel")

        con.execute(
            f"ATTACH '{self.connection_attach}' as db (TYPE {self.db_config.db_flavour})"
        )
        print("ATTACHED TO DB")
        return con


def get_db_connection():
    return DatabaseManager("config.toml").connection


# Now you can query directly

if __name__ == "__main__":
    conn = get_db_connection()
    result = conn.sql("SELECT * FROM db.public.ppmpkm LIMIT 10;").fetchall()
    print(result)
