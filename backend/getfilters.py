import toml


class DataFilter:
    def __init__(self, config):
        self.filters = {}
        self.filters_types = {}
        self.config = self.load_config(config)
        if self.config and "filters" in self.config:
            self.filters = self.config.get("filters", {})

        if self.config and "db" in self.config:
            self.db = self.config.get("db", {})

        if self.config and "filters_types" in self.config:
            self.filters_types = self.config.get("filters_types", {})

    @staticmethod
    def load_config(config_file):
        try:
            with open(config_file, "r") as f:
                return toml.load(f)
        except (FileNotFoundError, toml.TomlDecodeError) as e:
            print(f"Error loading config file: {e}")
            return None

    def getDB(self):
        if not self.db:
            return []

        # Return each filter's values as a list
        return self.db

    def getfilters(self):
        if not self.filters:
            return []

        # Return each filter's values as a list
        return self.filters

    def getfiltersTypes(self):
        if not self.filters_types:
            return []

        # Return each filter's values as a list
        return self.filters_types


if __name__ == "__main__":
    config_file = "config.toml"
    data_filter = DataFilter(config_file)
    filters = data_filter.getfilters()
    filters_types = data_filter.getfiltersTypes()
    print(filters_types)
