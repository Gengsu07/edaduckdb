import toml


class DataFilter:
    def __init__(self, config):
        self.filters = {}
        self.config = self.load_config(config)
        if self.config and "filters" in self.config:
            self.filters = self.config.get("filters", {})

    @staticmethod
    def load_config(config_file):
        try:
            with open(config_file, "r") as f:
                return toml.load(f)
        except (FileNotFoundError, toml.TomlDecodeError) as e:
            print(f"Error loading config file: {e}")
            return None

    def getfilters(self):
        if not self.filters:
            return []

        # Return each filter's values as a list
        return self.filters


if __name__ == "__main__":
    config_file = "config.toml"
    data_filter = DataFilter(config_file)
    filters = data_filter.getfilters()
    print(filters["KDMAP"])
