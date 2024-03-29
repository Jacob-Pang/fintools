class DataSeriesInterface:
    def __init__(self, data_source: str, frequency: str) -> None:
        self.data_source = data_source
        self.frequency = frequency

    def get_update_resources(self) -> set:
        raise NotImplementedError()

    def get_update_tasks(self, runs: int = 1, **kwargs) -> set:
        raise NotImplementedError()

if __name__ == "__main__":
    pass
