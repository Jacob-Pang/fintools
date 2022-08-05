class DataSeriesInterface:
    def __init__(self, data_source: str, frequency: str) -> None:
        self.data_source = data_source
        self.frequency = frequency

    def get_update_pytasks(self, repeat_tasks: bool = True) -> set:
        raise NotImplementedError()

if __name__ == "__main__":
    pass
