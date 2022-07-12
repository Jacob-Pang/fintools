from pyutils.database.github_database.github_artifact import GitHubParquetDataFrame

class FintoolsDataSeries (GitHubParquetDataFrame):
    def __init__(self, data_node_id: str, data_source: str, data_periodicity: str, update_frequency: str = None,
        connection_dpath: str = '', description: str = None, parent_database: any = None, **field_kwargs) -> None:

        self.data_source = data_source
        self.data_periodicity = data_periodicity
        self.update_frequency = update_frequency

        GitHubParquetDataFrame.__init__(self, data_node_id, connection_dpath, description,
                parent_database, **field_kwargs)

if __name__ == "__main__":
    pass
