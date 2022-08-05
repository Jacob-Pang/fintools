import time
import datetime
import requests
import pandas as pd

from pyutils.pytask_scheduler import PyTask
from pyutils.pytask_scheduler.request_provider import RequestProvider
from pyutils.database.github_database.github_artifact import GitHubPickleFile
from pyutils.database.github_database.github_dataframe import GitHubGraphDataFrame
from pyutils.database.dataframe_transformer import DataFrameTransformer, GroupByTransformer
from fintools.dataseries import DataSeriesInterface
from fintools.datareader import get_root_database, get_entity_metadata, get_entity_tracker

# Constants
COMTRADE_TRADE_DIRECTION_ID_MAPPER = {1: "imports", 2: "exports", 3: "re-imports", 4: "re-exports"}

class ComtradeGoods (DataSeriesInterface, GitHubGraphDataFrame):
    def __init__(self, data_node_id: str, connection_dpath: str = '', host_database: any = None,
        **field_kwargs) -> None:
        description = "Trade values of imports, exports, re-imports and re-exports in current USD, " + \
                "grouped by reporting country, counterparty and commodity category"
        
        DataSeriesInterface.__init__(self, "UN Comtrade database", "Y")
        GitHubGraphDataFrame.__init__(self, data_node_id, connection_dpath, description,
                host_database, **field_kwargs)

        self.update_tracker = dict()
        self.update_tracker_node = GitHubPickleFile(f"{data_node_id}_update_tracker", connection_dpath,
                "Update tracker for ComtradeGoods", host_database)
    
    def get_data_from_api(self, year: int, trade_direction_id: int, reporter_comtrade_id: int,
        counterparty_comtrade_id: int, reporting_entity: str, counterparty_entity: str) -> pd.DataFrame:
        """
        Parameters:
            year (int): The year of the dataseries.
            trade_flow_id (int): The trade flow ID.
            reporting_entity_id (int): The ComTrade ID of the reporting entity.
            counterparty_id (int): The ComTrade ID of the counterparty.
            trade_flow (str): The description of the trade flow direction.
            reporting_entity (str): The entity code for the reporting entity.
            counterparty_entity (str): The entity code for the counterparty.
        """
        try:
            request_json = requests.get(f"http://comtrade.un.org/api/get?type=C&freq=A&px=HS&ps={year}&" + \
                    f"r={reporter_comtrade_id}&p={counterparty_comtrade_id}&rg={trade_direction_id}&" + \
                    f"cc=AG6&fmt=json").json()
        
            observations_json = request_json.get("dataset")
            observation_pdf = pd.DataFrame([
                    (observation_json.get("pfCode"), observation_json.get("cmdCode"), observation_json.get("cmdDescE"),
                            observation_json.get("TradeValue"))
                    for observation_json in observations_json
                ] if observations_json else [],
                columns=["HSVersion", "commodityCode", "description", "tradeValue"])

            observation_pdf["tradeDirection"] = COMTRADE_TRADE_DIRECTION_ID_MAPPER.get(trade_direction_id)
            observation_pdf["reportingEntity"] = reporting_entity
            observation_pdf["counterpartyEntity"] = counterparty_entity
            observation_pdf["year"] = year
        except Exception as get_exception:
            raise Exception(
                f"Encountered GET request exception from:\nhttp://comtrade.un.org/api/get?type=C&freq=A&px=HS&ps={year}&" + \
                f"r={reporter_comtrade_id}&p={counterparty_comtrade_id}&rg={trade_direction_id}&" + \
                f"cc=AG6&fmt=json\nTrace:{get_exception}"
            )

        time.sleep(.5) # Prevent over-requesting.
        return observation_pdf

    def update_pytask(self, trade_direction_id: int, reporter_comtrade_id: int,
        counterparty_comtrade_id: int, reporting_entity: str, counterparty_entity: str) -> tuple:

        permutation_key = (trade_direction_id, reporter_comtrade_id, counterparty_comtrade_id)

        if not permutation_key in self.update_tracker:
            year = datetime.date.today().year - 10
        elif self.update_tracker.get(permutation_key) == datetime.date.today().year:
            return (False, False) # Updated to most available data
        else:
            year = self.update_tracker.get(permutation_key) + 1
        
        observation_pdf = self.get_data_from_api(year, trade_direction_id, reporter_comtrade_id,
                counterparty_comtrade_id, reporting_entity, counterparty_entity)
        
        if not observation_pdf.shape[0]:
            return (True, True) # Empty dataframe
        
        self.update_tracker[permutation_key] = year

        if self.version_timestamp is None: # Assumed no prior saved data
            self.save_data(observation_pdf, partition_columns=["tradeDirection", "reportingEntity", "year"])
            return (True, True)

        self.update_data(observation_pdf)
        return (True, True)

    def resync_update_tracker(self) -> None:
        self.update_tracker_node.connection_dpath = self.connection_dpath
        self.update_tracker_node.host_database = self.host_database

    def save_data(self, artifact_data: any, **kwargs) -> None:
        super().save_data(artifact_data, **kwargs)
        self.resync_update_tracker()
        self.update_tracker_node.save_data(self.update_tracker, **kwargs)
    
    def update_data(self, artifact_data: any, **kwargs) -> None:
        super().update_data(artifact_data, **kwargs)
        self.resync_update_tracker()
        self.update_tracker_node.save_data(self.update_tracker, **kwargs)

    def get_update_pytasks(self, repeat_tasks: bool = True) -> set:
        self.resync_update_tracker()
        self.update_tracker = self.update_tracker_node.read_data()

        root_database = get_root_database()
        entity_metadata = get_entity_metadata(root_database)
        entity_tracker = get_entity_tracker(root_database)

        reporting_entities = entity_tracker["comtradeGoods"][entity_tracker["comtradeGoods"] == 1].index
        comtrade_entity_id_mapper = entity_metadata["comtradeID"].dropna().to_dict()
        request_provider = RequestProvider([(99, 60 * 60)]) # Maximum of 100 requests per hour.
        request_provider.create_gate() # Create only one API gate
        update_pytasks = []

        for trade_direction_id in COMTRADE_TRADE_DIRECTION_ID_MAPPER:
            for reporting_entity in reporting_entities:
                for counterparty_entity in comtrade_entity_id_mapper:
                    reporter_comtrade_id = comtrade_entity_id_mapper.get(reporting_entity)
                    counterparty_comtrade_id = comtrade_entity_id_mapper.get(counterparty_entity)
                    update_pytasks.append(
                        PyTask( # Generate pytask accepting access_token parameter
                            self.update_pytask,
                            trade_direction_id=int(trade_direction_id),
                            reporter_comtrade_id=int(reporter_comtrade_id),
                            counterparty_comtrade_id=int(counterparty_comtrade_id),
                            reporting_entity=reporting_entity, counterparty_entity=counterparty_entity,
                            max_retries=5, # Buffer for exceptions
                            request_provider_usage={request_provider: 1}
                        )
                    )

        return update_pytasks

"""
class ComtradeAggregator (GroupByTransformer):
    class ComtradeAggregatorPreprocessor (DataFrameTransformer):
        def __init__(self, data_node_id: str, connected_dataframe: DataFrame, aggregate_factor: int,
            connection_dpath: str = None, description: str = None, parent_database: any = None,
            **field_kwargs) -> None:

            super().__init__(data_node_id, connected_dataframe, connection_dpath, description,
                    parent_database, **field_kwargs)
            
            self.aggregate_factor = aggregate_factor

        def read_data(self, *args, **kwargs) -> any:
            comtrade_pdf = super().read_data(*args, **kwargs)
            comtrade_pdf[:, "commodityCode"] = comtrade_pdf["commodityCode"] // self.aggregate_factor

            return comtrade_pdf.drop(columns=["description"])

    def __init__(self, data_node_id: str, connected_dataframe: ComtradeGoodsDataSeries,
        aggregate_factor: int = 2, connection_dpath: str = None, description: str = None,
        parent_database: any = None, **field_kwargs) -> None:

        preprocessor = ComtradeAggregator.ComtradeAggregatorPreprocessor(
            f"{data_node_id}_preprocessor", connected_dataframe, aggregate_factor
        )

        groupby_field_names = ["HSVersion", "commodityCode", "tradeDirection", "reportingEntity",
                "counterpartyEntity", "year"]

        super().__init__(data_node_id, preprocessor, connection_dpath, groupby_field_names,
                description, parent_database=parent_database, **field_kwargs)
    
    def read_data(self, *args, **kwargs) -> any:
        comtrade_pdf = super().read_data(*args, **kwargs)
        comtrade_pdf["description"] = None

        hs_metadata_query = get_root_database().get_child_node("hs_metadata_query")

        for hs_version in comtrade_pdf["HSVersion"].unique():
            hs_metadata = hs_metadata_query.read_data(hs_version)
            description_mapper = hs_metadata.set_index("commodityCode")["description"].to_dict()

            mask = comtrade_pdf["HSVersion"] = hs_version
            comtrade_pdf.loc[mask, "description"] = comtrade_pdf.loc[mask, "commodityCode"] \
                    .map(description_mapper)

        return comtrade_pdf
"""

if __name__ == "__main__":
    pass