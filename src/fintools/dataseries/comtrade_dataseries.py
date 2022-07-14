import time
import datetime
import requests
import pandas as pd

from pyutils.pytask_scheduler import PyTask
from pyutils.pytask_scheduler.request_provider import RequestProvider
from pyutils.database.dataframe import DataFrame
from pyutils.database.dataframe_transformer import DataFrameTransformer, GroupByTransformer
from fintools.dataseries import FintoolsDataSeries
from fintools.datareader import get_root_database, get_entity_metadata, get_entity_tracker

# Constants
COMTRADE_TRADE_DIRECTION_ID_MAPPER = {
    1: "imports",
    2: "exports",
    3: "re-imports",
    4: "re-exports"
}

class ComtradeGoodsDataSeries (FintoolsDataSeries):
    def __init__(self, data_node_id: str, connection_dpath: str = '', parent_database: any = None, **field_kwargs) -> None:
        self.update_tracker = dict()

        super().__init__(data_node_id, "United Nations (UN) Comtrade Database", "Yearly", connection_dpath,
                "Trade values of imports, exports, re-imports and re-exports in current USD, dimensioned by" +
                "reporting country, counterparty country, commodity types.", parent_database, **field_kwargs)

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
        request_json = requests.get(f"http://comtrade.un.org/api/get?type=C&freq=A&px=HS&ps={year}&" + \
                f"r={reporter_comtrade_id}&p={counterparty_comtrade_id}&rg={trade_direction_id}&" + \
                f"cc=AG6&fmt=json").json()

        assert request_json.get("validation").get("status").get("name") == "Ok", "Encountered error at:\n" + \
                f"http://comtrade.un.org/api/get?type=C&freq=A&px=HS&ps={year}&" + \
                f"r={reporter_comtrade_id}&p={counterparty_comtrade_id}&rg={trade_direction_id}&" + \
                f"cc=AG6&fmt=json"
        
        observations_json = request_json.get("dataset")
        observation_pdf = pd.DataFrame([
                (observation_json.get("pfCode"), observation_json.get("cmdCode"), observation_json.get("cmdDescE"),
                        observation_json.get("TradeValue"))
                for observation_json in observations_json
            ], columns=["HSVersion", "commodityCode", "description", "tradeValue"])

        observation_pdf["tradeDirection"] = COMTRADE_TRADE_DIRECTION_ID_MAPPER.get(trade_direction_id)
        observation_pdf["reportingEntity"] = reporting_entity
        observation_pdf["counterpartyEntity"] = counterparty_entity
        observation_pdf["year"] = year

        time.sleep(.5) # Prevent over-requesting.
        return observation_pdf

    def update_pytask(self, access_token: str, trade_direction_id: int, reporter_comtrade_id: int,
        counterparty_comtrade_id: int, reporting_entity: str, counterparty_entity: str) -> None:

        permutation_key = (trade_direction_id, reporter_comtrade_id, counterparty_comtrade_id)

        if not permutation_key in self.update_tracker:
            year = datetime.date.today().year - 10
        elif self.update_tracker.get(permutation_key) == datetime.date.today().year:
            return # Updated to most available data
        else:
            year = self.update_tracker.get(permutation_key) + 1
        
        observation_pdf = self.get_data_from_api(year, trade_direction_id, reporter_comtrade_id,
                counterparty_comtrade_id, reporting_entity, counterparty_entity)
        
        if not observation_pdf.shape[0]:
            return # Empty dataframe
        
        self.update_tracker[permutation_key] = year

        if self.version_timestamp is None: # Assumed no prior saved data
            return self.save_data(observation_pdf, access_token=access_token,
                    partition_columns=["tradeDirection", "reportingEntity", "year"])

        self.update_data(observation_pdf, access_token=access_token)

    def get_update_pytasks(self) -> tuple:
        root_database = get_root_database()
        entity_metadata = get_entity_metadata(root_database)
        entity_tracker = get_entity_tracker(root_database)

        reporting_entities = entity_tracker["comtradeGoods"][entity_tracker["comtradeGoods"] == 1].index
        comtrade_entity_id_mapper = entity_metadata["comtradeID"].dropna().to_dict()
        request_provider = RequestProvider([(99, 60 * 60)]) # Maximum of 100 requests per hour.
        update_pytasks = []

        for trade_direction_id in COMTRADE_TRADE_DIRECTION_ID_MAPPER:
            for reporting_entity in reporting_entities:
                for counterparty_entity in comtrade_entity_id_mapper:
                    reporter_comtrade_id = comtrade_entity_id_mapper.get(reporting_entity)
                    counterparty_comtrade_id = comtrade_entity_id_mapper.get(counterparty_entity)
                    update_pytasks.append(
                        PyTask( # Generate pytask accepting access_token parameter
                            self.update_pytask, trade_direction_id=int(trade_direction_id),
                            reporter_comtrade_id=int(reporter_comtrade_id),
                            counterparty_comtrade_id=int(counterparty_comtrade_id),
                            reporting_entity=reporting_entity, counterparty_entity=counterparty_entity,
                            request_provider_usage={request_provider.unique_id: 1}
                        )
                    )

        return request_provider, update_pytasks

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
        hs_metadata_query = get_root_database().get_child_node("hs_metadata_query")

        return comtrade_pdf

if __name__ == "__main__":
    pass