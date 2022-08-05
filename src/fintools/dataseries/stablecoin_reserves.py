import datetime
import pandas as pd

from bs4 import BeautifulSoup
from fintools.dataseries import DataSeriesInterface
from fintools.datareader.market_variables.cryptocurrency import coingecko_ticker_metadata, coingecko_get_price
from pyutils.pytask_scheduler import PyTask
from pyutils.selenium_ext.websurfer.chrome import ChromeSurfer
from pyutils.database.github_database.github_dataframe import GitHubGraphDataFrame
from selenium.webdriver.common.by import By

class CoinReservesInterface (DataSeriesInterface, GitHubGraphDataFrame):
    def __init__(self, data_source: str, data_node_id: str, connection_dpath: str = '',
        host_database: any = None, description: str = None, **field_kwargs) -> None:
        DataSeriesInterface.__init__(self, data_source, "D")
        GitHubGraphDataFrame.__init__(self, data_node_id, connection_dpath, description,
                host_database, **field_kwargs)

    def update_pytask(self) -> tuple:
        raise NotImplementedError()
    
    def merge_function(self, artifact_data: pd.DataFrame, other: pd.DataFrame) -> pd.DataFrame:
        return pd.concat([artifact_data, other], axis=0).drop_duplicates(subset=["date", "asset"],
                ignore_index=True, keep="last")

    def get_update_pytasks(self, repeat_tasks: bool = True) -> set:
        freq = (60 * 60) if repeat_tasks else None
        return [PyTask(self.update_pytask, freq=freq)]

class USDTReserves (CoinReservesInterface):
    def __init__(self, data_node_id: str, connection_dpath: str = '', host_database: any = None,
        **field_kwargs) -> None:
        super().__init__("https://tether.to/en/transparency/#reports", data_node_id, connection_dpath,
                host_database, "Tether (USDT) reserves composition.", **field_kwargs)

    def update_pytask(self) -> tuple:
        # Webscrapping caa 25 Jun 2022
        with ChromeSurfer(*ChromeSurfer.default_headless_option_args()) as websurfer:
            websurfer.get("https://tether.to/en/transparency/#reports")
            websurfer.pause(10)
            soup = BeautifulSoup(websurfer.page_source, "html.parser")

            reserves_breakdown = {}
            main_breakdown, sub_breakdown = soup.find("p", text="Reserves Breakdown").parent \
                    .find("div", recursive=False).find_all("div", recursive=False)[1] \
                    .find_all("div", recursive=False)

            for main_component in main_breakdown.find_all("div", recursive=False):
                percentage, asset_category = main_component.text.split("%")
                reserves_breakdown[asset_category] = float(percentage) / 100

            main_component = sub_breakdown.find("h6", recursive=False).text
            main_percentage = reserves_breakdown.pop(main_component)
            sub_breakdown = sub_breakdown.find("div", recursive=False).find_all("div", recursive=False)[1] \
                    .find("div", recursive=False)

            for sub_component in sub_breakdown.find_all("div", recursive=False):
                percentage, asset_category = sub_component.text.split("%")
                reserves_breakdown[asset_category] = float(percentage) / 100 * main_percentage

            websurfer.get("https://tether.to/en/transparency/")    
            websurfer.pause(5)
            soup = BeautifulSoup(websurfer.page_source, "html.parser")

            reserves_value = soup.find("span", text="USD₮ in Tether").parent \
                    .find("div", recursive=False).find("h4").text \
                    .replace('$', '').replace(',', '')

            reserves_value = float(reserves_value)

        observation_pdf = pd.DataFrame(reserves_breakdown.items(), columns=["asset", "proportion"])
        observation_pdf["proportion"] /= observation_pdf["proportion"].sum()
        observation_pdf["value"] = observation_pdf["proportion"] * reserves_value
        observation_pdf["date"] = datetime.date.today().strftime(r"%Y-%m-%d")
        observation_pdf["date"] = pd.to_datetime(observation_pdf["date"], format=r"%Y-%m-%d")

        if self.version_timestamp is None: # Assumed no prior saved data
            return self.save_data(observation_pdf)

        self.update_data(observation_pdf)

class DAIReserves (CoinReservesInterface):
    def __init__(self, data_node_id: str, connection_dpath: str = '', host_database: any = None,
        **field_kwargs) -> None:
        super().__init__("https://daistats.com/#/collateral", data_node_id, connection_dpath,
                host_database, "MakerDAO (DAI) reserves composition.", **field_kwargs)

    def update_pytask(self) -> tuple:
        # Webscrapping caa 20 Jun 2022
        with ChromeSurfer(*ChromeSurfer.default_headless_option_args()) as websurfer:
            websurfer.get("https://daistats.com/#/collateral")

            while "Overview" not in websurfer.page_source:
                websurfer.pause(1) # Busy waiting for page to render

            reserves_breakdown = dict() # assetCategory: (assetAmount, assetValue)

            # Scrape ERC-20
            # Elements cannot be identified by ID or attributes as they dynamically change with actions.
            soup = BeautifulSoup(websurfer.page_source, "html.parser")
            breakdown_table = soup.find("p", string="ERC-20").parent.parent.parent.find("div")

            for reserves_component in breakdown_table.find_all("div", recursive=False):
                value_stats = reserves_component.find("div").find_all("div", recursive=False)[2]

                description, _, asset_value = value_stats.find_all("p")
                asset_category = description.text.split(" Locked ")[0]
                asset_value = float(asset_value.text.replace("Value Locked: $", '').replace(',', ''))
                asset_amount = float(value_stats.find("h3").text.replace(',', ''))

                if asset_category in reserves_breakdown:
                    prior_asset_amount, prior_asset_value = reserves_breakdown.get(asset_category)
                    reserves_breakdown[asset_category] = (prior_asset_amount + asset_amount,
                            prior_asset_value + asset_value)
                else:
                    reserves_breakdown[asset_category] = (asset_amount, asset_value)

            # Scrape ERC-20 LP
            websurfer.find_element(By.XPATH, "//p[contains(text(), 'ERC-20 LP')]").click()
            websurfer.pause(10)

            soup = BeautifulSoup(websurfer.page_source, "html.parser")
            breakdown_table = soup.find("p", string="ERC-20 LP").parent.parent.parent \
                    .find_all("div", recursive=False)[1]

            for reserves_component in breakdown_table.find_all("div", recursive=False):
                value_stats = reserves_component.find("div").find_all("div", recursive=False)[2]

                description, _, asset_value = value_stats.find_all("p")
                asset_category = description.text.split(" Locked ")[0]
                asset_value = float(asset_value.text.replace("Value Locked: $", '').replace(',', ''))
                asset_amount = float(value_stats.find("h3").text.replace(',', ''))

                if asset_category in reserves_breakdown:
                    prior_asset_amount, prior_asset_value = reserves_breakdown.get(asset_category)
                    reserves_breakdown[asset_category] = (prior_asset_amount + asset_amount,
                            prior_asset_value + asset_value)
                else:
                    reserves_breakdown[asset_category] = (asset_amount, asset_value)

            # Scrape RWA
            websurfer.find_element(By.XPATH, "//p[contains(text(), 'Real World Assets')]").click()
            websurfer.pause(10)

            soup = BeautifulSoup(websurfer.page_source, "html.parser")
            breakdown_table = soup.find("p", string="Real World Assets").parent.parent.parent \
                    .find_all("div", recursive=False)[2]

            for reserves_component in breakdown_table.find_all("div", recursive=False):
                value_stats = reserves_component.find("div").find_all("div", recursive=False)[2]

                description, _, asset_value, _, _ = value_stats.find_all("p")
                asset_category = description.text.split(" Locked ")[0]
                asset_value = float(asset_value.text.replace("Value Locked: $", '').replace(',', ''))
                asset_amount = float(value_stats.find("h3").text.replace(',', ''))

                if asset_category in reserves_breakdown:
                    prior_asset_amount, prior_asset_value = reserves_breakdown.get(asset_category)
                    reserves_breakdown[asset_category] = (prior_asset_amount + asset_amount,
                            prior_asset_value + asset_value)
                else:
                    reserves_breakdown[asset_category] = (asset_amount, asset_value)

            # Scrape PSM
            websurfer.find_element(By.XPATH, "//p[contains(text(), 'Peg Stability Modules')]").click()
            websurfer.pause(10)

            soup = BeautifulSoup(websurfer.page_source, "html.parser")
            breakdown_table = soup.find("p", string="Peg Stability Modules").parent.parent.parent \
                    .find_all("div", recursive=False)[3]

            ticker_metadata = coingecko_ticker_metadata()

            for reserves_component in breakdown_table.find_all("div", recursive=False):
                value_stats = reserves_component.find("div").find_all("div", recursive=False)[2]

                description, _ = value_stats.find_all("p")
                asset_category = description.text.split(" Locked ")[0]
                asset_amount = float(value_stats.find("h3").text.replace(',', ''))
                asset_value = asset_amount * coingecko_get_price(asset_category.upper(), ticker_metadata)

                if asset_category in reserves_breakdown:
                    prior_asset_amount, prior_asset_value = reserves_breakdown.get(asset_category)
                    reserves_breakdown[asset_category] = (prior_asset_amount + asset_amount,
                            prior_asset_value + asset_value)
                else:
                    reserves_breakdown[asset_category] = (asset_amount, asset_value)

        observation_pdf = pd.DataFrame([
            (asset_category, asset_amount, asset_value) for asset_category, (asset_amount, asset_value)
            in reserves_breakdown.items()
        ], columns=["assetCategory", "assetAmount", "value"])

        observation_pdf["date"] = datetime.date.today().strftime(r"%Y-%m-%d")
        observation_pdf["date"] = pd.to_datetime(observation_pdf["date"], format=r"%Y-%m-%d")

        if self.version_timestamp is None: # Assumed no prior saved data
            return self.save_data(observation_pdf)

        self.update_data(observation_pdf)

if __name__ == "__main__":
    pass