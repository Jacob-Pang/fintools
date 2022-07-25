import datetime
import pandas as pd

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from pyutils.pytask_scheduler import PyTask
from pyutils.selenium_ext.websurfer.chrome import ChromeSurfer
from fintools.dataseries import FintoolsCSVDataSeries
from fintools.datareader.market_variables.cryptocurrency import coingecko_ticker_metadata, coingecko_get_price

class USDTReservesDataSeries (FintoolsCSVDataSeries):
    def __init__(self, data_node_id: str, connection_dpath: str = '', parent_database: any = None,
        **field_kwargs) -> None:
        super().__init__(
            data_node_id, "https://tether.to/en/transparency/#reports", "daily",
            connection_dpath, "Stablecoin Tether (USDT) reserves breakdown", parent_database,
            **field_kwargs
        )

    def update_pytask(self, access_token: str) -> tuple:
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

        observation_pdf = pd.DataFrame(reserves_breakdown.items(), columns=["assetCategory", "proportion"])
        observation_pdf["proportion"] /= observation_pdf["proportion"].sum()
        observation_pdf["value"] = observation_pdf["proportion"] * reserves_value
        observation_pdf["date"] = datetime.date.today().strftime(r"%Y-%m-%d")
        observation_pdf["date"] = pd.to_datetime(observation_pdf["date"], format=r"%Y-%m-%d")

        if self.version_timestamp is None: # Assumed no prior saved data
            return self.save_data(observation_pdf, access_token=access_token)

        self.update_data(observation_pdf, access_token=access_token)

    def drop_duplicates(self, artifact_data: pd.DataFrame, ignore_index: bool = True) -> None:
        # Override drop duplicates method
        artifact_data.drop_duplicates(subset=["date", "assetCategory"], inplace=True,
                ignore_index=ignore_index, keep="last")

    def get_update_pytasks(self) -> list:
        return [PyTask(self.update_pytask, freq=(60 * 60))]

class DAIReservesDataSeries (FintoolsCSVDataSeries):
    def __init__(self, data_node_id: str, connection_dpath: str = '', parent_database: any = None,
        **field_kwargs) -> None:
        super().__init__(
            data_node_id, "https://daistats.com/#/collateral", "daily",
            connection_dpath, "Stablecoin MakerDao (DAI) reserves breakdown", parent_database,
            **field_kwargs
        )

    def update_pytask(self, access_token: str) -> tuple:
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
            return self.save_data(observation_pdf, access_token=access_token)

        self.update_data(observation_pdf, access_token=access_token)

    def drop_duplicates(self, artifact_data: pd.DataFrame, ignore_index: bool = True) -> None:
        # Override drop duplicates method
        artifact_data.drop_duplicates(subset=["date", "assetCategory"], inplace=True,
                ignore_index=ignore_index, keep="last")

    def get_update_pytasks(self) -> list:
        return [PyTask(self.update_pytask, freq=(60 * 60))]

class USDCReservesDataSeries (FintoolsCSVDataSeries):
    pass

if __name__ == "__main__":
    pass