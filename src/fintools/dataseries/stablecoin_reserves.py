import pandas as pd

from bs4 import BeautifulSoup
from fintools.dataseries import FintoolsCSVDataSeries
from pyutils.pytask_scheduler import PyTask
from pyutils.selenium_ext.websurfer.chrome import ChromeSurfer

class USDTReservesDataSeries (FintoolsCSVDataSeries):
    def __init__(self, data_node_id: str, connection_dpath: str = '', parent_database: any = None,
        **field_kwargs) -> None:
        super().__init__(
            data_node_id, "https://tether.to/en/transparency/#reports", "daily",
            connection_dpath, "Stablecoin Tether (USDT) reserves breakdown", parent_database,
            **field_kwargs
        )

    def update_pytask(self, access_token: str) -> tuple:
        # Webscrapping caa 19 Jun 2022
        with ChromeSurfer() as websurfer:
            websurfer.get("https://tether.to/en/transparency/#reports")
            websurfer.pause(5)
            soup = BeautifulSoup(websurfer.page_source, "html.parser")

        reserves_breakdown = {}
        main_breakdown = soup.find("div", attrs={"class": "MuiBox-root jss235"})

        for main_component in main_breakdown.find_all("div", recursive=False):
            percentage, asset_category = main_component.text.split("%")
            reserves_breakdown[asset_category] = float(percentage) / 100

        main_component = soup.find("h6", attrs={"class": "MuiTypography-root jss88 MuiTypography-h6"}).text
        main_percentage = reserves_breakdown.pop(main_component)

        sub_breakdown = soup.find("div", attrs={"class": "MuiBox-root jss258"})

        for sub_component in sub_breakdown.find_all("div", recursive=False):
            percentage, asset_category = sub_component.text.split("%")
            reserves_breakdown[asset_category] = float(percentage) / 100 * main_percentage

        with ChromeSurfer() as websurfer:
            websurfer.get("https://tether.to/en/transparency/")    
            websurfer.pause(5)
            soup = BeautifulSoup(websurfer.page_source, "html.parser")

        reserves_value = soup.find("p", attrs={"class": f"MuiTypography-root jss87 MuiTypography-body1"}).text \
                .replace('$', '').replace(',', '')
        reserves_value = float(reserves_value)

        observation_pdf = pd.DataFrame(reserves_breakdown.items(), columns=["assetCategory", "proportion"])
        observation_pdf["proportion"] /= observation_pdf["proportion"].sum()
        observation_pdf["value"] = observation_pdf["proportion"] * reserves_value

        if self.version_timestamp is None: # Assumed no prior saved data
            return self.save_data(observation_pdf, access_token=access_token)

        self.update_data(observation_pdf, access_token=access_token)

    def get_update_pytasks(self) -> list:
        return [PyTask(self.update_pytask, freq=(60 * 60 - 10))]

if __name__ == "__main__":
    pass