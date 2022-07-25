from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from pyutils.selenium_ext.websurfer.chrome import ChromeSurfer
from fintools.datareader.market_variables.cryptocurrency import coingecko_ticker_metadata, coingecko_get_price

with ChromeSurfer() as websurfer:
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
    websurfer.pause(5)

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
    websurfer.pause(5)

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

print(reserves_breakdown)
