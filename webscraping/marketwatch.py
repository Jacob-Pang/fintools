import datetime
import requests
import pandas as pd

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from pyutils.selenium_ext.websurfer import WebsurferBase
from pyutils.selenium_ext.websurfer.chrome import ChromeSurfer

def marketwatch_ticker_page(ticker: str, ticker_category: str) -> str:
    return f"https://www.marketwatch.com/investing/{ticker_category}/{ticker}/download-data"

def scrape_marketwatch_ticker(ticker: str, ticker_category: str, page_source: str = None) -> pd.DataFrame:
    # Returns the ticker values for the most recent 20 business days.
    if not page_source:
        page_source = requests.get(marketwatch_ticker_page(ticker, ticker_category)).text

    soup = BeautifulSoup(page_source, "html.parser")
    ticker_values = []
    table = soup.find("div", attrs={"class": "download-data"}).find("div").find("table")

    for table_row in table.find("tbody").find_all("tr"):
        row_values = [ row_data.find("div").text for row_data in table_row.find_all("td") ]

        # date stamp, percentages
        row_values[0] = datetime.datetime.strptime(row_values[0].strip(), r"%m/%d/%Y")
        row_values[1:] = [ float(row_value.strip().replace('%', '')) for row_value in row_values[1:] ]

        ticker_values.append(row_values)
    
    ticker_values = pd.DataFrame(ticker_values, columns=["date", "open", "high", "low", "close"])
    ticker_values["ticker"] = ticker.upper() # standardize uppercase ticker formatting
    ticker_values["category"] = ticker_category

    # Reorder columns
    return ticker_values[["category", "ticker", "date", "open", "high", "low", "close"]]

def scrape_marketwatch_ticker_history(ticker: str, ticker_category: str, start_date: datetime.date,
    end_date: datetime.date = datetime.date.today(), websurfer: WebsurferBase = None) -> pd.DataFrame:

    if not websurfer:
        with ChromeSurfer() as default_websurfer:
            return scrape_marketwatch_ticker_history(ticker, ticker_category, start_date,
                    end_date, default_websurfer)

    websurfer.get(marketwatch_ticker_page(ticker, ticker_category), ignore_exception=True)
    websurfer.pause(2) # Render page

    # Setting custom date range
    start_date_input = websurfer.find_element(By.XPATH, "//input[@name='startdate']")
    websurfer.execute_script("arguments[0].setAttribute('value', arguments[1])", start_date_input,
            start_date.strftime(r"%m/%d/%Y"))

    end_date_input = websurfer.find_element(By.XPATH, "//input[@name='enddate']")
    websurfer.execute_script("arguments[0].setAttribute('value', arguments[1])", end_date_input,
            end_date.strftime(r"%m/%d/%Y"))

    websurfer.find_element(By.XPATH, "//form[@name='downloaddata-frm']/div/button").send_keys(Keys.RETURN)
    websurfer.pause(2) # Render page
    ticker_history_values = []

    while True:
        main_table = websurfer.find_element(By.XPATH, "//div[@class='tab__pane is-active j-tabPane']")
        ticker_history_values.append(scrape_marketwatch_ticker(ticker, ticker_category, websurfer.page_source))

        soup = BeautifulSoup(websurfer.page_source, "html.parser")
        next_page_link = soup.find("div", attrs={"class": "tab__pane is-active j-tabPane"}) \
                .find("div", attrs={"class": "pagination"}) \
                .find("a", attrs={"class": "link align--right j-next"})

        if not next_page_link: break
        main_table.find_element(By.XPATH, "//div[@class='pagination']/a[text()='Next']") \
                .send_keys(Keys.RETURN)

        websurfer.pause(2) # Render page

    return pd.concat(ticker_history_values)

if __name__ == "__main__":
    pass