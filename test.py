from webscraping.marketwatch import scrape_marketwatch_ticker_history
import datetime
from fintools.datareader import get_database
import pandas as pd
import os
from pyutils.selenium_ext.websurfer.chrome import ChromeSurfer

start_date = datetime.date(1975, 1, 1)
tickers = get_database().get_child_node("sovereign_bond_tickers").read_data()

with ChromeSurfer() as websurfer:
    for entity, maturityMonths, ticker in zip(tickers.entity, tickers.maturityMonths, tickers.ticker):
        observation_pdf = scrape_marketwatch_ticker_history(ticker, "bond", start_date, websurfer=websurfer)

        observation_pdf["entity"] = entity
        observation_pdf["maturityMonths"] = maturityMonths
        observation_pdf.to_csv(os.path.join(os.getcwd(), "data_bin", f"{ticker}.csv"))

        break
