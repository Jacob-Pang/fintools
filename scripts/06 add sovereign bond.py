# Libraries and dependencies
import bs4
import fintools
import os
import sys
import wget
import pandas as pd

from fintools.datareader import get_root_database, get_database
from pyutils.database.github_database.github_dataframe import GitHubDataFrame
from pyutils.dependency_tracer import DependencyGraph
from pyutils.dependency_tracer import mainify_dependencies

par_dpath = os.path.dirname(os.path.dirname(__file__))
sys.path.append(par_dpath)

import private_dataseries.sovereign_bond as sovereign_bond

mainify_dependencies(sovereign_bond.SovereignBondDataSeries, dependency_graph=DependencyGraph(bs4, fintools, wget))

# Settings and parameters
bin_dpath = os.path.join(par_dpath, "bin")
access_token = "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"

root_database = get_root_database()
markets_database = get_database("markets_database")

# Create metadata node
sovereign_bond_tickers_node = GitHubDataFrame(
    "sovereign_bond_tickers",
    description="Tickers and metadata for SovereignBondDataSeries.",
    data_source="NA"
)

root_database.add_connected_child_node(sovereign_bond_tickers_node, relative_dpath="metadata")
sovereign_bond_tickers_pdf = pd.read_csv(os.path.join(bin_dpath, "sovereign_bond_tickers.csv"))

sovereign_bond_tickers_node.save_data(sovereign_bond_tickers_pdf, access_token=access_token)

# Generate hist observation dataframe
observation_pdfs = []

for entity, maturityMonths, ticker in zip(sovereign_bond_tickers_pdf.entity, sovereign_bond_tickers_pdf.maturityMonths,
        sovereign_bond_tickers_pdf.ticker):
    observation_pdf = pd.read_csv(os.path.join(bin_dpath, f"{ticker}.csv"), index_col=0)
    observation_pdf["entity"] = entity
    observation_pdf["maturityMonths"] = maturityMonths
    observation_pdf["date"] = pd.to_datetime(observation_pdf["date"], format="%Y-%m-%d")

    observation_pdfs.append(observation_pdf)

observation_pdf = pd.concat(observation_pdfs)

# Creating dataseries node
import __main__
sovereign_bond_node = __main__.SovereignBondDataSeries("sovereign_bond")
markets_database.add_connected_child_node(sovereign_bond_node)

sovereign_bond_node.save_data(observation_pdf, access_token=access_token, partition_columns=["entity"])

# Saving databases
root_database.save_database_memory(access_token=access_token)
markets_database.save_database_memory(access_token=access_token)