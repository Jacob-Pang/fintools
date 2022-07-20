import requests
import types

def coingecko_ticker_metadata() -> dict:
    return {
        ticker_metadata.get("symbol").upper(): types.SimpleNamespace(
            token_id=ticker_metadata.get("id"),
            name=ticker_metadata.get("name")
        ) for ticker_metadata in requests.get("https://api.coingecko.com/api/v3/coins/list").json()
    }

def coingecko_get_price(ticker: str, metadata: dict = coingecko_ticker_metadata()) -> float:
    token_id = metadata.get(ticker).token_id
    return requests.get(f"https://api.coingecko.com/api/v3/simple/price?ids={token_id}" + \
            "&vs_currencies=USD").json().get(token_id).get("usd")

if __name__ == "__main__":
    pass