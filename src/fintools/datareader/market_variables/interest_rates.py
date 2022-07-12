import datetime
import ssl
import pandas as pd

from pandas.tseries.offsets import BMonthEnd

# Workaround for web certificate expiry errors
ssl._create_default_https_context = ssl._create_unverified_context

def effective_fed_funds_rate(start: datetime.date = datetime.date(2000, 1, 1),
    end: datetime.date = datetime.date.today()) -> pd.DataFrame:
    """ Returns the daily Effective Fed Funds Rate from <newyorkfed.org>
    Parameters:
        start (datetime.date): The starting date.
        end (datetime.date): The ending date.
    
    Returns:
        pdf (pd.DataFrame, Index: [date], Columns: [effective_fed_funds_rate,
            1st_percentile, 25th_percentile, 75th_percentile, 99th_percentile,
            volume, target_rate_lower_bound, target_rate_upper_bound])
    """
    download_link = "https://markets.newyorkfed.org/read?" \
        + f"startDt={start.strftime(r'%Y-%m-%d')}&" \
        + f"endDt={end.strftime(r'%Y-%m-%d')}&" \
        + "eventCodes=500&productCode=50&sort=postDt:-1,eventCode:1&format=csv"

    column_map = {
        "Effective Date":       "date",
        "Rate (%)":             "effective_fed_funds_rate",
        "1st Percentile (%)":   "1st_percentile",
        "25th Percentile (%)":  "25th_percentile",
        "75th Percentile (%)":  "75th_percentile",
        "99th Percentile (%)":  "99th_percentile",
        "Volume ($Billions)":   "volume",
        "Target Rate From (%)": "target_rate_lower_bound",
        "Target Rate To (%)":   "target_rate_upper_bound"
    }

    pdf = pd.read_csv(download_link).rename(columns=column_map) \
            [column_map.values()]

    pdf["date"] = pd.to_datetime(pdf["date"])

    return pdf.set_index("date")

def wuxia_shadow_fed_funds_rate() -> pd.DataFrame:
    """ Returns the monthly WuXia Shadow Fed Funds Rate from <atlantafed.org>

    Returns:
        pdf (pd.DataFrame, Index: [date], Columns: [wuxia_shadow_fed_funds_rate])
    """
    download_link = "https://www.atlantafed.org/-/media/documents/datafiles/cqer/" + \
            "research/wu-xia-shadow-federal-funds-rate/WuXiaShadowRate.xlsx"

    pdf = pd.read_excel(download_link)
    pdf = pdf.rename(columns={
        pdf.columns[0]: "date",
        "Wu-Xia shadow federal funds rate (last business day of month)":
                "wuxia_shadow_fed_funds_rate"
    })[["date", "wuxia_shadow_fed_funds_rate"]]

    busday_eom = BMonthEnd()
    pdf["date"] = [ busday_eom.rollforward(date) for date in pd.to_datetime(pdf["date"]) ]

    return pdf.set_index("date").dropna()

if __name__ == "__main__":
    pass