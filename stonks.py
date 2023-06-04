import requests
from datetime import datetime
import pandas as pd
import os
from io import StringIO
from typing import List, Optional

API_ENDPOINT = "https://www.alphavantage.co/query"
API_KEY = os.environ.get("ALPHA_VANTAGE_API_KEY")

def get_stock_history(ticker: str, date: str, interval: str = "1min", after_hours: bool=False) -> pd.DataFrame:
    """
    Get stock history for a given date using Alpha Vantage TIME_SERIES_INTRADAY_EXTENDED endpoint.
    
    :param ticker: Stock symbol
    :param date: Date in the form of "MM/YYYY"
    :param interval: Time interval between stock data points. Must be one of "1min", "5min", "15min", "30min", or "60min"
    :param after_hours: Whether to include after hours data
    :return: Stock history dataframe in the form of:
        time, open, high, low, close, volume
        2021-01-04 20:00:00, 129.9900, 129.9900, 129.9900, 129.9900, 100
        ...
    """
    # Check if date is in the correct format
    try:
        date_obj = datetime.strptime(date, "%m/%Y")
    except ValueError:
        raise ValueError("Date must be in the form of 'MM/YYYY'")
    
    # Check if interval is valid
    if interval not in ["1min", "5min", "15min", "30min", "60min"]:
        raise ValueError("Interval must be one of '1min', '5min', '15min', '30min', or '60min'")

    function = "TIME_SERIES_INTRADAY_EXTENDED"
    
    # Check if date is within the last 2 years and is not a future date
    if date_obj > datetime.now() or date_obj < datetime.now().replace(year=datetime.now().year - 2):
        raise ValueError("Date must be within the last 2 years and not a future date")

    # Calculate year and month difference from current date
    current_date = datetime.now()
    year_diff = current_date.year - date_obj.year
    month_diff = current_date.month - date_obj.month

    # Calculate slice string
    slice_value = (year_diff * 12) + month_diff
    slice_year = (slice_value - 1) // 12 + 1
    slice_month = (slice_value - 1) % 12 + 1
    slice_str = f"year{slice_year}month{slice_month}"  
    
    # Make API request
    url = f"{API_ENDPOINT}?function={function}&symbol={ticker}&interval={interval}&slice={slice_str}&apikey={API_KEY}"
    response = requests.get(url)
    content = response.content.decode("utf-8")

    # Load data into dataframe
    df =  pd.read_csv(StringIO(content))

    # Convert columns to appropriate types
    df = format_stock_df(df)

    # Filter out after hours data
    if not after_hours:
        df = df.between_time("9:30", "16:00")
    
    return df

def format_stock_df (stock_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert DF to appropriate types and set index to datetime.

    :param stock_df: Stock history dataframe
    :return: Formatted stock history dataframe
    """
    stock_df.index = pd.to_datetime(stock_df["time"], format="%Y-%m-%d %H:%M:%S")
    stock_df = stock_df.drop(columns=["time"])
    stock_df = stock_df.astype({"open": float, "high": float, "low": float, "close": float, "volume": int})
    return stock_df

def get_stock_sentiment(
    tickers: List[str] = [],
    topics: List[str] = [],
    time_from: Optional[str] = None,
    time_to: Optional[str] = None,
    sort: Optional[str] = None,
    limit: int = 200
) -> dict:
    """
    Get stock sentiment for a stock or topic using Alpha Vantage NEWS_SENTIMENT endpoint.

    :param tickers: List of stocks
    :param topics: List of topics
    :param sort: Sort by "EARLIEST", "LATEST", or "RELEVANCE"
    :param limit: Number of results to return, max 200
    :param time_from: From date in the form of "YYYY-MM-DD"
    :param time_to: To date in the form of "YYYY-MM-DD"
    :return: Sentiment JSON object
    """

    if not tickers and not topics:
        raise ValueError("At least one ticker or topic must be provided")

    VALID_TOPICS = {
        "blockchain",
        "earnings",
        "ipo",
        "mergers_and_acquisitions",
        "financial_markets",
        "economy_fiscal",
        "economy_monetary",
        "economy_macro",
        "finance",
        "life_sciences",
        "manufacturing",
        "real_estate",
        "retail_wholesale",
        "technology"
    }

    function = "NEWS_SENTIMENT"

    if topics:
        if not set(topics).issubset(VALID_TOPICS):
            raise ValueError(f"Topics must be in valid topics: {VALID_TOPICS}")

    if sort and sort not in ["EARLIEST", "LATEST", "RELEVANCE"]:
        raise ValueError("Sort must be 'EARLIEST', 'LATEST', or 'RELEVANCE'")

    if limit > 200:
        raise ValueError("Limit must be less than or equal to 200")

    if time_from:
        time_from = datetime.strptime(time_from, "%Y-%m-%d").strftime("%Y%m%dT%H%M")

    if time_to:
        time_to = datetime.strptime(time_to, "%Y-%m-%d").strftime("%Y%m%dT%H%M")

    topics_str = ",".join(topics)
    tickers_str = ",".join(tickers)

    url = f"{API_ENDPOINT}?function={function}&apikey={API_KEY}"

    if topics:
        url += f"&topics={topics_str}"

    if tickers:
        url += f"&tickers={tickers_str}"

    if sort:
        url += f"&sort={sort}"

    if time_from:
        url += f"&time_from={time_from}"

    if time_to:
        url += f"&time_to={time_to}"

    url += f"&limit={limit}"
    
    response = requests.get(url)
    data = response.json()

    return data
