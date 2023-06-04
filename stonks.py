import requests
from datetime import datetime
import pandas as pd
import os
from io import StringIO

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
    df.index = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M:%S")
    df = df.drop(columns=["time"])
    df = df.astype({"open": float, "high": float, "low": float, "close": float, "volume": int})

    # Filter out after hours data
    if not after_hours:
        df = df.between_time("9:30", "16:00")
    
    return df