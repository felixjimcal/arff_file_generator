import pandas as pd


if __name__ == "__main__":
    # To download data: https://finance.yahoo.com/quote/BTC-USD/history?period1=1410912000&period2=1650844800&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true
    df = pd.read_csv('./data/BTC-USD.csv')
    i = 0