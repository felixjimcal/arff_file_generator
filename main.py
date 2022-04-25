import datetime
import arff
import talib
import numpy as np
import pandas as pd
from binance.futures import Futures

HIGH = 'high'
LOW = 'low'
TIMESTAMP = 'timestamp'
OPEN = 'open'
CLOSE = 'close'
VOLUME = 'volume'
CTM_STRING = 'ctm_string'

if __name__ == "__main__":
    asset = 'BTCUSDT'
    client = Futures(key='<api_key>', secret='<api_secret>')
    ohlc = client.klines(asset, "1d", **{"limit": 1500})  # limit 1500
    ohlc.pop()  # https://binance-docs.github.io/apidocs/futures/en/#kline-candlestick-data
    df_day = pd.DataFrame(ohlc, columns=[TIMESTAMP, OPEN, HIGH, LOW, CLOSE, VOLUME, 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'])
    df_day = df_day.drop(['close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'], axis=1)
    df_day.insert(1, CTM_STRING, np.nan)
    df_day[CTM_STRING] = [str(datetime.datetime.utcfromtimestamp(x / 1000).strftime('%d/%m/%Y')) for x in df_day.timestamp]
    for col in df_day.columns[2:]:
        df_day[col] = pd.to_numeric(df_day[col])

    # df_day['ema'] = round(talib.EMA(df_day.close, 20), 4)
    # df_day['RSI'] = round(talib.RSI(df_day.close, 14), 4)
    # df_day['emas'] = np.where(df_day.ema.diff() >= 0.02, True, False)

    arff.dump(asset + '.arff', df_day.values, relation=asset, names=df_day.columns)
