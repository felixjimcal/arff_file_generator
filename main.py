import datetime
import arff
import talib
import numpy as np
import pandas as pd
from binance.futures import Futures
from XTBApi.api import Client, PERIOD
from utils.secrets import credentials

HIGH = 'high'
LOW = 'low'
TIMESTAMP = 'timestamp'
OPEN = 'open'
CLOSE = 'close'
VOLUME = 'volume'
CTM_STRING = 'ctm_string'

RATE_INFO = 'rateInfos'
time_format = '%d/%m/%Y %H:%M:%S'

if __name__ == "__main__":
    # asset = 'CADJPY'
    # collect_data_from_binance = False
    if False:
        client = Futures(key='<api_key>', secret='<api_secret>')
        ohlc = client.klines(asset, "1h", **{"limit": 1500})  # limit 1500
        ohlc.pop()  # https://binance-docs.github.io/apidocs/futures/en/#kline-candlestick-data
        df_day = pd.DataFrame(ohlc, columns=[TIMESTAMP, OPEN, HIGH, LOW, CLOSE, VOLUME, 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'])
        df_day = df_day.drop(['close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'], axis=1)
        df_day.insert(1, CTM_STRING, np.nan)
        df_day[CTM_STRING] = [str(datetime.datetime.utcfromtimestamp(x / 1000).strftime(time_format)) for x in df_day.timestamp]
        for col in df_day.columns[2:]:
            df_day[col] = pd.to_numeric(df_day[col])
    else:
        client = Client()
        client.login(credentials.XTB_DEMO_ID, credentials.XTB_PASS_KEY, mode='demo')

        asset = 'CADJPY'
        end = datetime.datetime.today().timestamp()
        start = datetime.datetime.timestamp(datetime.datetime.now() - datetime.timedelta(weeks=295))  # DAYS: weeks=1000, HOURS: weeks=295
        chart_day = client.get_chart_range_request(asset, PERIOD.ONE_HOUR.value, start, end, 0)
        digits = int('1' + ('0' * chart_day['digits']))
        for rate in chart_day[RATE_INFO]:
            rate[CLOSE] = (rate[OPEN] + rate[CLOSE]) / digits
            rate[HIGH] = (rate[OPEN] + rate[HIGH]) / digits
            rate[LOW] = (rate[OPEN] + rate[LOW]) / digits
            rate[OPEN] = (rate[OPEN] / digits)
        df_day = pd.DataFrame(chart_day[RATE_INFO])
        df_day = df_day.drop(['ctmString'], axis=1)
        df_day.insert(1, CTM_STRING, np.nan)
        df_day[CTM_STRING] = [str(datetime.datetime.fromtimestamp(x / 1000).strftime(time_format)) for x in df_day.ctm]
        df_day.rename(columns={'vol': VOLUME}, inplace=True)
        # asset += '_XTB'

    # Overlap Studies
    upperband, middleband, lowerband = talib.BBANDS(df_day.close, timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)
    df_day['upperband'] = upperband
    df_day['middleband'] = middleband
    df_day['lowerband'] = lowerband
    df_day['DEMA'] = talib.DEMA(df_day.close, timeperiod=30)
    df_day['EMA'] = talib.EMA(df_day.close, timeperiod=30)
    df_day['HT_TRENDLINE'] = talib.HT_TRENDLINE(df_day.close)
    df_day['KAMA'] = talib.KAMA(df_day.close, timeperiod=30)
    df_day['MA'] = talib.MA(df_day.close, timeperiod=30, matype=0)
    # mama, fama = talib.MAMA(df_day.close, fastlimit=0, slowlimit=0)
    # df_day['mama'] = mama
    # df_day['fama'] = fama
    # df_day['MAVP'] = talib.MAVP(df_day.close, periods, minperiod=2, maxperiod=30, matype=0)
    df_day['MIDPOINT'] = talib.MIDPOINT(df_day.close, timeperiod=14)
    df_day['MIDPRICE'] = talib.MIDPRICE(df_day.high, df_day.low, timeperiod=14)
    df_day['SAR'] = talib.SAR(df_day.high, df_day.low, acceleration=0, maximum=0)
    df_day['SAREXT'] = talib.SAREXT(df_day.high, df_day.low, startvalue=0, offsetonreverse=0, accelerationinitlong=0, accelerationlong=0, accelerationmaxlong=0, accelerationinitshort=0, accelerationshort=0, accelerationmaxshort=0)

    # Momentum Indicators
    df_day['ADX'] = talib.ADX(df_day.high, df_day.low, df_day.close, timeperiod=14)
    df_day['ADXR'] = talib.ADXR(df_day.high, df_day.low, df_day.close, timeperiod=14)
    df_day['APO'] = talib.APO(df_day.close, fastperiod=12, slowperiod=26, matype=0)
    aroondown, aroonup = talib.AROON(df_day.high, df_day.low, timeperiod=14)
    df_day['aroondown'] = aroondown
    df_day['aroonup'] = aroonup
    df_day['AROONOSC'] = talib.AROONOSC(df_day.high, df_day.low, timeperiod=14)
    df_day['BOP'] = talib.BOP(df_day.open, df_day.high, df_day.low, df_day.close)
    df_day['CCI'] = talib.CCI(df_day.high, df_day.low, df_day.close, timeperiod=14)
    df_day['CMO'] = talib.CMO(df_day.close, timeperiod=14)
    df_day['DX'] = talib.DX(df_day.high, df_day.low, df_day.close, timeperiod=14)
    macd, macdsignal, macdhist = talib.MACD(df_day.close, fastperiod=12, slowperiod=26, signalperiod=9)
    df_day['macd'] = macd
    df_day['macdsignal'] = macdsignal
    df_day['macdhist'] = macdhist
    macd_ext, macdsignal_ext, macdhist_ext = talib.MACDEXT(df_day.close, fastperiod=12, fastmatype=0, slowperiod=26, slowmatype=0, signalperiod=9, signalmatype=0)
    df_day['macd_ext'] = macd_ext
    df_day['macdsignal_ext'] = macdsignal_ext
    df_day['macdhist_ext'] = macdhist_ext
    macd_fix, macdsignal_fix, macdhist_fix = talib.MACDFIX(df_day.close, signalperiod=9)
    df_day['macd_fix'] = macd_fix
    df_day['macdsignal_fix'] = macdsignal_fix
    df_day['macdhist_fix'] = macdhist_fix
    df_day['MFI'] = talib.MFI(df_day.high, df_day.low, df_day.close, df_day.volume, timeperiod=14)
    df_day['MINUS_DI'] = talib.MINUS_DI(df_day.high, df_day.low, df_day.close, timeperiod=14)
    df_day['MINUS_DM'] = talib.MINUS_DM(df_day.high, df_day.low, timeperiod=14)
    df_day['MOM'] = talib.MOM(df_day.close, timeperiod=10)
    df_day['PLUS_DI'] = talib.PLUS_DI(df_day.high, df_day.low, df_day.close, timeperiod=14)
    df_day['PLUS_DM'] = talib.PLUS_DM(df_day.high, df_day.low, timeperiod=14)
    df_day['PPO'] = talib.PPO(df_day.close, fastperiod=12, slowperiod=26, matype=0)
    df_day['ROC'] = talib.ROC(df_day.close, timeperiod=10)
    df_day['ROCP'] = talib.ROCP(df_day.close, timeperiod=10)
    df_day['ROCR'] = talib.ROCR(df_day.close, timeperiod=10)
    df_day['ROCR100'] = talib.ROCR100(df_day.close, timeperiod=10)
    df_day['RSI'] = talib.RSI(df_day.close, timeperiod=14)
    slowk, slowd = talib.STOCH(df_day.high, df_day.low, df_day.close, fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
    df_day['slowk'] = slowk
    df_day['slowd'] = slowd
    fastk, fastd = talib.STOCHF(df_day.high, df_day.low, df_day.close, fastk_period=5, fastd_period=3, fastd_matype=0)
    df_day['fastk'] = fastk
    df_day['fastd'] = fastd
    fastk_rsi, fastd_rsi = talib.STOCHRSI(df_day.close, timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)
    df_day['fastk_rsi'] = fastk_rsi
    df_day['fastd_rsi'] = fastd_rsi
    df_day['TRIX'] = talib.TRIX(df_day.close, timeperiod=30)
    df_day['ULTOSC'] = talib.ULTOSC(df_day.high, df_day.low, df_day.close, timeperiod1=7, timeperiod2=14, timeperiod3=28)
    df_day['WILLR'] = talib.WILLR(df_day.high, df_day.low, df_day.close, timeperiod=14)

    # Volume Indicators
    df_day['AD'] = talib.AD(df_day.high, df_day.low, df_day.close, df_day.volume)
    df_day['ADOSC'] = talib.ADOSC(df_day.high, df_day.low, df_day.close, df_day.volume, fastperiod=3, slowperiod=10)
    df_day['OBV'] = talib.OBV(df_day.close, df_day.volume)

    # Volatility Indicators
    df_day['ATR'] = talib.ATR(df_day.high, df_day.low, df_day.close, timeperiod=14)
    df_day['NATR'] = talib.NATR(df_day.high, df_day.low, df_day.close, timeperiod=14)
    df_day['TRANGE'] = talib.TRANGE(df_day.high, df_day.low, df_day.close)

    # Cycle Indicators
    df_day['HT_DCPERIOD'] = talib.HT_DCPERIOD(df_day.close)
    df_day['AD'] = talib.HT_DCPHASE(df_day.close)
    inphase, quadrature = talib.HT_PHASOR(df_day.close)
    df_day['AD'] = inphase
    df_day['AD'] = quadrature
    sine, leadsine = talib.HT_SINE(df_day.close)
    df_day['AD'] = sine
    df_day['AD'] = leadsine
    df_day['HT_TRENDMODE'] = talib.HT_TRENDMODE(df_day.close)

    df_day["BULL"] = np.where(df_day.close.diff() >= 0.02, True, False)

    for col in df_day.columns:
        df_day.dropna(subset=[col], inplace=True)
    arff.dump(asset + '.arff', df_day.values, relation=asset, names=df_day.columns)
