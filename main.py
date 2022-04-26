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

if __name__ == "__main__":
    asset = 'BTCUSDT'
    collect_data_from_binance = False
    if collect_data_from_binance:
        client = Futures(key='<api_key>', secret='<api_secret>')
        ohlc = client.klines(asset, "1d", **{"limit": 1500})  # limit 1500
        ohlc.pop()  # https://binance-docs.github.io/apidocs/futures/en/#kline-candlestick-data
        df_day = pd.DataFrame(ohlc, columns=[TIMESTAMP, OPEN, HIGH, LOW, CLOSE, VOLUME, 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'])
        df_day = df_day.drop(['close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'], axis=1)
        df_day.insert(1, CTM_STRING, np.nan)
        df_day[CTM_STRING] = [str(datetime.datetime.utcfromtimestamp(x / 1000).strftime('%d/%m/%Y')) for x in df_day.timestamp]
        for col in df_day.columns[2:]:
            df_day[col] = pd.to_numeric(df_day[col])
    else:
        client = Client()
        client.login(credentials.XTB_DEMO_ID, credentials.XTB_PASS_KEY, mode='demo')

        asset = 'BITCOIN'
        end = datetime.datetime.today().timestamp()
        start = datetime.datetime.timestamp(datetime.datetime.now() - datetime.timedelta(weeks=1000))
        chart_day = client.get_chart_range_request(asset, PERIOD.ONE_DAY.value, start, end, 0)
        digits = int('1' + ('0' * chart_day['digits']))
        for rate in chart_day[RATE_INFO]:
            rate[CLOSE] = (rate[OPEN] + rate[CLOSE]) / digits
            rate[HIGH] = (rate[OPEN] + rate[HIGH]) / digits
            rate[LOW] = (rate[OPEN] + rate[LOW]) / digits
            rate[OPEN] = (rate[OPEN] / digits)
        df_day = pd.DataFrame(chart_day[RATE_INFO])
        df_day = df_day.drop(['ctmString'], axis=1)
        df_day.insert(1, CTM_STRING, np.nan)
        df_day[CTM_STRING] = [str(datetime.datetime.fromtimestamp(x / 1000).strftime('%d/%m/%Y')) for x in df_day.ctm]
        df_day.rename(columns={'vol': VOLUME}, inplace=True)
        asset += '_XTB'

    round_digits = 2
    df_day['ema'] = round(talib.EMA(df_day.close, 20), round_digits)
    df_day['RSI'] = round(talib.RSI(df_day.close, 14), round_digits)
    df_day['AD'] = round(talib.AD(df_day.high, df_day.low, df_day.close, df_day.volume), round_digits)
    df_day['ADOSC'] = round(talib.ADOSC(df_day.high, df_day.low, df_day.close, df_day.volume, fastperiod=3, slowperiod=10), round_digits)
    df_day['ADX'] = round(talib.ADX(df_day.high, df_day.low, df_day.close, timeperiod=14), round_digits)
    df_day['ADXR'] = round(talib.ADXR(df_day.high, df_day.low, df_day.close, timeperiod=14), round_digits)
    df_day['APO'] = round(talib.APO(df_day.close, fastperiod=12, slowperiod=26, matype=0), round_digits)
    # aroondown, aroonup = talib.AROON(df_day.high, df_day.low, timeperiod=14)
    # df_day['AROON'] = round(talib.AROON(), rounds_digits)
    # df_day['AROONOSC'] = round(talib.AROONOSC(), rounds_digits)
    # df_day['ATR'] = round(talib.ATR(), rounds_digits)
    # df_day['AVGPRICE'] = round(talib.AVGPRICE(), rounds_digits)
    # df_day['BBANDS'] = round(talib.BBANDS(), rounds_digits)
    # df_day['BETA'] = round(talib.BETA(), rounds_digits)
    # df_day['BOP'] = round(talib.BOP(), rounds_digits)
    # df_day['CCI'] = round(talib.CCI(), rounds_digits)
    # df_day['CDL2CROWS'] = round(talib.CDL2CROWS(), rounds_digits)
    # df_day['CDL3BLACKCROWS'] = round(talib.CDL3BLACKCROWS(), rounds_digits)
    # df_day['CDL3INSIDE'] = round(talib.CDL3INSIDE(), rounds_digits)
    # df_day['CDL3LINESTRIKE'] = round(talib.CDL3LINESTRIKE(), rounds_digits)
    # df_day['CDL3OUTSIDE'] = round(talib.CDL3OUTSIDE(), rounds_digits)
    # df_day['CDL3STARSINSOUTH'] = round(talib.CDL3STARSINSOUTH(), rounds_digits)
    # df_day['CDL3WHITESOLDIERS'] = round(talib.CDL3WHITESOLDIERS(), rounds_digits)
    # df_day['CDLABANDONEDBABY'] = round(talib.CDLABANDONEDBABY(), rounds_digits)
    # df_day['CDLADVANCEBLOCK'] = round(talib.CDLADVANCEBLOCK(), rounds_digits)
    # df_day['CDLBELTHOLD'] = round(talib.CDLBELTHOLD(), rounds_digits)
    # df_day['CDLBREAKAWAY'] = round(talib.CDLBREAKAWAY(), rounds_digits)
    # df_day['CDLCLOSINGMARUBOZU'] = round(talib.CDLCLOSINGMARUBOZU(), rounds_digits)
    # df_day['CDLCONCEALBABYSWALL'] = round(talib.CDLCONCEALBABYSWALL(), rounds_digits)
    # df_day['CDLCOUNTERATTACK'] = round(talib.CDLCOUNTERATTACK(), rounds_digits)
    # df_day['CDLDARKCLOUDCOVER'] = round(talib.CDLDARKCLOUDCOVER(), rounds_digits)
    # df_day['CDLDOJI'] = round(talib.CDLDOJI(), rounds_digits)
    # df_day['CDLDOJISTAR'] = round(talib.CDLDOJISTAR(), rounds_digits)
    # df_day['CDLDRAGONFLYDOJI'] = round(talib.CDLDRAGONFLYDOJI(), rounds_digits)
    # df_day['CDLENGULFING'] = round(talib.CDLENGULFING(), rounds_digits)
    # df_day['CDLEVENINGDOJISTAR'] = round(talib.CDLEVENINGDOJISTAR(), rounds_digits)
    # df_day['CDLEVENINGSTAR'] = round(talib.CDLEVENINGSTAR(), rounds_digits)
    # df_day['CDLGAPSIDESIDEWHITE'] = round(talib.CDLGAPSIDESIDEWHITE(), rounds_digits)
    # df_day['CDLGRAVESTONEDOJI'] = round(talib.CDLGRAVESTONEDOJI(), rounds_digits)
    # df_day['CDLHAMMER'] = round(talib.CDLHAMMER(), rounds_digits)
    # df_day['CDLHANGINGMAN'] = round(talib.CDLHANGINGMAN(), rounds_digits)
    # df_day['CDLHARAMI'] = round(talib.CDLHARAMI(), rounds_digits)
    # df_day['CDLHARAMICROSS'] = round(talib.CDLHARAMICROSS(), rounds_digits)
    # df_day['CDLHIGHWAVE'] = round(talib.CDLHIGHWAVE(), rounds_digits)
    # df_day['CDLHIKKAKE'] = round(talib.CDLHIKKAKE(), rounds_digits)
    # df_day['CDLHIKKAKEMOD'] = round(talib.CDLHIKKAKEMOD(), rounds_digits)
    # df_day['CDLHOMINGPIGEON'] = round(talib.CDLHOMINGPIGEON(), rounds_digits)
    # df_day['CDLIDENTICAL3CROWS'] = round(talib.CDLIDENTICAL3CROWS(), rounds_digits)
    # df_day['CDLINNECK'] = round(talib.CDLINNECK(), rounds_digits)
    # df_day['CDLINVERTEDHAMMER'] = round(talib.CDLINVERTEDHAMMER(), rounds_digits)
    # df_day['CDLKICKING'] = round(talib.CDLKICKING(), rounds_digits)
    # df_day['CDLKICKINGBYLENGTH'] = round(talib.CDLKICKINGBYLENGTH(), rounds_digits)
    # df_day['CDLLADDERBOTTOM'] = round(talib.CDLLADDERBOTTOM(), rounds_digits)
    # df_day['CDLLONGLEGGEDDOJI'] = round(talib.CDLLONGLEGGEDDOJI(), rounds_digits)
    # df_day['CDLLONGLINE'] = round(talib.CDLLONGLINE(), rounds_digits)
    # df_day['CDLMARUBOZU'] = round(talib.CDLMARUBOZU(), rounds_digits)
    # df_day['CDLMATCHINGLOW'] = round(talib.CDLMATCHINGLOW(), rounds_digits)
    # df_day['CDLMATHOLD'] = round(talib.CDLMATHOLD(), rounds_digits)
    # df_day['CDLMORNINGDOJISTAR'] = round(talib.CDLMORNINGDOJISTAR(), rounds_digits)
    # df_day['CDLMORNINGSTAR'] = round(talib.CDLMORNINGSTAR(), rounds_digits)
    # df_day['CDLONNECK'] = round(talib.CDLONNECK(), rounds_digits)
    # df_day['CDLPIERCING'] = round(talib.CDLPIERCING(), rounds_digits)
    # df_day['CDLRICKSHAWMAN'] = round(talib.CDLRICKSHAWMAN(), rounds_digits)
    # df_day['CDLRISEFALL3METHODS'] = round(talib.CDLRISEFALL3METHODS(), rounds_digits)
    # df_day['CDLSEPARATINGLINES'] = round(talib.CDLSEPARATINGLINES(), rounds_digits)
    # df_day['CDLSHOOTINGSTAR'] = round(talib.CDLSHOOTINGSTAR(), rounds_digits)
    # df_day['CDLSHORTLINE'] = round(talib.CDLSHORTLINE(), rounds_digits)
    # df_day['CDLSPINNINGTOP'] = round(talib.CDLSPINNINGTOP(), rounds_digits)
    # df_day['CDLSTALLEDPATTERN'] = round(talib.CDLSTALLEDPATTERN(), rounds_digits)
    # df_day['CDLSTICKSANDWICH'] = round(talib.CDLSTICKSANDWICH(), rounds_digits)
    # df_day['CDLTAKURI'] = round(talib.CDLTAKURI(), rounds_digits)
    # df_day['CDLTASUKIGAP'] = round(talib.CDLTASUKIGAP(), rounds_digits)
    # df_day['CDLTHRUSTING'] = round(talib.CDLTHRUSTING(), rounds_digits)
    # df_day['CDLTRISTAR'] = round(talib.CDLTRISTAR(), rounds_digits)
    # df_day['CDLUNIQUE3RIVER'] = round(talib.CDLUNIQUE3RIVER(), rounds_digits)
    # df_day['CDLUPSIDEGAP2CROWS'] = round(talib.CDLUPSIDEGAP2CROWS(), rounds_digits)
    # df_day['CDLXSIDEGAP3METHODS'] = round(talib.CDLXSIDEGAP3METHODS(), rounds_digits)
    # df_day['CMO'] = round(talib.CMO(), rounds_digits)
    # df_day['CORREL'] = round(talib.CORREL(), rounds_digits)
    # df_day['DEMA'] = round(talib.DEMA(), rounds_digits)
    # df_day['DX'] = round(talib.DX(), rounds_digits)
    # df_day['EMA'] = round(talib.EMA(), rounds_digits)
    # df_day['HT_DCPERIOD'] = round(talib.HT_DCPERIOD(), rounds_digits)
    # df_day['HT_DCPHASE'] = round(talib.HT_DCPHASE(), rounds_digits)
    # df_day['HT_PHASOR'] = round(talib.HT_PHASOR(), rounds_digits)
    # df_day['HT_SINE'] = round(talib.HT_SINE(), rounds_digits)
    # df_day['HT_TRENDLINE'] = round(talib.HT_TRENDLINE(), rounds_digits)
    # df_day['HT_TRENDMODE'] = round(talib.HT_TRENDMODE(), rounds_digits)
    # df_day['KAMA'] = round(talib.KAMA(), rounds_digits)
    # df_day['LINEARREG'] = round(talib.LINEARREG(), rounds_digits)
    # df_day['LINEARREG_ANGLE'] = round(talib.LINEARREG_ANGLE(), rounds_digits)
    # df_day['LINEARREG_INTERCEPT'] = round(talib.LINEARREG_INTERCEPT(), rounds_digits)
    # df_day['LINEARREG_SLOPE'] = round(talib.LINEARREG_SLOPE(), rounds_digits)
    # df_day['MA'] = round(talib.MA(), rounds_digits)
    # df_day['MACD'] = round(talib.MACD(), rounds_digits)
    # df_day['MACDEXT'] = round(talib.MACDEXT(), rounds_digits)
    # df_day['MACDFIX'] = round(talib.MACDFIX(), rounds_digits)
    # df_day['MAMA'] = round(talib.MAMA(), rounds_digits)
    # df_day['MAX'] = round(talib.MAX(), rounds_digits)
    # df_day['MAXINDEX'] = round(talib.MAXINDEX(), rounds_digits)
    # df_day['MEDPRICE'] = round(talib.MEDPRICE(), rounds_digits)
    # df_day['MFI'] = round(talib.MFI(), rounds_digits)
    # df_day['MIDPOINT'] = round(talib.MIDPOINT(), rounds_digits)
    # df_day['MIDPRICE'] = round(talib.MIDPRICE(), rounds_digits)
    # df_day['MIN'] = round(talib.MIN(), rounds_digits)
    # df_day['MININDEX'] = round(talib.MININDEX(), rounds_digits)
    # df_day['MINMAX'] = round(talib.MINMAX(), rounds_digits)
    # df_day['MINMAXINDEX'] = round(talib.MINMAXINDEX(), rounds_digits)
    # df_day['MINUS_DI'] = round(talib.MINUS_DI(), rounds_digits)
    # df_day['MINUS_DM'] = round(talib.MINUS_DM(), rounds_digits)
    # df_day['MOM'] = round(talib.MOM(), rounds_digits)
    # df_day['NATR'] = round(talib.NATR(), rounds_digits)
    # df_day['OBV'] = round(talib.OBV(df_day.close, df_day.volume), rounds_digits)
    # df_day['PLUS_DI'] = round(talib.PLUS_DI(), rounds_digits)
    # df_day['PLUS_DM'] = round(talib.PLUS_DM(), rounds_digits)
    # df_day['PPO'] = round(talib.PPO(), rounds_digits)
    # df_day['ROC'] = round(talib.ROC(), rounds_digits)
    # df_day['ROCP'] = round(talib.ROCP(), rounds_digits)
    # df_day['ROCR'] = round(talib.ROCR(), rounds_digits)
    # df_day['ROCR100'] = round(talib.ROCR100(), rounds_digits)
    # df_day['RSI'] = round(talib.RSI(), rounds_digits)
    # df_day['SAR'] = round(talib.SAR(), rounds_digits)
    # df_day['SAREXT'] = round(talib.SAREXT(), rounds_digits)
    # df_day['SMA'] = round(talib.SMA(), rounds_digits)
    # df_day['STDDEV'] = round(talib.STDDEV(), rounds_digits)
    # df_day['STOCH'] = round(talib.STOCH(), rounds_digits)
    # df_day['STOCHF'] = round(talib.STOCHF(), rounds_digits)
    fastk, fastd = talib.STOCHRSI(df_day.close, timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)
    df_day['STOCHRSI_fastk'] = round(fastk, round_digits)
    df_day['STOCHRSI_fastd'] = round(fastd, round_digits)
    # df_day['SUM'] = round(talib.SUM(), rounds_digits)
    # df_day['T3'] = round(talib.T3(), rounds_digits)
    # df_day['TEMA'] = round(talib.TEMA(), rounds_digits)
    # df_day['TRANGE'] = round(talib.TRANGE(), rounds_digits)
    # df_day['TRIMA'] = round(talib.TRIMA(), rounds_digits)
    # df_day['TRIX'] = round(talib.TRIX(), rounds_digits)
    # df_day['TSF'] = round(talib.TSF(), rounds_digits)
    # df_day['TYPPRICE'] = round(talib.TYPPRICE(), rounds_digits)
    # df_day['ULTOSC'] = round(talib.ULTOSC(), rounds_digits)
    # df_day['VAR'] = round(talib.VAR(), rounds_digits)
    # df_day['WCLPRICE'] = round(talib.WCLPRICE(), rounds_digits)
    # df_day['WILLR'] = round(talib.WILLR(), rounds_digits)
    # df_day['WMA'] = round(talib.WMA(), rounds_digits)
    df_day["BULL"] = np.where(df_day.close.diff() >= 0.02, True, False)

    for col in df_day.columns:
        df_day.dropna(subset=[col], inplace=True)
    arff.dump(asset + '.arff', df_day.values, relation=asset, names=df_day.columns)
