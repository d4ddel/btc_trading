import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl

plt.style.use('fivethirtyeight')

weights = np.arange(1,11)


def calculate_supertrend(df_hi_lo_close, factor=14):
    """
    BASIC UPPERBAND = (HIGH + LOW) / 2 + Multiplier * ATR
    BASIC LOWERBAND = (HIGH + LOW) / 2 - Multiplier * ATR

    FINAL UPPERBAND = IF( (Current BASICUPPERBAND < Previous FINAL UPPERBAND) or (Previous Close > Previous FINAL UPPERBAND))
                        THEN (Current BASIC UPPERBAND) ELSE Previous FINALUPPERBAND)
    FINAL LOWERBAND = IF( (Current BASIC LOWERBAND > Previous FINAL LOWERBAND) or (Previous Close < Previous FINAL LOWERBAND))
                        THEN (Current BASIC LOWERBAND) ELSE Previous FINAL LOWERBAND)

    SUPERTREND = IF((Previous SUPERTREND = Previous FINAL UPPERBAND) and (Current Close <= Current FINAL UPPERBAND)) THEN
                    Current FINAL UPPERBAND
                ELSE
                    IF((Previous SUPERTREND = Previous FINAL UPPERBAND) and (Current Close > Current FINAL UPPERBAND)) THEN
                        Current FINAL LOWERBAND
                    ELSE
                        IF((Previous SUPERTREND = Previous FINAL LOWERBAND) and (Current Close >= Current FINAL LOWERBAND)) THEN
                            Current FINAL LOWERBAND
                        ELSE
                            IF((Previous SUPERTREND = Previous FINAL LOWERBAND) and (Current Close < Current FINAL LOWERBAND)) THEN
                                Current FINAL UPPERBAND
    """
    import numpy as np
    import pandas as pd
    # data = all_trades_grouped[["High", "Low", "Close"]]
    data = df_hi_lo_close
    data = data.ffill()
    data = data.reset_index(drop=True)

    data['tr0'] = abs(data["High"] - data["Low"])               # total spread
    data['tr1'] = abs(data["High"] - data["Close"].shift(1))    # upper band
    data['tr2'] = abs(data["Low"] - data["Close"].shift(1))     # lower band
    data["TR"] = round(data[['tr0', 'tr1', 'tr2']].max(axis=1), 2)  # max of vola, upper and lower
    data["ATR"] = 0.00
    data['BUB'] = 0.00
    data["BLB"] = 0.00
    data["FUB"] = 0.00
    data["FLB"] = 0.00
    data["ST"] = 0.00

    # Calculating ATR (volatility indicator)
    for i in range(len(data)):
        if i == 0:
            data._set_value(i, 'ATR', 0.00)
        else:
            data._set_value(i, 'ATR', ((data.loc[i - 1, 'ATR'] * (factor - 1)) + data.loc[i, 'TR']) / factor)

    data['BUB'] = round(((data["High"] + data["Low"]) / 2) + (2 * data["ATR"]), 2)  # BASIC UPPERBAND
    data['BLB'] = round(((data["High"] + data["Low"]) / 2) - (2 * data["ATR"]), 2)  # BASIC LOWERBAND

    # FINAL UPPERBAND = IF( (Current BASICUPPERBAND < Previous FINAL UPPERBAND) or (Previous Close > Previous FINAL UPPERBAND))
    #                     THEN (Current BASIC UPPERBAND) ELSE Previous FINALUPPERBAND)
    for i in range(len(data)):
        if i == 0:
            data._set_value(i, "FUB", 0.00)
        else:
            if (data.loc[i, "BUB"] < data.loc[i - 1, "FUB"]) | (data.loc[i - 1, "Close"] > data.loc[i - 1, "FUB"]):
                data._set_value(i, "FUB", data.loc[i, "BUB"])
            else:
                data._set_value(i, "FUB", data.loc[i - 1, "FUB"])

    # FINAL LOWERBAND = IF( (Current BASIC LOWERBAND > Previous FINAL LOWERBAND) or (Previous Close < Previous FINAL LOWERBAND))
    #                     THEN (Current BASIC LOWERBAND) ELSE Previous FINAL LOWERBAND)

    for i, row in data.iterrows():
        if i == 0:
            data._set_value(i, "FLB", 0.00)
        else:
            if (data.loc[i, "BLB"] > data.loc[i - 1, "FLB"]) | (data.loc[i - 1, "Close"] < data.loc[i - 1, "FLB"]):
                data._set_value(i, "FLB", data.loc[i, "BLB"])
            else:
                data._set_value(i, "FLB", data.loc[i - 1, "FLB"])

    # SUPERTREND = IF((Previous SUPERTREND = Previous FINAL UPPERBAND) and (Current Close <= Current FINAL UPPERBAND)) THEN
    #                 Current FINAL UPPERBAND
    #             ELSE
    #                 IF((Previous SUPERTREND = Previous FINAL UPPERBAND) and (Current Close > Current FINAL UPPERBAND)) THEN
    #                     Current FINAL LOWERBAND
    #                 ELSE
    #                     IF((Previous SUPERTREND = Previous FINAL LOWERBAND) and (Current Close >= Current FINAL LOWERBAND)) THEN
    #                         Current FINAL LOWERBAND
    #                     ELSE
    #                         IF((Previous SUPERTREND = Previous FINAL LOWERBAND) and (Current Close < Current FINAL LOWERBAND)) THEN
    #                             Current FINAL UPPERBAND

    for i in range(len(data)):
        if i == 0:
            data.loc[i, "ST"] = 0.00
        elif (data.loc[i - 1, "ST"] == data.loc[i - 1, "FUB"]) & (data.loc[i, "Close"] <= data.loc[i, "FUB"]):
            data.loc[i, "ST"] = data.loc[i, "FUB"]
        elif (data.loc[i - 1, "ST"] == data.loc[i - 1, "FUB"]) & (data.loc[i, "Close"] > data.loc[i, "FUB"]):
            data.loc[i, "ST"] = data.loc[i, "FLB"]
        elif (data.loc[i - 1, "ST"] == data.loc[i - 1, "FLB"]) & (data.loc[i, "Close"] >= data.loc[i, "FLB"]):
            data.loc[i, "ST"] = data.loc[i, "FLB"]
        elif (data.loc[i - 1, "ST"] == data.loc[i - 1, "FLB"]) & (data.loc[i, "Close"] < data.loc[i, "FLB"]):
            data.loc[i, "ST"] = data.loc[i, "FUB"]

    # Buy Sell Indicator
    for i in range(len(data)):
        if i == 0:
            data["ST_BUY_SELL"] = "NA"
        elif (data.loc[i, "ST"] < data.loc[i, "Close"]):
            data.loc[i, "ST_BUY_SELL"] = "BUY"
        else:
            data.loc[i, "ST_BUY_SELL"] = "SELL"
    data[["TR", "ATR", "BUB", "FUB", "BLB", "FLB", "ST"]].plot()
    data["BUY"] = np.where(data["ST_BUY_SELL"]=="BUY",
                               data["FUB"],
                               None)
    data["SELL"] = np.where(data["ST_BUY_SELL"]=="SELL",
                               data["FLB"],
                               None)
    return data["ST_BUY_SELL"]


def calculate_wma(pd_series, periods):
    """ calculates the 'exponential mean average' of a given series with a given number of periods
    """
    weights = np.arange(1, periods + 1)  #this creates an array with integers 1 to 10 included
    wma = pd_series.rolling(periods).apply(lambda prices: np.dot(prices, weights)/weights.sum(), raw=True)
    return pd.DataFrame(wma.dropna())


def calculate_ema(pd_series, periods, adjust=False):
    """ calculates the 'exponential mean average' of a given series with a given number of periods
    """
    ema = pd_series.ewm(span=periods, adjust=adjust).mean()
    return pd.DataFrame(ema.dropna())


def get_local_maxima(df, columns=None):
    if isinstance(columns, list):
        for c in columns:
            df['max_{}'.format(c)] = df[c][(df[c].shift(1) < df[c]) & (df[c].shift(-1) < df[c])]
    elif isinstance(columns, str):
        df['max_{}'.format(columns)] = df[columns][(df[columns].shift(1) < df[columns]) & (df[columns].shift(-1) < df[columns])]
    else:
        raise TypeError('columns must be either string or list of string')
    return df


def get_local_minima(df, columns=None):
    if isinstance(columns, list):
        for c in columns:
            df['min_{}'.format(c)] = df[c][(df[c].shift(1) > df[c]) & (df[c].shift(-1) > df[c])]
    elif isinstance(columns, str):
        df['min_{}'.format(columns)] = df[columns][(df[columns].shift(1) > df[columns]) & (df[columns].shift(-1) > df[columns])]
    else:
        raise TypeError('columns must be either string or list of string')
    return df


