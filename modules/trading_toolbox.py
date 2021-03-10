import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl

plt.style.use('fivethirtyeight')

weights = np.arange(1,11)


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


