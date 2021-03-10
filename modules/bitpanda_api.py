import requests
import json
import datetime as dt
import pytz
import pandas as pd
import numpy as np

pd.options.display.max_rows = 200
pd.options.display.max_columns = 15
pd.options.display.width = 200

# Endpoint: wss://streams.exchange.bitpanda.com
# Base URLs:
#   https://api.exchange.bitpanda.com/public/v1
#   wss://streams.exchange.bitpanda.com

url = "https://api.exchange.bitpanda.com/public/v1"

currencies = "/currencies"

candlesticks = "/candlesticks" + "/"

instruments = "/instruments"


# requests.get("https://api.exchange.bitpanda.com/public/v1/instruments").text


def local_dt_to_iso_utc(a_dt):
    import pytz
    local = pytz.timezone("Europe/Berlin")
    local_dt = local.localize(a_dt, is_dst=None)
    utc_dt = local_dt.astimezone(pytz.utc)
    iso_dt_utc = utc_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]+"Z"
    return iso_dt_utc


def get_current_orderbook(instrument="BTC_EUR"):
    response = requests.get("https://api.exchange.bitpanda.com/public/v1/order-book/{}".format(instrument))
    order_book = response.json()
    bids = order_book.get("bids")
    asks = order_book.get("asks")
    return bids, asks


def market_ticker(instrument="BTC_EUR"):
    mt = requests.get("https://api.exchange.bitpanda.com/public/v1/market-ticker/BTC_EUR").json()
    mt = pd.DataFrame.from_dict(mt, orient='index')
    return mt


def price_ticks(instrument="BTC_EUR", ts_from=None, ts_to=None):
    import datetime as dt
    import requests
    # from: 2020-12-19T15:21:14.555Z
    # to: 2020-12-19T15:21:14.555Z
    if not ts_to:
        ts_to = dt.datetime.now()
    if not ts_from:
        ts_from = dt.datetime.now() - dt.timedelta(hours=4)
    ts_f = local_dt_to_iso_utc(ts_from)
    ts_t = local_dt_to_iso_utc(ts_to)
    params = {
        'from':ts_f,
        'to':ts_t
    }
    pt = requests.get(
        "https://api.exchange.bitpanda.com/public/v1/price-ticks/{}".format(instrument), params=params).json()
    try:
        pt = pd.DataFrame(pt)
        pt["price"] = pd.to_numeric(pt["price"])
        pt["trade_timestamp"] = pt["trade_timestamp"].apply(lambda x: dt.datetime.fromtimestamp(x / 1000.))
        pt.set_index("trade_timestamp", inplace=True, drop=True)
        pt.drop(["time"], axis=1, inplace=True)
    except BaseException as e:
        print("ts_fro: {}".format(ts_f))
        print("ts_to: {}".format(ts_t))
        print(pt)
        print(e)
        return pd.DataFrame()
    # print(pt)
    return pt


def price_ticks_long_timeframe(instrument="BTC_EUR", ts_from=None, ts_to=None):
    if not ts_to:
        ts_to = dt.datetime.now()
    if not ts_from:
        ts_from = dt.datetime.now() - dt.timedelta(hours=13)
    timeframe = pd.date_range(ts_from, ts_to, freq='3H')
    trades = pd.DataFrame()
    for ts in timeframe:
        trades = trades.append(price_ticks(instrument=instrument, ts_from=ts, ts_to=ts + dt.timedelta(hours=3)))
    print(trades)
    return trades


def get_candle_sticks(instrument="BTC_EUR", unit='HOURS', period=1, ts_from=None, ts_to=None):
    import datetime as dt
    import requests
    import pandas as pd
    import iso8601

    if not ts_to:
        ts_to = dt.datetime.now()
    if not ts_from:
        ts_from = dt.datetime.now() - dt.timedelta(hours=3)
    ts_f = local_dt_to_iso_utc(ts_from)
    ts_t = local_dt_to_iso_utc(ts_to)
    params = {
        'unit': unit,
        'period': period,
        'from': ts_f,
        'to': ts_t
    }
    csd = requests.get("https://api.exchange.bitpanda.com/public/v1/candlesticks/{}".format(instrument), params=params).json()
    #print(csd.url)
    #print(csd.text)

    try:
        csd = pd.DataFrame(csd)
        csd[["high", "low", "open", "close", "total_amount", "volume", "last_sequence"]] = csd[["high", "low", "open", "close", "total_amount", "volume", "last_sequence"]].apply(pd.to_numeric)
        csd["time"] = csd["time"].apply(lambda x: iso8601.parse_date(x))
        csd.set_index("time", inplace=True, drop=True)
    except BaseException as e:
        print("ts_fro: {}".format(ts_f))
        print("ts_to: {}".format(ts_t))
        print(e)
        return csd
    print(csd)

#d = dt.datetime.utcnow()
#d_with_timezone = d.replace(tzinfo=pytz.UTC)
#ts_until = d_with_timezone.isoformat()

#dt.datetime.isoformat()
