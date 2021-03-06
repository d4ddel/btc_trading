import modules.local_db as db
import modules.bitpanda_api as bapi
import modules.trading_toolbox as tools
import pandas as pd
import numpy as np
import datetime as dt


# create a database connection
database = r"H:\CryptoTradesDatenbank/pythonsqlite.db"
conn = db.create_connection(database)
# create a table in database
# db.create_table(conn, db.tables.public_trades_table)
# db.delete_trade(conn, sequence="1054563")
# db.drop_table(conn, 'public_trades')
# db.delete_all_trades(conn)


# get trades from database
all_trades = db.select_all_trades(conn)

# group trades in 5 min windows
all_trades_grouped = pd.DataFrame(all_trades.groupby(pd.Grouper(freq='15min'))['price'].mean())
all_trades_grouped.rename({"price": "Average"}, axis=1, inplace=True)
all_trades_grouped["High"] = pd.DataFrame(all_trades.groupby(pd.Grouper(freq='15min'))['price'].max())
all_trades_grouped["Low"] = pd.DataFrame(all_trades.groupby(pd.Grouper(freq='15min'))['price'].min())
all_trades_grouped["Close"] = pd.DataFrame(all_trades.groupby(pd.Grouper(freq='15min'))['price'].last())
all_trades_grouped["Trade_Count"] = pd.DataFrame(all_trades.groupby(pd.Grouper(freq='15min'))['price'].count())
all_trades_grouped = all_trades_grouped.ffill()
all_trades_grouped["Trade_Count"].plot()
all_trades_grouped["price"].plot()


trades_df = all_trades_grouped.interpolate(method='linear',
                                           axis='index')



# calculate weighted and exponential moving averages
trades_df["wma50"] = tools.calculate_wma(pd_series=trades_df['price'], periods=50)
trades_df["ema_alt50"] = tools.calculate_ema(pd_series=trades_df['price'], periods=50, adjust=True)
trades_df["ema_alt100"] = tools.calculate_ema(pd_series=trades_df['price'], periods=100, adjust=True)
indicator = "ema_alt100"
trades_df[indicator] = tools.calculate_ema(pd_series=trades_df['price'], periods=500, adjust=True)


trades_df = tools.get_local_maxima(trades_df, indicator)
trades_df = tools.get_local_minima(trades_df, indicator)

trades_df["buy_signal"] = pd.notna(trades_df["min_{}".format(indicator)])
trades_df["sell_signal"] = pd.notna(trades_df["max_{}".format(indicator)])
trades_df["buy_trade"] = np.where(trades_df["buy_signal"], trades_df["price"], None)
trades_df["sell_trade"] = np.where(trades_df["sell_signal"], trades_df["price"], None)

quasi_earnings = trades_df["sell_trade"].sum() - trades_df["buy_trade"].sum()

plt.figure(figsize=(12, 6))
plt.plot(trades_df['price'], label="Price", linewidth=1)
#plt.plot(trades_df["ema_alt50"], label="10-Day WMA", linewidth=1)
#plt.plot(trades_df["ema_alt100"], label="10-Day EMA-1", linewidth=1)
plt.plot(trades_df[indicator], label=indicator, linewidth=1)

# plt.scatter(x=trades_df.index, y=trades_df["max_ema_alt50"], label="sell", c='r')
# plt.scatter(x=trades_df.index, y=trades_df["min_ema_alt50"], label="buy", c='g')

# plt.scatter(x=trades_df.index, y=trades_df["max_ema_alt50"], label="sell", c='r')
# plt.scatter(x=trades_df.index, y=trades_df["min_ema_alt50"], label="buy", c='g')

# plt.scatter(x=trades_df.index, y=trades_df["max_ema_alt100"], label="sell", c='r', linewidths=2)
# plt.scatter(x=trades_df.index, y=trades_df["min_ema_alt100"], label="buy", c='g', linewidths=2)a

plt.scatter(x=trades_df.index, y=trades_df["sell_trade"], label="sell", c='r')
plt.scatter(x=trades_df.index, y=trades_df["buy_trade"], label="buy", c='g')



plt.xlabel("Date")
plt.ylabel("Price")
plt.legend()
plt.show()



trades_df['min'] = trades_df["price"][(trades_df["price"].shift(1) > trades_df["price"]) & (trades_df["price"].shift(-1) > trades_df["price"])]
trades_df['max'] = trades_df["price"][(trades_df["price"].shift(1) < trades_df["price"]) & (trades_df["price"].shift(-1) < trades_df["price"])]


