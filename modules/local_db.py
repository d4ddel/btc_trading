import sqlite3
from sqlite3 import Error
import numpy as np


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def save_public_trade(conn, public_trade):
    """
    Create a new project into the projects table
    :param conn:
    :param project:
    :return: project id
    """
    sql = ''' 
    INSERT INTO public_trades(sequence, instrument_code,price,amount,volume,taker_side, DateTime_UTC)
       VALUES(?,?,?,?,?,?,?)   
       
    '''.format()
    """WHERE NOT EXISTS (SELECT sequence FROM public_trades WHERE sequence = {}) LIMIT 1 """
    """WHERE NOT EXISTS (SELECT primary-key FROM table-name WHERE primary-key = inserted-record) LIMIT 1"""
    cur = conn.cursor()
    cur.execute(sql, public_trade)
    conn.commit()
    return cur.lastrowid


def save_public_trade_df(conn, public_trade_df):
    """
    Create a new project into the projects table
    :param conn:
    :param project:
    :return: project id
    """

    for row in public_trade_df.iterrows():
        vals = row[1].values
        indx = row[1].name
        trades_as_array = np.insert(vals, 0, vals[-1])[:-1]
        trades_as_array = np.append(trades_as_array, indx.replace(microsecond=0).to_pydatetime())
        trades_as_tuple = tuple(trades_as_array)

        sql = ''' 
        INSERT OR IGNORE INTO public_trades(sequence, instrument_code,price,amount,volume,taker_side, DateTime_UTC)
           VALUES(?,?,?,?,?,?,?) '''.format()
        cur = conn.cursor()
        cur.execute(sql, trades_as_tuple)
        conn.commit()
    return cur.lastrowid


def update_trade(conn, public_trade):
    """
    update priority, begin_date, and end date of a task
    :param conn:
    :param task:
    :return: project id
    """
    sql = ''' UPDATE public_trades
              SET sequence = ? ,
                  instrument_code = ? ,
                  price = ?,
                  amount = ? ,
                  taker_side = ? ,
                  volume = ? ,
                  time = ?
              WHERE sequence = ?'''
    cur = conn.cursor()
    cur.execute(sql, public_trade)
    conn.commit()


def select_all_trades(conn, tz_from=None, tz_until=None):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    import datetime as dt
    import pytz
    import pandas as pd
    cur = conn.cursor()
    cur.execute("SELECT * FROM public_trades")

    columns = []
    for col in cur.description:
        columns.append(col[0])

    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=columns)

    # df["DateTime_UTC"] = df["DateTime_UTC"].apply(lambda dt_utc: dt.datetime.strptime(dt_utc, "%Y-%m-%d %H:%M:%S.%f"))
    df["DateTime_UTC"] = df["DateTime_UTC"].apply(lambda dt_utc: dt.datetime.strptime(dt_utc, "%Y-%m-%d %H:%M:%S"))
    df.set_index("DateTime_UTC", inplace=True, drop=True)

    return df


def select_trade_by_sequence(conn, sequence):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param priority:
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM public_trades WHERE sequence=?", (sequence,))

    rows = cur.fetchall()

    for row in rows:
        print(row)
    return rows


def delete_trade(conn, sequence):
    """
    Delete a trade by trade id
    :param conn:  Connection to the SQLite database
    :param id: id of the task
    :return:
    """
    sql = 'DELETE FROM public_trades WHERE sequence=?'
    cur = conn.cursor()
    cur.execute(sql, (sequence,))
    conn.commit()


def delete_all_trades(conn):
    """
    Delete all rows in the public_trades table
    :param conn: Connection to the SQLite database
    :return:
    """
    sql = 'DELETE FROM public_trades'
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def drop_table(conn, table_name):
    """
    Delete a table from db
    :param conn: Connection to the SQLite database
    :param table_name: Name of table to be removed
    :return:
    """
    sql = 'DROP TABLE {}'.format(table_name)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def get_table_column_names(conn, table_name):
    """
    Get the Column names of a table
    :param conn: Connection to the SQLite database
    :param table_name: Name of table to show the columns from
    :return:
    """
    sql = '''SELECT * FROM INFORMATION_SCHEMA.COLUMNS
             --WHERE TABLE_NAME = N{}'''.format(table_name)
    """SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.yourTableName') """
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


database = r"pythonsqlite.db"
                                    # id integer PRIMARY KEY,

class tables():
    public_trades_table = """ CREATE TABLE IF NOT EXISTS public_trades (
                                        sequence INTEGER PRIMARY KEY,
                                        instrument_code text NOT NULL,
                                        price FLOAT,
                                        amount FLOAT,
                                        volume FLOAT,
                                        taker_side text NOT NULL,
                                        DateTime_UTC DATETIME
                                    ); """

"""# create a database connection
conn = create_connection(database)

create_table(conn, sql_create_public_trades_table)
# create a new project
#  "     ""instrument_code, price, amount, taker_side, volume, time, sequence""
a_trade = ("1054563", 'BTC_EUR', '27640.12', '0.00107', 'SELL', '29.5749284', '2021-01-31T12:06:00.694Z')
# a_trade = ('Cool App with SQLite & Python', '2015-01-01', '2015-01-30')
project_id = save_public_trade(conn, public_trade=a_trade)
update_trade(conn, ("1054563", 'BTC_EUR', '27640.12123123', '0.00107', 'SELL', '29.5749284', '2021-01-31T12:06:00.694Z', '1054563'))
select_all_trades(conn)
delete_trade(conn, sequence="1054563")
# drop_table(conn, 'public_trades')
"""