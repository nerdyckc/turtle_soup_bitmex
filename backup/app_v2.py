from environs import Env
env = Env()
env.read_env()
api_key=env("BITMEX_API_KEY")
api_secret=env("BITMEX_API_SECRET")

from datetime import datetime, timedelta
from multiprocessing import Process, Queue, Manager
from time import sleep
from trade_websocket import BitMEXWebsocket
from scipy.signal import argrelextrema
from utils import get_klines_df, get_instrument
import bitmex
import logging
import math
import numpy as np
import pandas as pd
import sys

from config import *
from trading import trade_entry, manage_position

pd.set_option('display.width', 250)

def setup_logger():
    # Prints logger info to terminal
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Change this to DEBUG if you want a lot more info
    ch = logging.StreamHandler()
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # add formatter to ch
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

def compute_columns(df, counter):
    '''
    indexer: look at last n rows in dataframe...
    '''
    # compute 20- and 4-period highs/lows
    df['period_high_slow'] = df['high'].rolling(19, min_periods=3).max().shift(1)
    df['period_high_fast'] = df['high'].rolling(3, min_periods=3).max().shift(1)
    df['period_low_slow'] = df['low'].rolling(19, min_periods=3).min().shift(1)
    df['period_low_fast'] = df['low'].rolling(3, min_periods=3).min().shift(1)
    
    # add high/low boolean columns
    df.loc[df.high > df.period_high_slow, 'isNewHigh'] = True
    df.loc[df.high <= df.period_high_slow, 'isNewHigh'] = False
    df.loc[df.low < df.period_low_slow, 'isNewLow'] = True
    df.loc[df.low >= df.period_low_slow, 'isNewLow'] = False

    n=3
    df['trough'] = df.iloc[argrelextrema(df['low'].values[:-1], np.less_equal, order=n)[0]]['low']
    df['peak'] = df.iloc[argrelextrema(df['high'].values[:-1], np.greater_equal, order=n)[0]]['high']

def handle_data(msg):
    global ORDER_QUEUE, data_df, LOOKBACK_PERIOD, i, POS_TABLE, isOrderInTransit, real_pnl
    if msg['table'] == 'trade':
        # >>>>>>> handle websocket 'trade' feed
        if data_df is None:
            # invoke only once at the start, fill dataframe with data
            print('fetching RESTful data....')
            data_df = get_klines_df(symbol, binSize, startDate='2019-05-22 00:00:00', partial=True)
            data_df = data_df.drop(columns=['homeNotional','foreignNotional', 'vwap','lastSize', 'turnover'])
            data_df.set_index('timestamp', drop=True, inplace=True)    # change RangeIndex to TimestampIndex

            data_df['period_high_slow'] = np.nan
            data_df['period_high_fast'] = np.nan
            data_df['period_low_slow'] = np.nan
            data_df['period_low_fast'] = np.nan
            data_df['isNewHigh'] = np.nan
            data_df['isNewLow'] = np.nan
            data_df['trough'] = np.nan
            data_df['peak'] = np.nan

            compute_columns(data_df, i)

            # print(data_df[['high','low','close','period_high_slow', 'period_high_fast', 'period_low_slow', 'period_low_fast', 'trough', 'peak']].tail(15))
            print(data_df[['high','low','close','period_high_slow','period_high_fast','period_low_slow','period_low_fast','isNewHigh','peak','isNewLow','trough']].tail(15))
            return
        elif len(data_df) > 0:
            start = -(LOOKBACK_PERIOD+4+1)
            end = -4
            current_window = data_df[start:end]   # IMPORTANT - exclude last 4 rows
            isNewLow_index = current_window[(current_window.isNewLow == True) & (current_window.trough > 0)].index
            local_troughs = current_window.low.loc[isNewLow_index]
            isNewHigh_index = current_window[(current_window.isNewHigh == True) & (current_window.peak > 0)].index
            local_peaks = current_window.high.loc[isNewHigh_index]
            for trade in msg['data']:
                trade_time = pd.Timestamp.strptime(trade['timestamp'][:-1], '%Y-%m-%dT%H:%M:%S.%f').tz_localize('UTC')

                if trade_time <= data_df.index[-1]:
                    # if trade within current bucket
                    data_df.loc[data_df.index[-1], 'close'] = trade['price']
                    data_df.loc[data_df.index[-1], 'high'] = max(data_df.loc[data_df.index[-1], 'high'], trade['price'])
                    data_df.loc[data_df.index[-1], 'low'] = min(data_df.loc[data_df.index[-1], 'low'], trade['price'])
                    data_df.loc[data_df.index[-1], 'trades'] += 1
                    data_df.loc[data_df.index[-1], 'volume'] += trade['size']
                    
                    recent_data = data_df[-4:]      # recent 4 bins
                    timestamp = data_df.index[-1]

                    # **************************************************************
                    # ********************* TRADING LOGIC HERE *********************
                    # VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV
                    ORDER_QUEUE, POS_TABLE, isOrderInTransit = trade_entry(symbol, TRADE_SIZING, ORDER_QUEUE, POS_TABLE, local_troughs, local_peaks, recent_data, timestamp, isOrderInTransit)

                    # ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                    # ********************* TRADING LOGIC ENDS *********************
                    # **************************************************************

                    # **************************************************************
                    # *************** POSITION MANAGEMENT LOGIC HERE ***************
                    # VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV
                    ORDER_QUEUE, POS_TABLE, isOrderInTransit = manage_position(symbol, ORDER_QUEUE, POS_TABLE, data_df[-1:].to_dict('records')[0], TRADE_SIZING['TICK_SIZE'], isOrderInTransit)

                    # ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                    # *************** POSITION MANAGEMENT LOGIC ENDS ***************
                    # **************************************************************

                else:
                    # create new bin if trade_time greater than last bin close
                    new_timestamp = data_df.index[-1] + td_map[binSize]
                    prev_close = data_df.loc[data_df.index[-1], 'close']    # prev_close = current_open
                    data_df.loc[new_timestamp] = [trade['symbol'], prev_close, trade['price'], trade['price'], trade['price'], 1, trade['size'], np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]

                    i += 1
                    print('counter', i)
                    print('compute columns...')
                    compute_columns(data_df, i)
                    print(data_df[['high','low','close','period_high_slow','period_high_fast','period_low_slow','period_low_fast','isNewHigh','peak','isNewLow','trough']].tail(6))
                    print(POS_TABLE)

            # truncate dataframe to preserve memory
            if len(data_df) > 500:
                data_df = data_df.tail(200)

    # >>>>>>> handle websocket 'position' feed
    # elif msg['table'] == 'position' and len(msg['data'][0]) >= 38:
    elif msg['table'] == 'position':
        current_qty = msg['data'][0]['currentQty']
        if 'avgCostPrice' in msg['data'][0]:
            timestamp = data_df.index[-1]
            recent_data = data_df[-4:]
            # POS_TABLE: {timestamp, quantity, remain_qty, entry_price, stop_loss, risk, orderID}
            entry_price = msg['data'][0]['avgCostPrice']
            real_pnl = msg['data'][0]['realisedPnl']
            if symbol in POS_TABLE and abs(current_qty) < abs(POS_TABLE[symbol]['remain_qty']):
                # update POS_TABLE remaining quantity
                print('Position Update: {} {} Realised PnL: {real_pnl:.8f}'.format(symbol, POS_TABLE[symbol]['remain_qty'], real_pnl=real_pnl*0.00000001))
                if isOrderInTransit: isOrderInTransit = False
            elif symbol not in POS_TABLE and abs(current_qty) > 0:
                # new position - add entry in POS_TABLE
                if current_qty > 0:     stop_loss = recent_data['low'].min()
                elif current_qty < 0:   stop_loss = recent_data['high'].max()
                POS_TABLE[symbol] = {'timestamp':timestamp, 'quantity': current_qty, 'remain_qty': current_qty, 'entry_price': entry_price, 'stop_loss': stop_loss, 'risk':abs(entry_price - stop_loss), 'stoploss_orderID': None, 'takeProfit_orderID': None, 'updateOrderQty': False}
                print('New Position: {} {} Stoploss: {}'.format(symbol, POS_TABLE[symbol]['remain_qty'], stop_loss))
                if isOrderInTransit: isOrderInTransit = False
        if 'execQty' in msg['data'][0] and abs(current_qty) > 0:
            if symbol in POS_TABLE and abs(current_qty) < abs(POS_TABLE[symbol]['remain_qty']):
                POS_TABLE[symbol]['updateOrderQty'] = True  # toggle updateOrder boolean, need to modify stop_loss order quantity
                print('Position Update: {} {} Realised PnL: {real_pnl:.8f}'.format(symbol, POS_TABLE[symbol]['remain_qty'], real_pnl=real_pnl*0.00000001))
                if isOrderInTransit: isOrderInTransit = False

def send_order(queue, POS_TABLE):
    while True:
        order_detail = queue.get()
        print('order detail --->', order_detail)
        orderID = order_detail['orderID']
        orderQty = order_detail['orderQty']
        price = order_detail['price']
        stopPx = order_detail['stopPx']

        status_code = None
        while status_code != 200:
            if status_code == 503: sleep(SLEEP_SECONDS)
            if orderID is None:
                # ************** new order **************
                if price is None and stopPx is None:          # market order
                    print('new market order')
                    order = client.Order.Order_new(symbol=symbol, orderQty=orderQty).result()
                elif price is not None and stopPx is None:    # limit order
                    print('new limit order')
                    order = client.Order.Order_new(symbol=symbol, orderQty=orderQty, price=price).result()
                elif price is None and stopPx is not None:    # stop order
                    print('new stop order')
                    order = client.Order.Order_new(symbol=symbol, orderQty=orderQty, stopPx=stopPx).result()
            else:
                # ************* amend existing order *************
                if price is None and stopPx is None:          # amend orderQty
                    order = client.Order.Order_amend(orderID=orderID, symbol=symbol, orderQty=orderQty).result()
                elif price is None and stopPx is not None:    # amend stopPx
                    order = client.Order.Order_amend(orderID=orderID, symbol=symbol, orderQty=orderQty, stopPx=stopPx).result()
            sleep(0.07)
            status_code =  order[1].status_code

        if symbol in POS_TABLE and orderID is None:
            print('checkpoint 1')
            if stopPx is not None and POS_TABLE[symbol]['stoploss_orderID'] is None:
                print('fill in stoploss orderID')
                POS_TABLE[symbol]['stoploss_orderID'] = order[0]['orderID']
            if price is not None and POS_TABLE[symbol]['takeProfit_orderID'] is None:
                print('fill in takeProfit orderID')
                POS_TABLE[symbol]['takeProfit_orderID'] = order[0]['orderID']
        print('{} orderID: {}'.format(order[0]['symbol'], order[0]['orderID']))
        print('Order Status: {} AvgPx: {} orderPrice: {} stopPx: {} orderQty: {}'.format(order[0]['ordStatus'], order[0]['avgPx'], order[0]['price'], order[0]['stopPx'], order[0]['orderQty'] ))
        print('POS_TABLE', POS_TABLE)
        print('Queue size', queue.qsize())


if __name__ == "__main__":
    symbol = 'XBTUSD'   # default symbol XBTUSD
    if len(sys.argv) >= 2: symbol = sys.argv[1]                # >>> python app.py ETHUSD
    if len(sys.argv) >= 3: PCT_RISK_PER_TRADE = float(sys.argv[2])    # >>> python app.py ETHUSD 0.015

    client = bitmex.bitmex(test=False, api_key=api_key, api_secret=api_secret)
    ref_price = client.Instrument.Instrument_get(symbol=symbol).result()[0][0]['vwap']
    MARGIN_BALANCE = client.User.User_getMargin().result()[0]['marginBalance'] * 0.00000001
    RISK_BTC = MARGIN_BALANCE * PCT_RISK_PER_TRADE     # risk (i.e. stop loss) btc amount: btc_amount x percentage
    TICK_SIZE = get_instrument(symbol)['tickSize']
    print('balance in btc:', MARGIN_BALANCE)
    print('RISK_BTC:', RISK_BTC)

    TRADE_SIZING = {'ref_price':ref_price, 'MARGIN_BALANCE':MARGIN_BALANCE, 'RISK_BTC':RISK_BTC, 'TICK_SIZE':TICK_SIZE}

    data_df = None
    i = -1      # counter purposely starts from -1
    ORDER_QUEUE = Queue()   # order queue for multiprocessing
    isOrderInTransit = False   # IMPORTANT - prevent sending same order multiple times!!!
    
    manager = Manager()
    POS_TABLE = manager.dict()      # symbol as key, include 'timestamp', 'side', 'entry_price' and 'stop_loss'

    order_manager = Process( target=send_order, args=(ORDER_QUEUE,POS_TABLE) )
    order_manager.start()

    logger = setup_logger()

    # Instantiating the WS will make it connect. Be sure to add your api_key/api_secret.
    print('connect to websocket')
    ws = BitMEXWebsocket(endpoint="wss://www.bitmex.com/realtime", symbol=symbol, api_key=api_key, api_secret=api_secret, callback=handle_data)
    ws.start()
