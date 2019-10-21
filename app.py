from environs import Env
env = Env()
env.read_env()
api_key=env("BITMEX_API_KEY")
api_secret=env("BITMEX_API_SECRET")

from datetime import datetime, timedelta
from multiprocessing import Process, Queue, Manager
import threading
from time import sleep
from trade_websocket import BitMEXWebsocket
from scipy.signal import argrelextrema
from utils import get_klines_df, get_instrument, send_trade_notif_email
import bitmex
import bravado
import logging
import math
import numpy as np
import pandas as pd
import sys
import os

from config import *
from trading import trade_entry, manage_position

pd.set_option('display.width', 250)

def setup_logger():
    FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    # filename=os.path.abspath('log/'+symbol+'.log'),
    logging.basicConfig(format=FORMAT, 
                        level=logging.INFO,
                        handlers=[
                            logging.FileHandler(os.path.abspath('log/'+symbol+'.log')),
                            logging.StreamHandler()
                        ])
    logger = logging.getLogger()
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
    global ORDER_QUEUE, data_df, i, POS_TABLE, isOrderInTransit, real_pnl
    if msg['table'] == 'trade':
        # >>>>>>> handle websocket 'trade' feed
        if data_df is None:
            # invoke only once at the start, fill dataframe with data
            logging.info('fetching RESTful data....')
            data_df = get_klines_df(symbol, binSize, startDate='2019-07-15 00:00:00', partial=True)
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
            start = -(LOOKBACK_PERIOD+2+1)
            end = -2
            current_window = data_df[start:end]   # IMPORTANT - exclude last 2 rows
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
                    logging.info('{}'.format(POS_TABLE))

            # truncate dataframe to preserve memory
            if len(data_df) > 400:
                data_df = data_df.tail(LOOKBACK_PERIOD + 10)

            recent_data = data_df[-2:]      # recent 2 bins
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
            ORDER_QUEUE, POS_TABLE, isOrderInTransit = manage_position(symbol, TRADE_SIZING, ORDER_QUEUE, POS_TABLE, data_df[-1:].to_dict('records')[0], isOrderInTransit)

            # ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
            # *************** POSITION MANAGEMENT LOGIC ENDS ***************
            # **************************************************************

    # >>>>>>> handle websocket 'position' feed
    # elif msg['table'] == 'position' and len(msg['data'][0]) >= 38:
    elif msg['table'] == 'position':
        current_qty = msg['data'][0]['currentQty']

        if 'avgCostPrice' in msg['data'][0]:    # close entire position
            timestamp = data_df.index[-1]
            recent_data = data_df[-2:]
            entry_price = msg['data'][0]['avgCostPrice']
            real_pnl = msg['data'][0]['realisedPnl']
            if symbol in POS_TABLE:
                if POS_TABLE[symbol]['quantity'] is not None and abs(current_qty) < abs(POS_TABLE[symbol]['remain_qty']):
                    # update POS_TABLE remaining quantity
                    POS_TABLE[symbol]['remain_qty'] = current_qty
                    logging.info('Position Update: {} {} Realised PnL: {real_pnl:.8f}'.format(symbol, POS_TABLE[symbol]['remain_qty'], real_pnl=real_pnl*0.00000001))
                    if isOrderInTransit: isOrderInTransit = False
                elif POS_TABLE[symbol]['quantity'] is None and abs(current_qty) > 0:
                    # new position - add entry in POS_TABLE
                    if current_qty > 0:     stop_loss = recent_data['low'].min()
                    elif current_qty < 0:   stop_loss = recent_data['high'].max()
                    POS_TABLE[symbol] = {
                        'timestamp':timestamp, 'quantity': current_qty, 'remain_qty': current_qty, 'entry_price': entry_price,
                        'stop_loss': stop_loss,'risk':abs(entry_price - stop_loss), 'stoploss_orderID': None,
                        'takeProfit_orderID1': None, 'takeProfit_orderID2': None, 'updateOrderQty': False
                    }
                    new_position_msg = 'New Position: {} {} Entry: {} Stoploss: {}'.format(symbol, POS_TABLE[symbol]['remain_qty'], entry_price, stop_loss)
                    logging.info(new_position_msg)
                    
                    try:
                        send_trade_notif_email(user_email, new_position_msg, 'testing...')
                    except Exception as e:
                        logging.info('send email error {}'.format(e))
                        pass

                    if isOrderInTransit: isOrderInTransit = False
        if 'execQty' in msg['data'][0] and abs(current_qty) > 0:    # close partial position
            if symbol in POS_TABLE and abs(current_qty) < abs(POS_TABLE[symbol]['remain_qty']):
                POS_TABLE[symbol]['remain_qty'] = current_qty
                POS_TABLE[symbol]['updateOrderQty'] = True  # toggle updateOrder boolean, need to modify stop_loss order quantity
                logging.info('Position Update: {} {} Realised PnL: {real_pnl:.8f}'.format(symbol, POS_TABLE[symbol]['remain_qty'], real_pnl=real_pnl*0.00000001))
                if isOrderInTransit: isOrderInTransit = False

def send_order(queue):
    global POS_TABLE, isOrderInTransit
    while True:
        order_detail = queue.get()
        logging.info('order detail ---> {}'.format(order_detail), )
        orderID = order_detail['orderID']
        orderQty = order_detail['orderQty']
        orderType = order_detail['orderType']
        price = order_detail['price']
        stopPx = order_detail['stopPx']
        order = None
        status_code = None

        while status_code != 200:
            try:
                if orderID is None:
                    # ************** new order **************
                    if price is None and stopPx is None:          # market order
                        logging.info('ORDER_QUEUE -- new market order')
                        order = client.Order.Order_new(symbol=symbol, orderQty=orderQty).result()
                    elif price is not None and stopPx is None:    # limit order
                        logging.info('ORDER_QUEUE -- new limit order')
                        order = client.Order.Order_new(symbol=symbol, orderQty=orderQty, price=price).result()
                    elif price is None and stopPx is not None:    # stop order
                        logging.info('ORDER_QUEUE -- new stop order')
                        order = client.Order.Order_new(symbol=symbol, orderQty=orderQty, stopPx=stopPx).result()
                else:
                    # ************* amend existing order *************
                    if price is None and stopPx is None:          # amend orderQty
                        logging.info('ORDER_QUEUE -- amend orderQty')
                        order = client.Order.Order_amend(orderID=orderID, orderQty=orderQty).result()
                    elif price is None and stopPx is not None:    # amend stopPx
                        logging.info('ORDER_QUEUE -- amend stopPx')
                        order = client.Order.Order_amend(orderID=orderID, orderQty=orderQty, stopPx=stopPx).result()
                sleep(0.07)
                status_code =  order[1].status_code
                logging.info('status_code {}'.format(status_code) )
            # except bravado.exception.HTTPServiceUnavailable as e:
            except Exception as e:
                logging.info('error: {} sleep and re-try'.format(e))
                logging.info('error_type {}'.format(type(e)))
                sleep(SLEEP_SECONDS)

        if symbol in POS_TABLE and orderID is None:     # populate POS_TABLE with orderID's
            if orderType == 'stoploss' and POS_TABLE[symbol]['stoploss_orderID'] == 'pending':
                logging.info('fill in stoploss orderID')
                POS_TABLE[symbol]['stoploss_orderID'] = order[0]['orderID']
            if orderType == 'tp1' and POS_TABLE[symbol]['takeProfit_orderID1'] == 'pending':
                logging.info('fill in takeProfit orderID 1')
                POS_TABLE[symbol]['takeProfit_orderID1'] = order[0]['orderID']
            if orderType == 'tp2' and POS_TABLE[symbol]['takeProfit_orderID2'] == 'pending' and POS_TABLE[symbol]['takeProfit_orderID1'] != 'pending':
                logging.info('fill in takeProfit orderID 2')
                POS_TABLE[symbol]['takeProfit_orderID2'] = order[0]['orderID']
        if symbol in POS_TABLE and orderID is not None:     # orderID's exist, then modify stop_loss price
            if stopPx is not None:
                POS_TABLE[symbol]['stop_loss'] = order[0]['stopPx']
        if isOrderInTransit: isOrderInTransit = False
        logging.info('Order Status: {} AvgPx: {} orderPrice: {} stopPx: {} orderQty: {}'.format(order[0]['ordStatus'], order[0]['avgPx'], order[0]['price'], order[0]['stopPx'], order[0]['orderQty'] ))
        logging.info('POS_TABLE \n {}'.format(POS_TABLE))
        logging.info('Queue size {}'.format(queue.qsize()) )


if __name__ == "__main__":
    symbol = 'XBTUSD'   # default symbol XBTUSD
    if len(sys.argv) >= 2: symbol = sys.argv[1]                # >>> python app.py ETHUSD
    if len(sys.argv) >= 3: PCT_RISK_PER_TRADE = float(sys.argv[2])    # >>> python app.py ETHUSD 0.015

    logger = setup_logger()

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
    POS_TABLE = dict()

    order_manager = threading.Thread( target=send_order, args=(ORDER_QUEUE,) )
    order_manager.start()

    # Instantiating the WS will make it connect. Be sure to add your api_key/api_secret.
    print('connect to websocket')
    # wst = threading.Thread( target=lambda: BitMEXWebsocket(endpoint="wss://www.bitmex.com/realtime", symbol=symbol, api_key=api_key, api_secret=api_secret, callback=handle_data) )
    # wst.start()
    ws = BitMEXWebsocket(endpoint="wss://www.bitmex.com/realtime", symbol=symbol, api_key=api_key, api_secret=api_secret, callback=handle_data)
    ws.start()
