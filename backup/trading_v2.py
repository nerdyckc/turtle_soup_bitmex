from environs import Env
env = Env()
env.read_env()
api_key=env("BITMEX_API_KEY")
api_secret=env("BITMEX_API_SECRET")

import bitmex
import json
import math
import numpy as np
import pandas as pd
import logging
from time import sleep
from datetime import datetime, timedelta
from config import binSize, binSize2, td_map, STOP_LOSS_PCT_FLOOR, user_email, SLEEP_SECONDS
from utils import send_trade_notif_email
import sys

logger = logging.getLogger(__name__)
client = bitmex.bitmex(test=False, api_key=api_key, api_secret=api_secret)

recent_timestamp = None
recent_timestamp2 = None
BTC_price = None


def manage_position(symbol, POS_TABLE, this_bin, TICK_SIZE):
    global recent_timestamp
    rr_ratio = 0
    if symbol in POS_TABLE:
        if POS_TABLE[symbol]['remain_qty'] == 0: # Flat
            # pos = client.Position.Position_get(filter=json.dumps({'symbol': symbol})).result()[0][0]
            client.Order.Order_cancel(orderID=POS_TABLE[symbol]['stoploss_orderID']).result()
            client.Order.Order_cancel(orderID=POS_TABLE[symbol]['takeProfit_orderID']).result()
            del POS_TABLE[symbol]
            print('<<<<<<<< ------- Position closed! ------- >>>>>>>>>')
            return POS_TABLE

        TRADE_SIZE = POS_TABLE[symbol]['remain_qty']
        STOP_LOSS = POS_TABLE[symbol]['stop_loss']

        if POS_TABLE[symbol]['quantity'] > 0: # BUY side
            reward = this_bin['close'] - POS_TABLE[symbol]['entry_price']
            rr_ratio = reward / POS_TABLE[symbol]['risk']

            # create (SELL) take profit order if doesn't exist
            if POS_TABLE[symbol]['takeProfit_orderID'] is None:
                # close 50% position at 3x
                target_price = POS_TABLE[symbol]['entry_price'] + (3 * POS_TABLE[symbol]['risk'])
                takeProfit_order = client.Order.Order_new(symbol=symbol, price=target_price, orderQty=-TRADE_SIZE*0.5).result()
                if takeProfit_order[1].status_code == 200:
                    print('create take profit order for new position')
                    POS_TABLE[symbol]['takeProfit_orderID'] = takeProfit_order[0]['orderID']
                elif takeProfit_order[1].status_code == 503:
                    logger.error('stop_loss order_new status 503, X-RateLimit-Remaining:', order_amend_req[1].headers['X-RateLimit-Remaining'])
                    sleep(SLEEP_SECONDS)
            # create (SELL) stop loss order if doesn't exist
            if POS_TABLE[symbol]['stoploss_orderID'] is None:
                stop_loss_order = client.Order.Order_new(symbol=symbol, stopPx=STOP_LOSS-TICK_SIZE, orderQty=-TRADE_SIZE).result()
                if stop_loss_order[1].status_code == 200:
                    print('create stop loss (SELL) order for new position')
                    POS_TABLE[symbol]['stoploss_orderID'] = stop_loss_order[0]['orderID']
                elif stop_loss_order[1].status_code == 503:
                    logger.error('stop_loss order_new status 503, X-RateLimit-Remaining:', order_amend_req[1].headers['X-RateLimit-Remaining'])
                    sleep(SLEEP_SECONDS)
            # raise stop_loss to entry_price if rr_ratio > 2.0
            if rr_ratio >= 2.0 and STOP_LOSS < POS_TABLE[symbol]['entry_price']:
                order_amend_req = client.Order.Order_amend(orderID=POS_TABLE[symbol]['stoploss_orderID'], stopPx=POS_TABLE[symbol]['entry_price'], orderQty=-TRADE_SIZE).result()
                if order_amend_req[1].status_code == 200:
                    print('raise stop_loss to entry_price if rr_ratio > 2.0')
                    POS_TABLE[symbol]['stop_loss'] = POS_TABLE[symbol]['entry_price']
                elif order_amend_req[1].status_code == 503:
                    logger.error('order_amend status 503, X-RateLimit-Remaining:', order_amend_req[1].headers['X-RateLimit-Remaining'])
                    sleep(SLEEP_SECONDS)
            # raise stop_loss to rr3 if rr_ratio > 6.0
            elif rr_ratio >= 6.0 and STOP_LOSS == POS_TABLE[symbol]['entry_price']:
                new_stopPx = POS_TABLE[symbol]['entry_price'] + (3 * POS_TABLE[symbol]['risk'])
                order_amend_req = client.Order.Order_amend(orderID=POS_TABLE[symbol]['stoploss_orderID'], stopPx=new_stopPx, orderQty=-TRADE_SIZE).result()
                if order_amend_req[1].status_code == 200:
                    print('raise stop_loss to rr3 if rr_ratio > 6.0')
                    POS_TABLE[symbol]['stop_loss'] = new_stopPx
                elif order_amend_req[1].status_code == 503:
                    logger.error('order_amend status 503, X-RateLimit-Remaining:', order_amend_req[1].headers['X-RateLimit-Remaining'])
                    sleep(SLEEP_SECONDS)

        if POS_TABLE[symbol]['quantity'] < 0: # SELL side
            reward = POS_TABLE[symbol]['entry_price'] - this_bin['close']
            rr_ratio = reward / POS_TABLE[symbol]['risk']

            # create (BUY) take profit order if doesn't exist
            if POS_TABLE[symbol]['takeProfit_orderID'] is None:
                # close 50% position at 3x
                target_price = POS_TABLE[symbol]['entry_price'] - (3 * POS_TABLE[symbol]['risk'])
                takeProfit_order = client.Order.Order_new(symbol=symbol, price=target_price, orderQty=-TRADE_SIZE*0.5).result()
                if takeProfit_order[1].status_code == 200:
                    print('create take profit order for new position')
                    POS_TABLE[symbol]['takeProfit_orderID'] = takeProfit_order[0]['orderID']
                elif takeProfit_order[1].status_code == 503:
                    logger.error('stop_loss order_new status 503, X-RateLimit-Remaining:', order_amend_req[1].headers['X-RateLimit-Remaining'])
                    sleep(SLEEP_SECONDS)
            # create (BUY) stop loss order if doesn't exist
            if POS_TABLE[symbol]['stoploss_orderID'] is None:
                stop_loss_order = client.Order.Order_new(symbol=symbol, stopPx=STOP_LOSS+TICK_SIZE, orderQty=-TRADE_SIZE).result()
                if stop_loss_order[1].status_code == 200:
                    print('create stop loss (BUY) order for new position')
                    POS_TABLE[symbol]['stoploss_orderID'] = stop_loss_order[0]['orderID']
                elif stop_loss_order[1].status_code == 503:
                    logger.error('stop_loss order_new status 503, X-RateLimit-Remaining:', order_amend_req[1].headers['X-RateLimit-Remaining'])
                    sleep(SLEEP_SECONDS)
            # lower stop_loss to entry_price if rr_ratio > 2.0
            if rr_ratio >= 2.0 and STOP_LOSS > POS_TABLE[symbol]['entry_price']:
                order_amend_req = client.Order.Order_amend(orderID=POS_TABLE[symbol]['stoploss_orderID'], stopPx=POS_TABLE[symbol]['entry_price'], orderQty=-TRADE_SIZE).result()
                if order_amend_req[1].status_code == 200:
                    print('lower stop_loss to entry_price if rr_ratio > 2.0')
                    POS_TABLE[symbol]['stop_loss'] = POS_TABLE[symbol]['entry_price']
                elif order_amend_req[1].status_code == 503:
                    logger.error('order_amend status 503, X-RateLimit-Remaining:', order_amend_req[1].headers['X-RateLimit-Remaining'])
                    sleep(SLEEP_SECONDS)
            # lower stop_loss to rr3 if rr_ratio > 6.0
            if rr_ratio >= 6.0 and STOP_LOSS == POS_TABLE[symbol]['entry_price']:
                new_stopPx = POS_TABLE[symbol]['entry_price'] - (3 * POS_TABLE[symbol]['risk'])
                order_amend_req = client.Order.Order_amend(orderID=POS_TABLE[symbol]['stoploss_orderID'], stopPx=new_stopPx, orderQty=-TRADE_SIZE).result()
                if order_amend_req[1].status_code == 200:
                    print('lower stop_loss to rr3 if rr_ratio > 6.0')
                    POS_TABLE[symbol]['stop_loss'] = new_stopPx
                elif order_amend_req[1].status_code == 503:
                    logger.error('order_amend status 503, X-RateLimit-Remaining:', order_amend_req[1].headers['X-RateLimit-Remaining'])
                    sleep(SLEEP_SECONDS)
        # report position every X minutes
        now_timestamp = datetime.now()
        if recent_timestamp is None or (now_timestamp - recent_timestamp) >= timedelta(minutes = 2):
            recent_timestamp = now_timestamp
            if abs(POS_TABLE[symbol]['remain_qty']) > 0:
                print('Position {symbol} {pos} >> RR_ratio {rr_ratio:.2f} Entry_price {entry_price} Last_price {last_price}'.format(symbol=symbol, pos=POS_TABLE[symbol]['remain_qty'], rr_ratio=rr_ratio, entry_price=POS_TABLE[symbol]['entry_price'], last_price=this_bin['close']))

    return POS_TABLE


def trade_entry(symbol, TRADE_SIZING, ORDER_TABLE, POS_TABLE, local_troughs, local_peaks, recent_data, timestamp, isOrderInTransit):
    global recent_timestamp2, BTC_price
    this_bin = recent_data[-1:].to_dict('records')[0]    # get current data bin
    isOrderInTransit = isOrderInTransit
    # update BTC price every X minutes - for trade sizing purpose!!!!
    now_timestamp = datetime.now()
    if recent_timestamp2 is None or (now_timestamp - recent_timestamp2) >= timedelta(minutes = 5):
        recent_timestamp2 = now_timestamp
        BTC_price = client.Instrument.Instrument_get(symbol='XBTUSD').result()[0][0]['fairPrice']

    # ********************** STOP BUY & SELL logic **********************
    # ****** if symbol has pending stop order from previous period ******
    # ******               i.e. Turtle Soup PLUS ONE               ******
    if symbol in ORDER_TABLE and symbol not in POS_TABLE:
        # check timestamps match (valid for 1 period only) AND order not yet triggered
        if ORDER_TABLE[symbol]['timestamp'] == timestamp and not ORDER_TABLE[symbol]['triggered']:
            # print('checking for Turtle Soup PLUS ONE', ORDER_TABLE[symbol]['side'], 'entry')
            stop_price = ORDER_TABLE[symbol]['stop_price']  # raw stop-entry price

            if ORDER_TABLE[symbol]['side'] == 'BUY':
                stop_loss = recent_data['low'].min()

                # adjust entry price if stop and entry prices are too close, to minimise excessive price whipsaws
                if stop_loss < (stop_price * (1-STOP_LOSS_PCT_FLOOR)):    entry_price = stop_price
                else:
                    adj_price = stop_loss / (1-STOP_LOSS_PCT_FLOOR)
                    entry_price = math.floor(adj_price/TRADE_SIZING['TICK_SIZE']) * TRADE_SIZING['TICK_SIZE']
                
                if not isOrderInTransit and this_bin['high'] > entry_price:  # TRIGGER STOP BUY
                    ORDER_TABLE[symbol]['triggered'] = True
                    # calculate trade size
                    RISK_DOLLAR = BTC_price * TRADE_SIZING['RISK_BTC']
                    TRADE_SIZE_debug = RISK_DOLLAR / abs((this_bin['low'] / entry_price) - 1)
                    print('debug TRADE_SIZE:', TRADE_SIZE_debug)
                    TRADE_SIZE = 10
                    # send order
                    buy_order = client.Order.Order_new(symbol=symbol, orderQty=TRADE_SIZE).result()
                    if buy_order[1].status_code == 200:
                        trade_detail = 'triggered stop buy @ {entry_price:6.2f} stop-loss {stop_loss:6.2f}'.format(entry_price=buy_order[0]['avgPx'], stop_loss=stop_loss)
                        print(trade_detail)
                        send_trade_notif_email(user_email, 'BitMEX Turtle Soup trade notification', trade_detail)
                        isOrderInTransit = True
                    elif sell_order[1].status_code == 503:
                        logger.error('sell_order status 503, X-RateLimit-Remaining:', sell_order[1].headers['X-RateLimit-Remaining'])
                        sleep(SLEEP_SECONDS)
                    return (ORDER_TABLE, POS_TABLE, isOrderInTransit)

            if ORDER_TABLE[symbol]['side'] == 'SELL':
                stop_loss = recent_data['high'].max()

                # adjust entry price if stop-loss and entry prices are too close, to minimise excessive price whipsaws
                if stop_loss > (stop_price * (1+STOP_LOSS_PCT_FLOOR)):    entry_price = stop_price
                else:
                    adj_price = stop_loss / (1+STOP_LOSS_PCT_FLOOR)
                    entry_price = math.ceil(adj_price/TRADE_SIZING['TICK_SIZE']) * TRADE_SIZING['TICK_SIZE']

                if not isOrderInTransit and this_bin['low'] < entry_price:   # TRIGGER STOP SELL
                    ORDER_TABLE[symbol]['triggered'] = True
                    # calculate trade size
                    RISK_DOLLAR = BTC_price * TRADE_SIZING['RISK_BTC']
                    TRADE_SIZE_debug = RISK_DOLLAR / abs((this_bin['high'] / entry_price) - 1)
                    print('debug TRADE_SIZE:', TRADE_SIZE_debug)
                    TRADE_SIZE = 10
                    # send order
                    sell_order = client.Order.Order_new(symbol=symbol, orderQty=-TRADE_SIZE).result()
                    if sell_order[1].status_code == 200:
                        trade_detail = 'triggered stop sell @ {entry_price:6.2f} stop-loss {stop_loss:6.2f}'.format(entry_price=sell_order[0]['avgPx'], stop_loss=stop_loss)
                        print(trade_detail)
                        send_trade_notif_email(user_email, 'BitMEX Turtle Soup trade notification', trade_detail)
                        isOrderInTransit = True
                    elif sell_order[1].status_code == 503:
                        logger.error('sell_order status 503, X-RateLimit-Remaining:', sell_order[1].headers['X-RateLimit-Remaining'])
                        sleep(SLEEP_SECONDS)
                    return (ORDER_TABLE, POS_TABLE, isOrderInTransit)

    # ******************** BUY logic ********************
    if len(local_troughs) > 0 and symbol not in POS_TABLE:
        for idx in local_troughs.index:
            window = local_troughs[idx:local_troughs.index[len(local_troughs)-1]]
            abs_min = window.min()
            abs_min_idx = window.idxmin()

            # if not lowest trough and occurs prior to lowest trough, ignore it
            if window[idx] > abs_min and idx < abs_min_idx:
                continue
            else:
                if this_bin['low'] < this_bin['period_low_slow'] and this_bin['period_low_fast'] > window[idx]:
                    # make sure is 20-day low, AND previous low is at least 4 days earlier
                    # print('>>>>> is Turtle Soup? {} {} prev trough {} curr_low {} last_price {}'.format(this_bin['low'] < window[idx], this_bin['close'] > window[idx], window[idx], this_bin['low'], this_bin['close']))
                    
                    # BUY outright - price penetrated previous low but has closed above it
                    if this_bin['low'] < window[idx] and this_bin['close'] > window[idx]:
                        # adjust entry price if stop and entry prices are too close, to minimise excessive price whipsaws
                        if this_bin['low'] < (window[idx] * (1-STOP_LOSS_PCT_FLOOR)):  entry_price = window[idx]
                        else:
                            adj_price = this_bin['low'] / (1-STOP_LOSS_PCT_FLOOR)
                            entry_price = math.floor(adj_price/TRADE_SIZING['TICK_SIZE']) * TRADE_SIZING['TICK_SIZE']
                        if not isOrderInTransit and this_bin['close'] > entry_price:
                            # calculate trade size
                            RISK_DOLLAR = BTC_price * TRADE_SIZING['RISK_BTC']
                            TRADE_SIZE_debug = RISK_DOLLAR / abs((this_bin['low'] / entry_price) - 1)
                            print('debug TRADE_SIZE:', TRADE_SIZE_debug)
                            TRADE_SIZE = 10
                            # send order
                            buy_order = client.Order.Order_new(symbol=symbol, orderQty=TRADE_SIZE).result()
                            if buy_order[1].status_code == 200:
                                if symbol in ORDER_TABLE:
                                    del ORDER_TABLE[symbol]
                                trade_detail = 'buy @ {entry_price:6.2f} stop-loss {stop_loss:6.2f}'.format(entry_price=buy_order[0]['avgPx'], stop_loss=this_bin['low'])
                                print(trade_detail)
                                send_trade_notif_email(user_email, 'BitMEX Turtle Soup trade notification', trade_detail)
                                isOrderInTransit = True
                            elif sell_order[1].status_code == 503:
                                logger.error('sell_order status 503, X-RateLimit-Remaining:', sell_order[1].headers['X-RateLimit-Remaining'])
                                sleep(SLEEP_SECONDS)
                            return (ORDER_TABLE, POS_TABLE, isOrderInTransit)
                    # BUY NEXT PERIOD - price penetrated previous low and closed below
                    if this_bin['low'] < window[idx] and this_bin['close'] < window[idx]:
                        stop_loss = recent_data['low'].min()
                        # stop order exists
                        if symbol in ORDER_TABLE:
                            if stop_loss < ORDER_TABLE[symbol]['stop_loss']:
                                print('BUY trade stoploss price updated to {stop_loss:6.2f}'.format(stop_loss=stop_loss))
                            ORDER_TABLE[symbol]['stop_loss'] = stop_loss
                            # price penetrated a lower previous trough
                            if window[idx] < ORDER_TABLE[symbol]['stop_price']:
                                print('stop buy order UPDATE: entry {entry_price:6.2f} stop-loss {stop_loss:6.2f}'.format(entry_price=window[idx], stop_loss=stop_loss))
                                ORDER_TABLE[symbol]['stop_price'] = window[idx]
                                # update timestamp if currently in "plus one" period
                                if ORDER_TABLE[symbol]['timestamp'] == timestamp: ORDER_TABLE[symbol]['timestamp'] = timestamp + td_map[binSize]
                        if symbol not in ORDER_TABLE:   # create new stop order
                            print('stop buy @ {entry_price:6.2f} stop-loss {stop_loss:6.2f} valid for next period'.format(entry_price=window[idx], stop_loss=stop_loss))
                            ORDER_TABLE[symbol] = {'timestamp': timestamp + td_map[binSize], 'side':'BUY', 'stop_price':window[idx], 'stop_loss':stop_loss, 'triggered': False}
                            return (ORDER_TABLE, POS_TABLE, isOrderInTransit)

    # ******************** SELL logic ********************
    if len(local_peaks) > 0 and symbol not in POS_TABLE:
        for idx in local_peaks.index:
            window = local_peaks[idx:local_peaks.index[len(local_peaks)-1]]
            abs_max = window.max()
            abs_max_idx = window.idxmax()
            
            # if not highest peak and occurs prior to highest peak, ignore it
            if window[idx] < abs_max and idx < abs_max_idx:
                continue
            else:
                if this_bin['high'] > this_bin['period_high_slow'] and this_bin['period_high_fast'] < window[idx]:
                    # make sure is 20-day high, AND previous high is at least 4 days earlier
                    # print('>>>>> is Turtle Soup? {} {} prev peak {} curr_high {} last_price {}'.format(this_bin['high'] > window[idx], this_bin['close'] < window[idx], window[idx], this_bin['high'], this_bin['close']))

                    # SELL outright - price penetrated previous high but has closed below it
                    if this_bin['high'] > window[idx] and this_bin['close'] < window[idx]:
                        # adjust entry price if stop and entry prices are too close, to minimise excessive price whipsaws
                        if this_bin['high'] > (window[idx] * (1+STOP_LOSS_PCT_FLOOR)):  entry_price = window[idx]
                        else:
                            adj_price = this_bin['high'] / (1+STOP_LOSS_PCT_FLOOR)
                            entry_price = math.ceil(adj_price/TRADE_SIZING['TICK_SIZE']) * TRADE_SIZING['TICK_SIZE']
                        if not isOrderInTransit and this_bin['close'] < entry_price:
                            # calculate trade size
                            RISK_DOLLAR = BTC_price * TRADE_SIZING['RISK_BTC']
                            TRADE_SIZE_debug = RISK_DOLLAR / abs((this_bin['high'] / entry_price) - 1)
                            print('debug TRADE_SIZE:', TRADE_SIZE_debug)                        
                            TRADE_SIZE = 10
                            # send order
                            sell_order = client.Order.Order_new(symbol=symbol, orderQty=-TRADE_SIZE).result()
                            if sell_order[1].status_code == 200:
                                if symbol in ORDER_TABLE:
                                    del ORDER_TABLE[symbol]
                                trade_detail = 'sell @ {entry_price:6.2f} stop-loss {stop_loss:6.2f}'.format(entry_price=sell_order[0]['avgPx'], stop_loss=this_bin['high'])
                                print(trade_detail)
                                send_trade_notif_email(user_email, 'BitMEX Turtle Soup trade notification', trade_detail)
                                isOrderInTransit = True
                            elif sell_order[1].status_code == 503:
                                logger.error('sell_order status 503, X-RateLimit-Remaining:', sell_order[1].headers['X-RateLimit-Remaining'])
                                sleep(SLEEP_SECONDS)
                            return (ORDER_TABLE, POS_TABLE, isOrderInTransit)
                    # SELL NEXT PERIOD - price penetrated previous high and closed above
                    if this_bin['high'] > window[idx] and this_bin['close'] > window[idx]:
                        stop_loss = recent_data['high'].max()
                        # stop order exists
                        if symbol in ORDER_TABLE:
                            if stop_loss > ORDER_TABLE[symbol]['stop_loss']:
                                print('SELL trade stoploss price updated to {stop_loss:6.2f}'.format(stop_loss=stop_loss))
                            ORDER_TABLE[symbol]['stop_loss'] = stop_loss
                            # price penetrated a higher previous peak
                            if window[idx] > ORDER_TABLE[symbol]['stop_price']:
                                print('stop sell order UPDATE: entry {entry_price:6.2f} stop-loss {stop_loss:6.2f}'.format(entry_price=window[idx], stop_loss=stop_loss))
                                ORDER_TABLE[symbol]['stop_price'] = window[idx]
                                # update timestamp if currently in "plus one" period
                                if ORDER_TABLE[symbol]['timestamp'] == timestamp: ORDER_TABLE[symbol]['timestamp'] = timestamp + td_map[binSize]
                        if symbol not in ORDER_TABLE:   # create new stop order
                            print('stop sell @ {entry_price:6.2f} stop-loss {stop_loss:6.2f} valid for next period'.format(entry_price=window[idx], stop_loss=stop_loss))
                            ORDER_TABLE[symbol] = {'timestamp':timestamp + td_map[binSize], 'side':'SELL', 'stop_price':window[idx], 'stop_loss':stop_loss, 'triggered': False}
                            return (ORDER_TABLE, POS_TABLE, isOrderInTransit)
    return (ORDER_TABLE, POS_TABLE, isOrderInTransit)


# def send_order(symbol, orderQty, ORDER_TABLE, callback=None, price = None, stopPx = None, stop_loss = None):
#     if price is None and stopPx is None:
#         new_order = client.Order.Order_new(symbol=symbol, orderQty=orderQty).result()
#         if new_order[1].status_code == 200:
#             print(printMsg)
#             callback(symbol, entry_price=new_order[0]['avgPx'], stop_loss, ORDER_TABLE)
#     return ORDER_TABLE

# def order_filled(symbol, entry_price, stop_loss, ORDER_TABLE):
#     if symbol in ORDER_TABLE:
#         del return ORDER_TABLE[symbol]
#     trade_detail = 'buy @ {entry_price:6.2f} stop-loss {stop_loss:6.2f}'.format(entry_price=entry_price,stop_loss=stop_loss)
#     print(trade_detail)
#     send_trade_notif_email(user_email, 'BitMEX Turtle Soup trade notification', trade_detail)
#     return ORDER_TABLE
