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
from datetime import datetime, timedelta
from config import binSize, binSize2, td_map, STOP_LOSS_PCT_FLOOR, user_email
from utils import send_trade_notif_email
import sys

client = bitmex.bitmex(test=False, api_key=api_key, api_secret=api_secret)

recent_timestamp = None
recent_timestamp2 = None
BTC_price = None

def manage_position(symbol, POS_TABLE, this_bin):
    global recent_timestamp
    rr_ratio = 0
    if symbol in POS_TABLE:
        if POS_TABLE[symbol]['remain_qty'] == 0: # Flat
            # pos = client.Position.Position_get(filter=json.dumps({'symbol': symbol})).result()[0][0]
            del POS_TABLE[symbol]
            print('<<<<<<<< ------- Position closed! ------- >>>>>>>>>')
            return POS_TABLE

        if POS_TABLE[symbol]['quantity'] > 0: # BUY side
            reward = this_bin['close'] - POS_TABLE[symbol]['entry_price']
            rr_ratio = reward / POS_TABLE[symbol]['risk']
            # raise stop_loss to entry_price if rr_ratio > 2.0
            if rr_ratio >= 2.0 and POS_TABLE[symbol]['stop_loss'] < POS_TABLE[symbol]['entry_price']:
                client.Order.Order_amend(orderID=POS_TABLE[symbol]['orderID'], stopPx=POS_TABLE[symbol]['entry_price'], orderQty=-POS_TABLE[symbol]['remain_qty']).result()
                POS_TABLE[symbol]['stop_loss'] = POS_TABLE[symbol]['entry_price']
            # raise stop_loss to rr3 if rr_ratio > 6.0
            elif rr_ratio >= 6.0 and POS_TABLE[symbol]['stop_loss'] == POS_TABLE[symbol]['entry_price']:
                new_stopPx = POS_TABLE[symbol]['entry_price'] + (3 * POS_TABLE[symbol]['risk'])
                client.Order.Order_amend(orderID=POS_TABLE[symbol]['orderID'], stopPx=new_stopPx, orderQty=-POS_TABLE[symbol]['remain_qty']).result()
                POS_TABLE[symbol]['stop_loss'] = new_stopPx
        if POS_TABLE[symbol]['quantity'] < 0: # SELL side
            reward = POS_TABLE[symbol]['entry_price'] - this_bin['close']
            rr_ratio = reward / POS_TABLE[symbol]['risk']
            # lower stop_loss to entry_price if rr_ratio > 2.0
            if rr_ratio >= 2.0 and POS_TABLE[symbol]['stop_loss'] > POS_TABLE[symbol]['entry_price']:
                client.Order.Order_amend(orderID=POS_TABLE[symbol]['orderID'], stopPx=POS_TABLE[symbol]['entry_price'], orderQty=-POS_TABLE[symbol]['remain_qty']).result()
                POS_TABLE[symbol]['stop_loss'] = POS_TABLE[symbol]['entry_price']
            # lower stop_loss to rr3 if rr_ratio > 6.0
            if rr_ratio >= 6.0 and POS_TABLE[symbol]['stop_loss'] == POS_TABLE[symbol]['entry_price']:
                new_stopPx = POS_TABLE[symbol]['entry_price'] - (3 * POS_TABLE[symbol]['risk'])
                client.Order.Order_amend(orderID=POS_TABLE[symbol]['orderID'], stopPx=new_stopPx, orderQty=-POS_TABLE[symbol]['remain_qty']).result()
                POS_TABLE[symbol]['stop_loss'] = new_stopPx
        
        # report position every X minutes
        now_timestamp = datetime.now()
        if recent_timestamp is None or (now_timestamp - recent_timestamp) >= timedelta(minutes = 2):
            recent_timestamp = now_timestamp
            if abs(POS_TABLE[symbol]['remain_qty']) > 0:
                print('Position {symbol} {pos} >> RR_ratio {rr_ratio:.2f} Entry_price {entry_price} Last_price {last_price}'.format(symbol=symbol, pos=POS_TABLE[symbol]['remain_qty'], rr_ratio=rr_ratio, entry_price=POS_TABLE[symbol]['entry_price'], last_price=this_bin['close']))

    return POS_TABLE


def trade_entry(symbol, TRADE_SIZING, ORDER_TABLE, POS_TABLE, local_troughs, local_peaks, this_bin, timestamp):
    global recent_timestamp2, BTC_price
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
                stop_loss = min(ORDER_TABLE[symbol]['stop_loss'], this_bin['low'])

                # adjust entry price if stop and entry prices are too close, to minimise excessive price whipsaws
                if stop_loss < (stop_price * (1-STOP_LOSS_PCT_FLOOR)):    entry_price = stop_price
                else:
                    adj_price = stop_loss / (1-STOP_LOSS_PCT_FLOOR)
                    entry_price = math.floor(adj_price/TRADE_SIZING['TICK_SIZE']) * TRADE_SIZING['TICK_SIZE']
                
                if this_bin['high'] > entry_price:  # TRIGGER STOP BUY
                    ORDER_TABLE[symbol]['triggered'] = True
                    # calculate trade size
                    RISK_DOLLAR = BTC_price * TRADE_SIZING['RISK_BTC']
                    TRADE_SIZE_debug = RISK_DOLLAR / abs((this_bin['low'] / entry_price) - 1)
                    print('debug TRADE_SIZE:', TRADE_SIZE_debug)
                    TRADE_SIZE = 10
                    # send order
                    buy_order = client.Order.Order_new(symbol=symbol, orderQty=TRADE_SIZE).result()
                    stop_loss_order = client.Order.Order_new(symbol=symbol, stopPx=stop_loss-TRADE_SIZING['TICK_SIZE'], orderQty=-TRADE_SIZE).result()
                    POS_TABLE[symbol] = {'timestamp':timestamp, 'quantity': TRADE_SIZE, 'remain_qty': TRADE_SIZE, 'entry_price': buy_order[0]['avgPx'], 'stop_loss': stop_loss, 'risk':entry_price-stop_loss, 'orderID': stop_loss_order[0]['orderID']}
                    trade_detail = 'triggered stop buy @ {entry_price:6.2f} stop-loss {stop_loss:6.2f}'.format(entry_price=buy_order[0]['avgPx'], stop_loss=stop_loss)
                    print(trade_detail)
                    send_trade_notif_email(user_email, 'BitMEX Turtle Soup trade notification', trade_detail)
                    return (ORDER_TABLE, POS_TABLE)

            if ORDER_TABLE[symbol]['side'] == 'SELL':
                stop_loss = max(ORDER_TABLE[symbol]['stop_loss'], this_bin['high'])

                # adjust entry price if stop-loss and entry prices are too close, to minimise excessive price whipsaws
                if stop_loss > (stop_price * (1+STOP_LOSS_PCT_FLOOR)):    entry_price = stop_price
                else:
                    adj_price = stop_loss / (1+STOP_LOSS_PCT_FLOOR)
                    entry_price = math.ceil(adj_price/TRADE_SIZING['TICK_SIZE']) * TRADE_SIZING['TICK_SIZE']

                if this_bin['low'] < entry_price:   # TRIGGER STOP SELL
                    ORDER_TABLE[symbol]['triggered'] = True
                    # calculate trade size
                    RISK_DOLLAR = BTC_price * TRADE_SIZING['RISK_BTC']
                    TRADE_SIZE_debug = RISK_DOLLAR / abs((this_bin['high'] / entry_price) - 1)
                    print('debug TRADE_SIZE:', TRADE_SIZE_debug)
                    TRADE_SIZE = 10
                    # send order
                    sell_order = client.Order.Order_new(symbol=symbol, orderQty=-TRADE_SIZE).result()
                    stop_loss_order = client.Order.Order_new(symbol=symbol, stopPx=stop_loss+TRADE_SIZING['TICK_SIZE'], orderQty=TRADE_SIZE).result()
                    POS_TABLE[symbol] = {'timestamp':timestamp, 'quantity': -TRADE_SIZE, 'remain_qty': -TRADE_SIZE, 'entry_price': sell_order[0]['avgPx'], 'stop_loss': stop_loss, 'risk':stop_loss-entry_price, 'orderID': stop_loss_order[0]['orderID']}

                    trade_detail = 'triggered stop sell @ {entry_price:6.2f} stop-loss {stop_loss:6.2f}'.format(entry_price=sell_order[0]['avgPx'], stop_loss=stop_loss)
                    print(trade_detail)
                    send_trade_notif_email(user_email, 'BitMEX Turtle Soup trade notification', trade_detail)
                    return (ORDER_TABLE, POS_TABLE)

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
                        if this_bin['close'] > entry_price:
                            # calculate trade size
                            RISK_DOLLAR = BTC_price * TRADE_SIZING['RISK_BTC']
                            TRADE_SIZE_debug = RISK_DOLLAR / abs((this_bin['low'] / entry_price) - 1)
                            print('debug TRADE_SIZE:', TRADE_SIZE_debug)
                            TRADE_SIZE = 10
                            # send order
                            buy_order = client.Order.Order_new(symbol=symbol, orderQty=TRADE_SIZE).result()
                            stop_loss_order = client.Order.Order_new(symbol=symbol, stopPx=this_bin['low']-TRADE_SIZING['TICK_SIZE'], orderQty=-TRADE_SIZE).result()
                            POS_TABLE[symbol] = {'timestamp':timestamp, 'quantity': TRADE_SIZE, 'remain_qty': TRADE_SIZE, 'entry_price': buy_order[0]['avgPx'], 'stop_loss': this_bin['low'], 'risk':entry_price-this_bin['low'], 'orderID': stop_loss_order[0]['orderID']}
                            if symbol in ORDER_TABLE:
                                del ORDER_TABLE[symbol]

                            trade_detail = 'buy @ {entry_price:6.2f} stop-loss {stop_loss:6.2f}'.format(entry_price=buy_order[0]['avgPx'], stop_loss=this_bin['low'])
                            print(trade_detail)
                            send_trade_notif_email(user_email, 'BitMEX Turtle Soup trade notification', trade_detail)
                            return (ORDER_TABLE, POS_TABLE)
                    # BUY NEXT PERIOD - price penetrated previous low and closed below
                    if this_bin['low'] < window[idx] and this_bin['close'] < window[idx]:
                        if symbol in ORDER_TABLE and window[idx] < ORDER_TABLE[symbol]['stop_price']:
                            # update order if stop order exists already - price penetrated a lower previous trough
                            print('stop buy order UPDATE: entry {entry_price:6.2f} stop-loss {stop_loss:6.2f}'.format(entry_price=window[idx], stop_loss=this_bin['low']))
                            ORDER_TABLE[symbol]['stop_price'] = window[idx]
                            ORDER_TABLE[symbol]['stop_loss'] = min(ORDER_TABLE[symbol]['stop_loss'], this_bin['low'])
                            # update timestamp if currently in "plus one" period
                            if ORDER_TABLE[symbol]['timestamp'] == timestamp: ORDER_TABLE[symbol]['timestamp'] = timestamp + td_map[binSize]
                        if symbol in ORDER_TABLE and this_bin['low'] < ORDER_TABLE[symbol]['stop_loss']:
                            # update stop-loss price if new low is made in current period
                            print('BUY trade stoploss price updated to {stop_loss:6.2f}'.format(stop_loss=this_bin['low']))
                            ORDER_TABLE[symbol]['stop_loss'] = this_bin['low']
                        if symbol not in ORDER_TABLE:   # create new stop order
                            print('stop buy @ {entry_price:6.2f} stop-loss {stop_loss:6.2f} valid for next period'.format(entry_price=window[idx], stop_loss=this_bin['low']))
                            ORDER_TABLE[symbol] = {'timestamp': timestamp + td_map[binSize], 'side':'BUY', 'stop_price':window[idx], 'stop_loss':this_bin['low'], 'triggered': False}
                            return (ORDER_TABLE, POS_TABLE)

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
                        if this_bin['close'] < entry_price:
                            # calculate trade size
                            RISK_DOLLAR = BTC_price * TRADE_SIZING['RISK_BTC']
                            TRADE_SIZE_debug = RISK_DOLLAR / abs((this_bin['high'] / entry_price) - 1)
                            print('debug TRADE_SIZE:', TRADE_SIZE_debug)                        
                            TRADE_SIZE = 10
                            # send order
                            sell_order = client.Order.Order_new(symbol=symbol, orderQty=-TRADE_SIZE).result()
                            stop_loss_order = client.Order.Order_new(symbol=symbol, stopPx=this_bin['high']+TRADE_SIZING['TICK_SIZE'], orderQty=TRADE_SIZE).result()
                            POS_TABLE[symbol] = {'timestamp':timestamp, 'quantity': -TRADE_SIZE, 'remain_qty': -TRADE_SIZE, 'entry_price': sell_order[0]['avgPx'], 'stop_loss': this_bin['high'], 'risk':this_bin['high']-entry_price, 'orderID': stop_loss_order[0]['orderID']}
                            if symbol in ORDER_TABLE:
                                del ORDER_TABLE[symbol]
                            trade_detail = 'sell @ {entry_price:6.2f} stop-loss {stop_loss:6.2f}'.format(entry_price=sell_order[0]['avgPx'], stop_loss=this_bin['high'])
                            print(trade_detail)
                            send_trade_notif_email(user_email, 'BitMEX Turtle Soup trade notification', trade_detail)
                            return (ORDER_TABLE, POS_TABLE)
                    # SELL NEXT PERIOD - price penetrated previous high and closed above
                    if this_bin['high'] > window[idx] and this_bin['close'] > window[idx]:
                        if symbol in ORDER_TABLE and window[idx] > ORDER_TABLE[symbol]['stop_price']:
                            # update order if stop order exists already - price penetrated a higher previous peak
                            print('stop sell order UPDATE: entry {entry_price:6.2f} stop-loss {stop_loss:6.2f}'.format(entry_price=window[idx], stop_loss=this_bin['high']))
                            ORDER_TABLE[symbol]['stop_price'] = window[idx]
                            ORDER_TABLE[symbol]['stop_loss'] = max(ORDER_TABLE[symbol]['stop_loss'], this_bin['high'])
                            # update timestamp if currently in "plus one" period
                            if ORDER_TABLE[symbol]['timestamp'] == timestamp: ORDER_TABLE[symbol]['timestamp'] = timestamp + td_map[binSize]
                        if symbol in ORDER_TABLE and this_bin['high'] > ORDER_TABLE[symbol]['stop_loss']:
                            # update stop-loss price if new high is made in current period
                            print('SELL trade stoploss price updated to {stop_loss:6.2f}'.format(stop_loss=this_bin['high']))
                            ORDER_TABLE[symbol]['stop_loss'] = this_bin['high']
                        if symbol not in ORDER_TABLE:   # create new stop order
                            print('stop sell @ {entry_price:6.2f} stop-loss {stop_loss:6.2f} valid for next period'.format(entry_price=window[idx], stop_loss=this_bin['high']))
                            ORDER_TABLE[symbol] = {'timestamp':timestamp + td_map[binSize], 'side':'SELL', 'stop_price':window[idx], 'stop_loss':this_bin['high'], 'triggered': False}
                            return (ORDER_TABLE, POS_TABLE)
    return (ORDER_TABLE, POS_TABLE)
