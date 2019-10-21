from environs import Env
env = Env()
env.read_env()
api_key=env("BITMEX_API_KEY")
api_secret=env("BITMEX_API_SECRET")

import bitmex
import logging
import math
from config import *
from datetime import datetime, timedelta
from time import sleep

logger = logging.getLogger(__name__)
client = bitmex.bitmex(test=False, api_key=api_key, api_secret=api_secret)

recent_timestamp = None
recent_timestamp2 = None
recent_low = None
recent_high = None
BTC_price = None
cooling_down = True

def manage_position(symbol, TRADE_SIZING, ORDER_QUEUE, POS_TABLE, recent_bin1, isOrderInTransit):
    global recent_timestamp
    rr_ratio = 0
    if symbol in POS_TABLE:
        if POS_TABLE[symbol]['remain_qty'] is not None and POS_TABLE[symbol]['remain_qty'] == 0: # Flat
            try:
                logging.info('cancelling stoploss order {} if exists'.format(POS_TABLE[symbol]['stoploss_orderID'])) 
                cancel1 = client.Order.Order_cancel(orderID=POS_TABLE[symbol]['stoploss_orderID']).result()
            except Exception as e:
                logging.info('cancel order error: {} continue'.format(e))
                pass
            try:
                logging.info('cancelling takeProfit order {} if exists'.format(POS_TABLE[symbol]['takeProfit_orderID1'])) 
                cancel2 = client.Order.Order_cancel(orderID=POS_TABLE[symbol]['takeProfit_orderID1']).result()
            except Exception as e:
                logging.info('cancel order error: {} continue'.format(e))
                pass
            try:
                logging.info('cancelling takeProfit order {} if exists'.format(POS_TABLE[symbol]['takeProfit_orderID2'])) 
                cancel3 = client.Order.Order_cancel(orderID=POS_TABLE[symbol]['takeProfit_orderID2']).result()
            except Exception as e:
                logging.info('cancel order error: {} continue'.format(e))
                pass
            recent_low = None
            recent_high = None
            # logging.info('CANCEL ORDER\n {}'.format(cancel1))
            # logging.info('CANCEL ORDER\n {}'.format(cancel2))
            # logging.info('CANCEL ORDER\n {}'.format(cancel3))
            logging.info('<<<<<<<< ------- Position closed! Orders cancelled ------- >>>>>>>>>')
            del POS_TABLE[symbol]
            return (ORDER_QUEUE, POS_TABLE, isOrderInTransit)
        if not isOrderInTransit and POS_TABLE[symbol]['updateOrderQty']:  # need to update stop_loss order quantity
            ORDER_QUEUE.put({'orderID': POS_TABLE[symbol]['stoploss_orderID'], 'orderQty':POS_TABLE[symbol]['remain_qty'], 'price': None, 'stopPx': None})
            isOrderInTransit = True
            POS_TABLE[symbol]['updateOrderQty'] = False

        TRADE_SIZE = POS_TABLE[symbol]['remain_qty']
        STOP_LOSS = POS_TABLE[symbol]['stop_loss']
        TICK_SIZE = TRADE_SIZING['TICK_SIZE']

        if POS_TABLE[symbol]['quantity'] is not None and POS_TABLE[symbol]['quantity'] > 0: # BUY side
            reward = recent_bin1['close'] - POS_TABLE[symbol]['entry_price']
            rr_ratio = reward / POS_TABLE[symbol]['risk']

            # create (SELL) take profit order if doesn't exist
            if not isOrderInTransit and POS_TABLE[symbol]['takeProfit_orderID1'] is None:
                # close 50% position at TP1
                logging.info('debug take profit (before) {} {}'.format(isOrderInTransit, POS_TABLE[symbol]['takeProfit_orderID1']))
                target_price_raw = POS_TABLE[symbol]['entry_price'] + (takeProfit1 * POS_TABLE[symbol]['risk'])
                target_price = math.ceil(target_price_raw/TICK_SIZE) * TICK_SIZE
                ORDER_QUEUE.put({'orderType': 'tp1', 'orderID': None, 'orderQty':-TRADE_SIZE*0.5, 'price':target_price, 'stopPx': None })
                POS_TABLE[symbol]['takeProfit_orderID1'] = 'pending'
                logging.info('debug take profit (after) {} {}'.format(isOrderInTransit, POS_TABLE[symbol]['takeProfit_orderID1']))
            if not isOrderInTransit and POS_TABLE[symbol]['takeProfit_orderID2'] is None:
                # close remaining 50% position at TP2
                logging.info('debug take profit (before) {} {}'.format(isOrderInTransit, POS_TABLE[symbol]['takeProfit_orderID2']))
                target_price_raw = POS_TABLE[symbol]['entry_price'] + (takeProfit2 * POS_TABLE[symbol]['risk'])
                target_price = math.ceil(target_price_raw/TICK_SIZE) * TICK_SIZE
                ORDER_QUEUE.put({'orderType': 'tp2', 'orderID': None, 'orderQty':-TRADE_SIZE*0.5, 'price':target_price, 'stopPx': None })
                POS_TABLE[symbol]['takeProfit_orderID2'] = 'pending'
                logging.info('debug take profit (after) {} {}'.format(isOrderInTransit, POS_TABLE[symbol]['takeProfit_orderID2']))
            # create (SELL) stop loss order if doesn't exist
            if not isOrderInTransit and POS_TABLE[symbol]['stoploss_orderID'] is None:
                logging.info('debug stop loss order (before) {} {}'.format(isOrderInTransit, POS_TABLE[symbol]['stoploss_orderID']))
                ORDER_QUEUE.put({'orderType': 'stoploss', 'orderID': None, 'orderQty':-TRADE_SIZE, 'price': None, 'stopPx':STOP_LOSS-TICK_SIZE })
                POS_TABLE[symbol]['stoploss_orderID'] = 'pending'
                isOrderInTransit = True
                logging.info('debug stop loss order (after) {} {}'.format(isOrderInTransit, POS_TABLE[symbol]['stoploss_orderID']))
            # raise stop_loss to entry_price if rr_ratio > 1.0
            # if not isOrderInTransit and rr_ratio >= 1.20 and STOP_LOSS < POS_TABLE[symbol]['entry_price']:
            #     logging.info('raise stop_loss to entry_price if rr_ratio > 1.0')
            #     new_stopPx_raw = POS_TABLE[symbol]['entry_price']
            #     new_stopPx = math.ceil(new_stopPx_raw/TICK_SIZE) * TICK_SIZE
            #     ORDER_QUEUE.put({ 'orderID':POS_TABLE[symbol]['stoploss_orderID'], 'stopPx':new_stopPx, 'orderQty':-TRADE_SIZE, 'price': None })
            #     isOrderInTransit = True
            # # raise stop_loss to rr3 if rr_ratio > 6.0
            # elif not isOrderInTransit and rr_ratio >= 6.0 and STOP_LOSS == POS_TABLE[symbol]['entry_price']:
            #     new_stopPx_raw = POS_TABLE[symbol]['entry_price'] + (3 * POS_TABLE[symbol]['risk'])
            #     new_stopPx = math.ceil(new_stopPx_raw/TICK_SIZE) * TICK_SIZE
            #     ORDER_QUEUE.put({ 'orderID':POS_TABLE[symbol]['stoploss_orderID'], 'stopPx':new_stopPx, 'orderQty':-TRADE_SIZE, 'price': None })
            #     isOrderInTransit = True

        if POS_TABLE[symbol]['quantity'] is not None and POS_TABLE[symbol]['quantity'] < 0: # SELL side
            reward = POS_TABLE[symbol]['entry_price'] - recent_bin1['close']
            rr_ratio = reward / POS_TABLE[symbol]['risk']

            # create (BUY) take profit order if doesn't exist
            if not isOrderInTransit and POS_TABLE[symbol]['takeProfit_orderID1'] is None:
                # close 50% position at TP1
                logging.info('debug take profit order (before) {} {}'.format(isOrderInTransit, POS_TABLE[symbol]['takeProfit_orderID1']))
                target_price_raw = POS_TABLE[symbol]['entry_price'] - (takeProfit1 * POS_TABLE[symbol]['risk'])
                target_price = math.floor(target_price_raw/TICK_SIZE) * TICK_SIZE
                ORDER_QUEUE.put({'orderType': 'tp1', 'orderID': None, 'orderQty':-TRADE_SIZE*0.5, 'price':target_price, 'stopPx': None })
                POS_TABLE[symbol]['takeProfit_orderID1'] = 'pending'
                logging.info('debug take profit order (after) {} {}'.format(isOrderInTransit, POS_TABLE[symbol]['takeProfit_orderID1']))
            if not isOrderInTransit and POS_TABLE[symbol]['takeProfit_orderID2'] is None:
                # close remaining 50% position at TP2
                logging.info('debug take profit order (before) {} {}'.format(isOrderInTransit, POS_TABLE[symbol]['takeProfit_orderID2']))
                target_price_raw = POS_TABLE[symbol]['entry_price'] - (takeProfit2 * POS_TABLE[symbol]['risk'])
                target_price = math.floor(target_price_raw/TICK_SIZE) * TICK_SIZE
                ORDER_QUEUE.put({'orderType': 'tp2', 'orderID': None, 'orderQty':-TRADE_SIZE*0.5, 'price':target_price, 'stopPx': None })
                POS_TABLE[symbol]['takeProfit_orderID2'] = 'pending'
                logging.info('debug take profit order (after) {} {}'.format(isOrderInTransit, POS_TABLE[symbol]['takeProfit_orderID2']))
            # create (BUY) stop loss order if doesn't exist
            if not isOrderInTransit and POS_TABLE[symbol]['stoploss_orderID'] is None:
                logging.info('debug stop loss order (before) {} {}'.format(isOrderInTransit, POS_TABLE[symbol]['stoploss_orderID']))
                ORDER_QUEUE.put({'orderType': 'stoploss', 'orderID': None, 'orderQty':-TRADE_SIZE, 'price': None, 'stopPx':STOP_LOSS+TICK_SIZE })
                POS_TABLE[symbol]['stoploss_orderID'] = 'pending'
                isOrderInTransit = True
                logging.info('debug stop loss order (after) {} {}'.format(isOrderInTransit, POS_TABLE[symbol]['stoploss_orderID']))
            # lower stop_loss to entry_price if rr_ratio > 1.0
            # if not isOrderInTransit and rr_ratio >= 1.20 and STOP_LOSS > POS_TABLE[symbol]['entry_price']:
            #     logging.info('lower stop_loss to entry_price if rr_ratio > 1.0')
            #     new_stopPx_raw = POS_TABLE[symbol]['entry_price']
            #     new_stopPx = math.floor(new_stopPx_raw/TICK_SIZE) * TICK_SIZE
            #     ORDER_QUEUE.put({ 'orderID':POS_TABLE[symbol]['stoploss_orderID'], 'stopPx':new_stopPx, 'orderQty':-TRADE_SIZE, 'price': None })
            #     isOrderInTransit = True
            # # lower stop_loss to rr3 if rr_ratio > 6.0
            # if not isOrderInTransit and rr_ratio >= 6.0 and STOP_LOSS == POS_TABLE[symbol]['entry_price']:
            #     new_stopPx_raw = POS_TABLE[symbol]['entry_price'] - (3 * POS_TABLE[symbol]['risk'])
            #     new_stopPx = math.floor(new_stopPx_raw/TICK_SIZE) * TICK_SIZE
            #     ORDER_QUEUE.put({ 'orderID':POS_TABLE[symbol]['stoploss_orderID'], 'stopPx':new_stopPx, 'orderQty':-TRADE_SIZE, 'price': None })
            #     isOrderInTransit = True

        # report position every X minutes
        now_timestamp = datetime.now()
        if recent_timestamp is None or (now_timestamp - recent_timestamp) >= timedelta(minutes = 2):
            recent_timestamp = now_timestamp
            if POS_TABLE[symbol]['remain_qty'] is not None and abs(POS_TABLE[symbol]['remain_qty']) > 0:
                logging.info('Position {symbol} {pos} >> RR_ratio {rr_ratio:.2f} Entry_price {entry_price} Last_price {last_price}'.format(symbol=symbol, pos=POS_TABLE[symbol]['remain_qty'], rr_ratio=rr_ratio, entry_price=POS_TABLE[symbol]['entry_price'], last_price=recent_bin1['close']))

    return (ORDER_QUEUE, POS_TABLE, isOrderInTransit)


def trade_entry(symbol, TRADE_SIZING, ORDER_QUEUE, POS_TABLE, local_troughs, local_peaks, recent_data, timestamp, isOrderInTransit):
    global recent_timestamp2, BTC_price, recent_low, recent_high, cooling_down
    # recent_bin4 = recent_data.head(1).to_dict('records')[0]    # get t-3 (last) bin
    # recent_bin3 = recent_data.tail(3).head(1).to_dict('records')[0]    # get t-2 bin
    recent_bin2 = recent_data.tail(2).head(1).to_dict('records')[0]    # get t-1 bin
    recent_bin1 = recent_data.tail(1).to_dict('records')[0]    # get current data bin
    isOrderInTransit = isOrderInTransit
    TICK_SIZE = TRADE_SIZING['TICK_SIZE']
    # update BTC price every X minutes - for trade sizing purpose!!!!
    now_timestamp = datetime.now()
    if recent_timestamp2 is None or (now_timestamp - recent_timestamp2) >= timedelta(minutes = 3):
        recent_timestamp2 = now_timestamp
        BTC_price = client.Instrument.Instrument_get(symbol='XBTUSD').result()[0][0]['fairPrice']

    # ******************** BUY logic ********************
    if len(local_troughs) > 0 and symbol not in POS_TABLE:
        for idx in local_troughs.index:
            window = local_troughs[idx:local_troughs.index[len(local_troughs)-1]]
            abs_min = window.min()
            abs_min_idx = window.idxmin()

            # if not lowest trough and occurs prior to lowest trough, ignore it
            if window[idx] > abs_min and idx < abs_min_idx:
                continue
            # make sure is 20-day low, AND previous low is at least 4 days earlier
            signal_new_low1 = recent_bin1['low'] < recent_bin1['period_low_slow'] and recent_bin1['period_low_fast'] > window[idx]
            signal_new_low2 = recent_bin2['isNewLow'] and recent_bin2['period_low_fast'] >= window[idx]
            # signal_new_low3 = recent_bin3['isNewLow'] and recent_bin3['period_low_fast'] >= window[idx]
            # signal_new_low4 = recent_bin4['isNewLow'] and recent_bin4['period_low_fast'] >= window[idx]
            # if signal_new_low1 or signal_new_low2 or signal_new_low3 or signal_new_low4:
            if signal_new_low1 or signal_new_low2:
                if recent_low is None: recent_low = recent_data['low'].min()
                if recent_data['low'].min() < recent_low:
                    recent_low = recent_data['low'].min()
                    cooling_down = False    # lower low (stop loss hit), reset cooldown
                    logging.info('potential BUY order, stop_loss price updated to {}'.format(recent_low))
                if recent_low < (window[idx]*(1-STOP_LOSS_PCT_CEIL)):   # stop loss percent exceed max, no trade
                    continue
                entry_price_raw = window[idx] * (1 + ENTRY_BUFFER)
                
                # BUY outright - price penetrated previous low but has closed above it
                if not cooling_down and recent_low < entry_price_raw and recent_bin1['close'] > entry_price_raw:
                    # adjust entry price if stop and entry prices are too close, to minimise excessive price whipsaws
                    if recent_low < (entry_price_raw * (1-STOP_LOSS_PCT_FLOOR)):  entry_price = entry_price_raw
                    else:
                        adj_price = recent_low / (1-STOP_LOSS_PCT_FLOOR)
                        entry_price = math.floor(adj_price/TICK_SIZE) * TICK_SIZE
                    if not isOrderInTransit and recent_bin1['close'] > entry_price:
                        # calculate trade size
                        RISK_DOLLAR = BTC_price * TRADE_SIZING['RISK_BTC']
                        TRADE_SIZE = math.floor(RISK_DOLLAR / abs((recent_low / entry_price) - 1))
                        logging.info('entry price {} reference trough {} current time {}'.format(entry_price, window[idx], datetime.now()))
                        logging.info('debug TRADE_SIZE: {}'.format(TRADE_SIZE))
                        # send order
                        ORDER_QUEUE.put({'orderType': 'entry', 'orderID': None, 'orderQty':TRADE_SIZE, 'price': None, 'stopPx': None})
                        POS_TABLE[symbol] = {
                            'timestamp':timestamp, 'quantity': None, 'remain_qty': None, 'entry_price': None,
                            'stop_loss': None,'risk': None, 'stoploss_orderID': None,
                            'takeProfit_orderID1': None, 'takeProfit_orderID2': None, 'updateOrderQty': False
                        }
                        isOrderInTransit = True
                        cooling_down = True     # cooling down, cannot open position
                        return (ORDER_QUEUE, POS_TABLE, isOrderInTransit)

    # ******************** SELL logic ********************
    if len(local_peaks) > 0 and symbol not in POS_TABLE:
        for idx in local_peaks.index:
            window = local_peaks[idx:local_peaks.index[len(local_peaks)-1]]
            abs_max = window.max()
            abs_max_idx = window.idxmax()
            
            # if not highest peak and occurs prior to highest peak, ignore it
            if window[idx] < abs_max and idx < abs_max_idx:
                continue
            # make sure is 20-day high, AND previous high is at least 4 days earlier
            signal_new_high1 = recent_bin1['high'] > recent_bin1['period_high_slow'] and recent_bin1['period_high_fast'] < window[idx]
            signal_new_high2 = recent_bin2['isNewHigh'] and recent_bin2['period_high_fast'] <= window[idx]
            # signal_new_high3 = recent_bin3['isNewHigh'] and recent_bin3['period_high_fast'] <= window[idx]
            # signal_new_high4 = recent_bin4['isNewHigh'] and recent_bin4['period_high_fast'] <= window[idx]
            # if signal_new_high1 or signal_new_high2 or signal_new_high3 or signal_new_high4:
            if signal_new_high1 or signal_new_high2:
                if recent_high is None: recent_high = recent_data['high'].max()
                if recent_data['high'].max() > recent_high:
                    recent_high = recent_data['high'].max()
                    cooling_down = False    # higher high (stop loss hit), reset cooldown
                    logging.info('potential SELL order, stop_loss price updated to {}'.format(recent_high))
                if recent_high > (window[idx]*(1+STOP_LOSS_PCT_CEIL)):  # stop loss percent exceed max, no trade
                    continue
                entry_price_raw = window[idx] * (1 - ENTRY_BUFFER)

                # SELL outright - price penetrated previous high but has closed below it
                if not cooling_down and recent_high > entry_price_raw and recent_bin1['close'] < entry_price_raw:
                    # adjust entry price if stop and entry prices are too close, to minimise excessive price whipsaws
                    if recent_high > (entry_price_raw * (1+STOP_LOSS_PCT_FLOOR)):  entry_price = entry_price_raw
                    else:
                        adj_price = recent_high / (1+STOP_LOSS_PCT_FLOOR)
                        entry_price = math.ceil(adj_price/TICK_SIZE) * TICK_SIZE
                    if not isOrderInTransit and recent_bin1['close'] < entry_price:
                        # calculate trade size
                        RISK_DOLLAR = BTC_price * TRADE_SIZING['RISK_BTC']
                        TRADE_SIZE = math.floor(RISK_DOLLAR / abs((recent_high / entry_price) - 1))
                        logging.info('entry price {} reference peak {} current time {}'.format(entry_price, window[idx], datetime.now()))
                        logging.info('debug TRADE_SIZE: {}'.format(TRADE_SIZE))
                        # send order
                        ORDER_QUEUE.put({'orderType': 'entry', 'orderID': None, 'orderQty':-TRADE_SIZE, 'price': None, 'stopPx': None})
                        POS_TABLE[symbol] = {
                            'timestamp':timestamp, 'quantity': None, 'remain_qty': None, 'entry_price': None,
                            'stop_loss': None,'risk': None, 'stoploss_orderID': None,
                            'takeProfit_orderID1': None, 'takeProfit_orderID2': None, 'updateOrderQty': False
                        }
                        isOrderInTransit = True
                        cooling_down = True     # cooling down, cannot open position
                        return (ORDER_QUEUE, POS_TABLE, isOrderInTransit)

    return (ORDER_QUEUE, POS_TABLE, isOrderInTransit)
