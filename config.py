from datetime import datetime, timedelta

binSize = '1h'
binSize2 = '1h'
td_map = {'1m':timedelta(minutes=1), '5m':timedelta(minutes=5), '1h':timedelta(hours=1), '4h':timedelta(hours=4)}
LOOKBACK_PERIOD = 340
ENTRY_BUFFER = 0.001
STOP_LOSS_PCT_FLOOR = 0.003
STOP_LOSS_PCT_CEIL = 0.0200
PCT_RISK_PER_TRADE = 0.01   # percentage to risk on each trade
takeProfit1 = 1.50
takeProfit2 = 1.75

user_email='xxxxx@gmail.com'

SLEEP_SECONDS = 0.90