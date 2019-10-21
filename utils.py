from environs import Env
env = Env()
env.read_env()
api_key=env("BITMEX_API_KEY")
api_secret=env("BITMEX_API_SECRET")

import bitmex
import pandas as pd
from datetime import datetime, timedelta
from time import sleep



client = bitmex.bitmex(test=False, api_key=api_key, api_secret=api_secret)

def get_klines_df(symbol, binSize, startDate, endDate=None, partial=True):
    # binSize [1m, 5m, 1h, 1d]
    count = 500
    d = timedelta(seconds=1)
    startDate = datetime.strptime(startDate, '%Y-%m-%d %H:%M:%S')
    if endDate is None: endDate = datetime.now()
    else:               endDate = datetime.strptime(endDate, '%Y-%m-%d %H:%M:%S')

    klines = client.Trade.Trade_getBucketed(symbol=symbol, binSize=binSize, startTime=startDate, endTime=endDate, count=count, partial=partial).result()
    df = pd.DataFrame(klines[0])
    # print('RateLimit-Remaining', klines[1].headers['X-RateLimit-Remaining'])

    while len(klines[0]) == count:
        # sleep(0.7)
        klines = client.Trade.Trade_getBucketed(symbol=symbol, binSize=binSize, startTime=(pd.to_datetime((df['timestamp'] + d).values[-1])), endTime=endDate, count=count, partial=partial).result()
        # print(klines[0], len(klines[0]))
        if len(klines[0]) > 0:
            df = df.append(pd.DataFrame(klines[0]), ignore_index=True)
    
    df = df[['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'trades', 'volume', 'vwap', 'lastSize', 'turnover', 'homeNotional', 'foreignNotional']]
    return df

def get_instrument(symbol):
    instr = client.Instrument.Instrument_get(symbol=symbol).result()[0][0]
    return instr


import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_trade_notif_email(toAddr, subject, body):
    fromAddr = env("EMAIL_USER")
    msg = MIMEMultipart()
    msg['From'] = fromAddr
    msg['To'] = toAddr
    msg['Subject'] = subject
    body = body
    msg.attach(MIMEText(body, 'plain'))
    
    # server = smtplib.SMTP('smtp.office365.com', 587)
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()

    #Next, log in to the server
    server.login(fromAddr, env("EMAIL_PW"))

    #Send the mail
    text = msg.as_string()
    server.sendmail(fromAddr, toAddr, text)
    server.quit()

