import websocket
import threading
import traceback
import time
import json
import logging
import urllib
import math
from util.api_key import generate_nonce, generate_signature


class BitMEXWebsocket:

    def __init__(self, endpoint, symbol, api_key=None, api_secret=None, callback=None):
        '''Connect to the websocket and initialize data stores.'''
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Initializing WebSocket.")

        self.endpoint = endpoint
        self.symbol = symbol
        self.callback = callback

        if api_key is not None and api_secret is None:
            raise ValueError('api_secret is required if api_key is provided')
        if api_key is None and api_secret is not None:
            raise ValueError('api_key is required if api_secret is provided')

        self.api_key = api_key
        self.api_secret = api_secret

        self.data = {}
        self.message = []
        self.keys = {}
        self.exited = False

        # We can subscribe right in the connection querystring, so let's build that.
        # Subscribe to all pertinent endpoints
        wsURL = self.__get_url()
        self.logger.info("Connecting to %s" % wsURL)
        self.__connect(wsURL, symbol)
        self.logger.info('Connected to WS.')

        # Connected. Wait for partials
        self.__wait_for_symbol(symbol)
        if api_key:
            self.__wait_for_account()
        self.logger.info('Got all market data. Starting.')

    def __connect(self, wsURL, symbol):
        '''Connect to the websocket in a thread.'''
        self.logger.debug("Starting thread")

        local_threads = []

        self.ws = websocket.WebSocketApp(wsURL,
                                         on_message=self.__on_message,
                                         on_close=self.__on_close,
                                         on_open=self.__on_open,
                                         on_error=self.__on_error,
                                         header=self.__get_auth())

        self.wst = threading.Thread(target=lambda: self.ws.run_forever())
        self.wst.daemon = True
        local_threads.append(self.wst)
        for thread in local_threads:
            thread.start()
        self.logger.debug("Started thread")

        # Wait for connect before continuing
        conn_timeout = 5
        while not self.ws.sock or not self.ws.sock.connected and conn_timeout:
            time.sleep(1)
            conn_timeout -= 1
        if not conn_timeout:
            self.logger.error("Couldn't connect to WS! Exiting.")
            self.exit()
            raise websocket.WebSocketTimeoutException('Couldn\'t connect to WS! Exiting.')

    def __get_auth(self):
        '''Return auth headers. Will use API Keys if present in settings.'''
        if self.api_key:
            self.logger.info("Authenticating with API Key.")
            # Refer to BitMEX/sample-market-maker on Github
            expires = int(round(time.time()) + 5)  # 5s grace period in case of clock skew
            return [
                "api-expires: " + str(expires),
                "api-signature: " + generate_signature(self.api_secret, 'GET', '/realtime', expires, ''),
                "api-key:" + self.api_key
            ]
        else:
            self.logger.info("Not authenticating.")
            return []

    def __get_url(self):
            '''
            Generate a connection URL. We can define subscriptions right in the querystring.
            Most subscription topics are scoped by the symbol we're listening to.
            '''

            # You can sub to orderBookL2 for all levels, or orderBook10 for top 10 levels & save bandwidth
            symbolSubs = ["execution", "instrument", "order", "position", "trade"]
            genericSubs = ["margin"]

            subscriptions = [sub + ':' + self.symbol for sub in symbolSubs]
            subscriptions += genericSubs

            urlParts = list(urllib.parse.urlparse(self.endpoint))
            urlParts[0] = urlParts[0].replace('http', 'ws')
            urlParts[2] = "/realtime?subscribe={}".format(','.join(subscriptions))
            return urllib.parse.urlunparse(urlParts)

    def __wait_for_account(self):
        '''On subscribe, this data will come down. Wait for it.'''
        # Wait for the keys to show up from the ws
        while not {'margin', 'position', 'order'} <= set(self.data):
            time.sleep(0.1)
        # print('account partials arrived')

    def __wait_for_symbol(self, symbol):
        '''On subscribe, this data will come down. Wait for it.'''
        while not {'instrument', 'trade'} <= set(self.data):
            time.sleep(1.0)
        # print('symbol partials arrived')

    def __on_message(self, ws, message):
        '''Handler for parsing WS messages.'''
        message = json.loads(message)
        self.logger.debug(json.dumps(message))

        table = message['table'] if 'table' in message else None
        # invoke callback (handle_data.....)
        if self.callback:
            if table == 'trade':
                self.callback(message)
            if table == 'position' and message['action'] == 'update':
                self.callback(message)

        # if table == 'trade' and self.callback:
        #     self.callback(message['data'])
        # if table == 'position' and message['action'] == 'update':
        #     print('position:', message, '\n')


    def __on_error(self, ws, error):
        '''Called on fatal websocket errors. We exit on these.'''
        if not self.exited:
            self.logger.error("Error : %s" % error)
            print('error type', type(error))
            print('print error', error)
            print('attempt to reconnect')
            wsURL = self.__get_url()
            self.__connect(wsURL, self.symbol)
            # raise websocket.WebSocketException(error)


    def __on_open(self, ws):
        '''Called when the WS opens.'''
        self.logger.debug("Websocket Opened.")
        

    def __on_close(self, ws):
        '''Called on websocket close.'''
        self.logger.info('Websocket Closed')

