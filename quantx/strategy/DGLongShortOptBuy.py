import pandas as pd
import datetime as dt
from copy import copy

# from AlgoTrading import utils, ORDER, TRANSACTION
# from AlgoTrading.Strategies import BaseStrategy

from Exchange.executor import Exchange, Order
from .base_strategy import Strategy, StrategyModes

import streaming_indicators as si
from collections import deque

class TRANSACTION:
    BUY = "BUY"
    SELL = "SELL"




class DGLongShortOptBuy(Strategy):
    '''
    description: identifies long conditions, enters call option , places buy order on strike closest to candle.close(underlying)
    fetch premium candles for the last 3 minutes. get the sl_price as the min of lows of premium candles, place tgt order.
    params:
        symbol: text
        start_time: time
        squareoff_time: time
    '''
    __version__ = '1.0.0'
    STATE_INITIAL = 'INITIAL'
    STATE_SQUAREDOFF = 'SQUAREOFF'

    OPT_TYPES = ['CALL','PUT']


    def __init__(self, *args, data_obj, params):
        super().__init__(*args)
        self.instrument = int(self.universe[0])
        self.tf = dt.timedelta(minutes=1)



    
    def setup(self, t):
        self.state = self.STATE_INITIAL
        # self.instrument = self.trader.get_instrument({'exchange':'NSE', 'symbol':self.symbol})
        if(self.instrument is None):
            self.logger.error(f"No instrument found for symbol '{self.symbol}'")
            raise Exception("InstrumentNotFound")
        
        # list of names of options? No dataframe of all options
        # expiry=0?
        all_options = self.trader.get_instrument({
            'symbol':self.symbol, 'exchange':self.instrument.exchange, 'type':'OPT', 'expiry':0,
        }, return_multiple=True, verbose=False)
        if(all_options is None or len(all_options) == 0):
            self.logger.error("No options found for this instrument.")
            raise Exception("NotFnOInstrument")
        self.all_options = {
            ot: all_options[all_options['opt_type'] == ot]
            for ot in self.OPT_TYPES
        }

        self.lot = 1
        # indicators
        self.RSI = si.RSI(14)
        self.prev_RSI = deque(maxlen=3)
        self.PLUS_DI = si.PLUS_DI(14)
        self.prev_PLUS_DI = deque(maxlen=3)
        self.MINUS_DI = si.PLUS_DI(14)
        self.prev_MINUS_DI = deque(maxlen=3)
        self.BBANDS = si.BBands(14, 2)
        self.last_update_dt = None
        self.open_positions = []
        
        self.update_indicators(t)
        
        return True

    def update_indicators(self, t):
        # function to fetch candles and update indicators
        if(self.last_update_dt is None):
            candles = self.data_obj.fetch_candle(self.instrument, t-dt.timedelta(minutes=5), t, self.tf)
            if(candles is None):
                self.logger.error(f"No historical candles")
                raise Exception("NoHistoricalData")
        else:#if(self.last_update_dt < t):
            candles = self.data_obj.fetch_candle(self.instrument, self.last_update_dt, t, self.tf)
            if(candles is None):
                self.logger.error(f"Live candle not received")
                raise Exception("NoLiveData")
                return False
        if(not isinstance(candles, pd.DataFrame)): candles = pd.DataFrame([candles])
        for _, candle in candles.iterrows():
            rsi = self.RSI.update(candle['close'])
            self.prev_RSI.append(rsi)
            plus_di = self.PLUS_DI.update(candle)
            self.prev_PLUS_DI.append(plus_di)
            minus_di = self.MINUS_DI.update(candle)
            self.prev_MINUS_DI.append(minus_di)
            self.BBANDS.update(candle['close'])
            self.last_update_dt = candle['datetime'] + self.tf

        self.rsi_rc = (self.prev_RSI[-1] - self.prev_RSI[-3]) / self.prev_RSI[-3] * 100
        self.plus_di_rc  = (self.prev_PLUS_DI[-1]  - self.prev_PLUS_DI[-3] ) / self.prev_PLUS_DI[-3]  * 100
        self.minus_di_rc = (self.prev_MINUS_DI[-1] - self.prev_MINUS_DI[-3]) / self.prev_MINUS_DI[-3] * 100
        self.band_diff = (self.BBANDS.upperband - self.BBANDS.lowerband) / candle['close'] * 100

        # last candle
        self.candle = candle
        return

    def update(self, t):
        self.update_indicators(t)
        if(self._long_condition()):
            self.logger.info("Long condition met")
            self._enter(t, 'CALL')
        elif(self._short_condition()):
            self.logger.info("Short condition met")
            self._enter(t, 'PUT')
        return True

    def _long_condition(self):
        return (
            (self.candle['high'] > self.BBANDS.upperband) &
            (self.rsi_rc > 5) &
            (self.plus_di_rc > 5) &
            (self.band_diff > 0.4)
        )

    def _short_condition(self):
        return (
            (self.candle['low'] < self.BBANDS.lowerband) &
            (self.rsi_rc <= -5) &
            (self.minus_di_rc > 5) &
            (self.band_diff > 0.4)
        )

    def _enter(self, t, ot):
        # idx of option having strike closest to underlying
        trade_ins_idx = (self.all_options[ot]['strike_price'] - self.candle['close']).abs().idxmin()
        trade_ins = self.all_options[ot].loc[trade_ins_idx]
        # entire object trade_ins? No just the name
        # order_id = self.exchange.place_order(self.instrument, self.candle['close'], side, qty*self.qty)



        order_id = self.exchange.place_order(trade_ins, self.candle['close'], TRANSACTION.BUY, self.lot)
        if(order_id is None):
            self.logger.error(f"Error in placing order in {trade_ins.repr}")
            raise Exception("OrderPlacementException")
        # order = self.trader.fetch_order(self, order_id)
        
        # place stoploss order
        # if the premium falls below a threshhold
        # call option we predict markets to go up, premium of call rises. if it falls instead then we are wrong
        # put option we predict markets to go down, put premium to rise. if it falls instead then we were wrong.

        t = utils.round_time(t)
        # earlier it was self.instrument, here it is trade_ins
        # premium_candles = self.trader.fetch_candle(trade_ins, t-utils.get_timedelta('3m'), t)
        # if(premium_candles is None or len(premium_candles) == 0):
        #     self.logger.error("No candle for option, can't compute SL")
        #     raise Exception("OptionDataError")
        
        order_price = self.candle['close']
        # sl_price = min(premium_candles['low'])
        if (ot == 'CALL'):

        sl_order_id = self.trader.place_order(self, trade_ins, ORDER.TYPE.SL_LIMIT, TRANSACTION.SELL, self.lot, limit_price=sl_price, trigger_price=sl_price)
        
        # place the order if the trigger price is hit at the trigger price
        # if markets are falling adn trigger price is hit then the order is placed but 
        # by the time the order is to be filled market drop further so selling at trigger price might not get filled. use limit price = ORDER.TYPE.SL_MARKET instead
        
        
        
        if(sl_order_id is None):
            self.error(f"Unable to place SL order in {trade_ins.repr}")
            raise Exception("OrderPlacementException")
        # place tgt order
        # target order log in profits
        #  no need to set trigger. it is a sell order wont get executed unless price rises to limit price
        sl_points = order['traded_price'] - sl_price
        tgt_price = round(order['traded_price'] + sl_points, 2)
        tgt_order_id = self.trader.place_order(self, trade_ins, ORDER.TYPE.LIMIT, TRANSACTION.SELL, self.lot, limit_price=tgt_price)
        if(tgt_order_id is None):
            self.error(f"Unable to place Target order in {trade_ins.repr}")
            raise Exception("OrderPlacementException")
        
        # why not append the order id as well? why only the sl adn tgt
        self.open_positions.append({'trade_ins':trade_ins, 'sl_order_id':sl_order_id, 'tgt_order_id':tgt_order_id})
        self.logger.info(f"Entered {trade_ins.opt_type} at {order['traded_price']} SL: {sl_price} Target: {tgt_price}")
        return

    def on_order_update(self, t, order):
        if(order['status'] == 'COMPLETE'):
            # reference self.open_positions updated when pos changed
            # stop loss or target has not been hit
            for pos in self.open_positions:
                if(order['id'] == pos['sl_order_id']):
                    self.logger.info(f"SL hit in {pos['trade_ins'].repr}")
                    pos['sl_order_id'] = None
                    self.trader.cancel_order(self, pos['tgt_order_id'])
                    pos['tgt_order_id'] = None
                    return True
                elif(order['id'] == pos['tgt_order_id']):
                    self.logger.info(f"Target hit in {pos['trade_ins'].repr}")
                    pos['tgt_order_id'] = None
                    self.trader.cancel_order(self, pos['sl_order_id'])
                    pos['sl_order_id'] = None
                    return True
        
    def squareoff(self, t):
        self.logger.debug("squaring off...")
        # self.open_positions keeps track of all call/put buys
        for pos in self.open_positions:
            if(pos['sl_order_id'] is not None):
                self.trader.cancel_order(self, pos['sl_order_id'])
                self.trader.cancel_order(self, pos['tgt_order_id'])
                order_id = self.trader.place_order(self, pos['trade_ins'], ORDER.TYPE.MARKET, TRANSACTION.SELL, self.lot)
                if(order_id is None):
                    self.logger.error(f"Unable to squareoff position in {pos['trade_ins'].repr}")
                    raise Exception("OrderPlacementException")
        self.state = self.STATE_SQUAREDOFF
        return True
    
    def schedule(self, t):
        from apscheduler.triggers.date import DateTrigger
        # strptime: Parses a string like '09:15' into a datetime.datetime
        start_dt = dt.datetime.combine(t,dt.datetime.strptime(self.params['start_time'],'%H:%M').time())
        end_dt = dt.datetime.combine(t,dt.datetime.strptime(self.params['squareoff_time'],'%H:%M').time())
        
        self.trader.schedule(self.name, DateTrigger(start_dt-utils.get_timedelta('1m')), 'setup')
        self.trader.schedule(self.name, DateTrigger(end_dt), 'squareoff')
        
        from AlgoTrading.schedules import get_schedule
        # update every minutue
        # (start_dt).strftime('%H:%M') Converts the time portion of start_dt back into a string like '09:15' ignores the date
        update_sched = get_schedule((start_dt).strftime('%H:%M'),(end_dt-utils.get_timedelta('10s')).strftime('%H:%M'), '1m')
        self.trader.schedule(self.name, update_sched, 'update')
        
        self.trader.subscribe(self.name, 'order_update', 'on_order_update')