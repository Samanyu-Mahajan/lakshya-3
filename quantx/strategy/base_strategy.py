import datetime

from Exchange.logger import setup_logger, get_current_log_path, formatter, get_general_logger
from Exchange.executor import Exchange, Order
import pdb

class StrategyModes:
    INTRADAY = 1 # this mode will liquidate position at 15:15 IST
    INTERDAY = 2 # this mode will not liquidate position at all

# general logger is written to at start and build_eod_report quantx/logs/20250205/stdout.log
# self.logger quantx/logs/20250205/20250205_lakshya.csv
class Strategy:
    """
    name: name is basically an identifier for the strategy used for initializing logger
    start_date: the date from which the strategy should start
    end_date : the end date when the strategy stops
    data_building_date :  the data start date for initializing predictors , must be less than start_date
    exchange : an exchange object to hit the orders to
    inst_live_pnl_map : dict of inst-pnl for currently open positions

    """
    def __init__(self, locks, universe: list, exchange: Exchange, name, start_date:str,
                 end_date:str, data_building_date):

        current_log_path = get_current_log_path()         # quantx/logs/20250205
        self.general_logger = get_general_logger()     # quantx/logs/20250205/stdout.log
        self.name = name
        self.counter = 0
        # '20250205'
        self.start_date = start_date#datetime.datetime.strptime(start_date, "%Y%m%d").date()
        self.end_date = end_date#datetime.datetime.strptime(end_date, "%Y%m%d").date()
        self.data_building_date = data_building_date#datetime.datetime.strptime(data_building_date, "%Y%m%d").date()
        self.universe = universe
        self.exchange = exchange
        # difference between self.logger and general logger and predictor logger
        # name: lakshya_logger_20250205
        # log_file: quantx/logs/20250205/20250205_lakshya.csv
        self.logger  = setup_logger(locks[3], f"{self.name}_logger_{start_date}",
                                    f"{current_log_path}/{self.start_date}_{self.name}.csv",
                                    formatter)
        # name: lakshya_logger_20250205_predictors
        # log_file: quantx/logs/20250205/20250205_lakshya_predictors.csv
        self.predictor_logger = setup_logger(locks[4], f"{self.name}_logger_{start_date}_predictors",
                                   f"{current_log_path}/{self.start_date}_{self.name}_predictors.csv",
                                   formatter)
        # pdb.set_trace()
        self.last_pnl_update_time = None
        self.position = dict()  # {"HDFCBANK":{"quantity":10, "avg_buy":100}}
        headers = "inst,timestamp,position,pnl"

        self.liquidation_time = datetime.time(15, 16)  # time to start liquidating intraday strats, default
        self.logger.info(headers)
        self.mode = StrategyModes.INTRADAY
        self.start_time = datetime.time(9, 15)
        self.inst_live_pnl_map = dict()
        self.inst_wise_pnl = dict()
        self.report_build = False

#         2025-03-12 15:47:39,854 INFO, 
# universe:['757']
# start_date:20250205
# end_date:20250205
        self.general_logger.info(f"\nuniverse:{self.universe}\nstart_date:{self.start_date}\n"
                            f"end_date:{self.end_date}\n")
        self.non_tradeable = set()
        self.liquidation_time = datetime.time(15, 15)
        self.report_building_time = datetime.time(15, 26)
        self.prev_date = None
        # pdb.set_trace()


     # # log_file: quantx/logs/20250205/20250205_lakshya.csv

    def log_pnl(self, packet):
        # print("in log_pnl")

        inst = packet.inst
        if inst not in self.position:
            return
        # pdb.set_trace()

        
        # what is this?
        #  if volume traded buy = volume traded sell then pnl = sellingprice-costprice
        # avg sell is summation volume* selling price
        if self.position[inst]["quantity"] == 0:
            pnl = self.position[inst]["avg_sell"] - self.position[inst]["avg_buy"]
        # for buy side
        # assuming the buy position is sold at packet.close
        elif self.position[inst]["quantity"] > 0:
            pnl = (self.position[inst]["avg_sell"] - self.position[inst]["avg_buy"] +
                   (self.position[inst]["quantity"] * float(packet.close)))
            
            # what is this?
            # above pnl is total pnl where is live pnl is for currently open positions
            self.inst_live_pnl_map[inst]["pnl"] = ((float(packet.close) - self.inst_live_pnl_map[inst]["last_fill_price"]) *
                                                   self.position[inst]["quantity"])
            
            self.inst_live_pnl_map[inst]["max_pnl"] = max(self.inst_live_pnl_map[inst]["pnl"],
                                                          self.inst_live_pnl_map[inst]["max_pnl"])
        # assuming the sell position is bought at packet.close
        elif self.position[inst]["quantity"] < 0:
            pnl = (self.position[inst]["avg_sell"] - self.position[inst]["avg_buy"] +
                   (self.position[inst]["quantity"] * float(packet.close)))
            self.inst_live_pnl_map[inst]["pnl"] = ((-float(packet.close) + self.inst_live_pnl_map[inst]["last_fill_price"])
                                                   * abs(self.position[inst]["quantity"]))
            self.inst_live_pnl_map[inst]["max_pnl"] = max(self.inst_live_pnl_map[inst]["pnl"],
                                                          self.inst_live_pnl_map[inst]["max_pnl"])
        # if pnl < -20000:
        #     self.non_tradeable.add(packet.inst)
        # throws error inst is int 757 .name_ist
        # self.inst_wise_pnl[inst.name_inst] = pnl 
        # inst wise pnl is updated here and used in eod report.
        self.inst_wise_pnl[inst] = pnl 


        # # log_file: quantx/logs/20250205/20250205_lakshya.csv
        log_line = f"{inst},{packet.timestamp},{self.position[inst]['quantity']},{pnl}"
        # print(log_line)
        self.logger.info(f"{inst},{packet.timestamp},{self.position[inst]['quantity']},{pnl}")
        # pdb.set_trace()
        # if packet.inst.name_inst == "BANDHANBNK":
        #     print(self.inst_live_pnl_map[packet.inst], packet.inst)

    def check_log_pnl(self, packet):
        self.counter += 1
        if self.last_pnl_update_time is None:
            self.last_pnl_update_time = packet.timestamp
            return
        # if ((packet.timestamp == self.last_pnl_update_time) or
        #         (packet.timestamp - self.last_pnl_update_time).total_seconds() >= 500):
        #     print("abcdf, packet_timestamp, self.last_pnl_update_time", packet.timestamp, self.last_pnl_update_time)
        #     self.log_pnl(packet)
        # log at intervals of 500 seconds
        if ((packet.timestamp-self.last_pnl_update_time)//10**9)>=500:
            # print("abcdf, packet_timestamp, self.last_pnl_update_time", packet.timestamp_seconds, packet.timestamp, self.last_pnl_update_time)
            self.log_pnl(packet)
            self.last_pnl_update_time=packet.timestamp
            


#  strategy does nothing
    def on_data(self, packet):
        # print("inside on_data")
        # print("abcdef")
        # print(packet)
        self.check_log_pnl(packet)
        # self.exchange.place_order(packet.inst, packet.close, "BUY", 1, signal="COVER")


        # if time> 3:15 cancel pending orders and liquidate instrument
        # if packet.timestamp.time() > self.liquidation_time and self.mode == StrategyModes.INTRADAY:
        packet_time = packet.timestamp_seconds.time()
        # print(packet_time)
        if packet_time > self.liquidation_time and self.mode == StrategyModes.INTRADAY:

            if packet.inst in self.position and self.position[packet.inst]["quantity"] != 0:
                self.exchange.cancel_pending_orders(packet)
                self.liquidate(packet)
            return False
        # if packet.timestamp.time() >= self.report_building_time:
        if packet_time >= self.report_building_time:

            self.build_eod_report()
            return False
        # what is nontradeable
        if packet.inst in self.non_tradeable and self.position[packet.inst]["quantity"] == 0:
            return False
        return True

    def build_data(self, packet):
        pass # to be implemented in strategy

    def raise_day_begin(self, packet):
        print(packet.timestamp)
        pass

# general logger used
    def build_eod_report(self):
        # print("eod report")
        # quesstion: why not do the same logic for both modes. intraday uses self.position and interday uses self.inst_wise_pnl
        # print(len(self.exchange.completed_order))
        if self.report_build:
            return
        if not self.report_build and self.mode == StrategyModes.INTRADAY:
            # print("in intraday")
            total_pnl = 0
            turnover = 0
            for key, value in self.position.items():
                # quantity =0 because positions are liquidated.
                inst_pnl = value['avg_sell'] - value['avg_buy']
                total_pnl += inst_pnl
                # what is turnover why is it so
                turnover += value['avg_sell'] + value['avg_buy']

                self.general_logger.info(f"{key}:{inst_pnl},{key}\n")
            self.general_logger.info(f"Total Orders:{len(self.exchange.completed_order)}\n")
            self.general_logger.info(f"Total PNL:{total_pnl}, START_DATE : {self.start_date}, "
                                     f"TOTAL_ORDERS: {len(self.exchange.completed_order)}\n*******\n")
            self.report_build = True
        elif not self.report_build and self.mode == StrategyModes.INTERDAY:
            # print("in inter day")
            total_pnl = 0
            # pdb.set_trace()
            for key, value in self.inst_wise_pnl.items():
                self.general_logger.info(f"{key}:{value}\n")
                total_pnl += value
            self.general_logger.info(f"Total Orders:{len(self.exchange.completed_order)}\n")
            self.general_logger.info(f"Total PNL:{total_pnl}")
            self.report_build = True

    # called by exchange when order filled. 
    # updates self.position 
    def on_order_update(self, order: Order):
        # print("inside on_order_update")
        # pdb.set_trace()
        if order.inst not in self.position:
            self.position[order.inst] = {"quantity": 0, "avg_sell": 0, "total_sell": 0, "total_buy": 0,
                                         "avg_buy": 0}
            self.inst_live_pnl_map[order.inst] = {"pnl": 0, "max_pnl": 0, "last_fill_price": 0}
        #quantity plus means buy - means sell totalbuy-totalsell  = quantity
        if order.side == Order.BUY and order.status == order.FILLED:
            if order.inst in self.position:
                self.position[order.inst]["quantity"] += order.quantity 
                self.position[order.inst]["avg_buy"] += order.quantity * float(order.fill_price)
                self.position[order.inst]["total_buy"] += order.quantity
        elif order.side == Order.SELL and order.status == order.FILLED:
            if order.inst in self.position:
                self.position[order.inst]["quantity"] -= order.quantity
                self.position[order.inst]["avg_sell"] += abs(order.quantity) * float(order.fill_price)
                self.position[order.inst]["total_sell"] += order.quantity
        
        # why order type != order.limit?
        # if self.position[order.inst]["quantity"] != 0 and order.FILLED and order.order_type != Order.LIMIT:
        if self.position[order.inst]["quantity"] != 0 and order.FILLED :
            self.inst_live_pnl_map[order.inst]["last_fill_price"] = float(order.fill_price)
        # why initializing?
        # becuase since position is closed there is no live pnl
        elif self.position[order.inst]["quantity"] == 0 and order.FILLED:
            self.inst_live_pnl_map[order.inst] = {"pnl": 0, "max_pnl": 0, "last_fill_price": 0}


    # close all positions
    def liquidate(self, packet):
        inst = packet.inst
        if self.position[inst]["quantity"] > 0:
            self.exchange.place_order(inst, packet.close, "SELL", abs(self.position[inst]["quantity"]),
                                      order_type=Order.LIQUIDATE)
        elif self.position[inst]["quantity"] < 0:
            self.exchange.place_order(inst, packet.close, "BUY", abs(self.position[inst]["quantity"]),
                                      order_type=Order.LIQUIDATE)
