from config import START_DATE, END_DATE, DATA_BUILDING_DATE, UNIVERSE, DATA_LOC, NUM_TOKENS, BASE_LOG_PATH #"quantx/logs"

from Exchange.logger import setup_general_logger
from Exchange.executor import Exchange
from data_store.data_feed import DataStore
from strategy.lakshya import CWA2SSigma
from strategy.base_strategy import Strategy
from concurrent.futures import ThreadPoolExecutor
import threading
import queue

from multiprocessing import Process, Queue
from multiprocessing import Lock

import multiprocessing
import time
import os
import os.path
import pandas as pd



class Infinity:
    def __init__(self, locks, start_date, end_date, data_building_date, universe,build_data=True, all_data=None):
        """
          Driver class for Strategy and Exchange
          This class fetches data at go and keeps sending minutely candles to both exchange and strategy
          All these variables can be imported from config as well
        """
        setup_general_logger(locks[0], start_date)
        self.start_date = start_date
        self.end_date = end_date
        self.data_building_date = data_building_date

        self.universe = universe
        # reads all data for all dates btw start and end dates as mkt_data
        # next function reads next row. (packet)
        start = time.time()
        self.data_obj = DataStore(
            universe=self.universe,
            start_date=self.start_date,
            end_date=self.end_date,
            data_building_date=self.data_building_date,
            data_path = DATA_LOC, 
            build_data=build_data, 
            all_data=all_data
        )
        end = time.time()
        # print(f"datastore: {self.universe}: {end-start}")

        self.exchange = Exchange(locks, fill_type="ON_OPEN", log_name=self.start_date)
        # self.strategy = CWA2SSigma(self.universe, self.exchange, "lakshya", self.start_date,
        #                       self.end_date, self.data_building_date)
        self.strategy = CWA2SSigma(locks, self.universe, self.exchange, "lakshya", self.start_date,
                              self.end_date, self.data_building_date)
        # so that upon order filling strategy is notified
        # stratgy updates self.position
        self.exchange.order_update_subscribers.append(self.strategy)
    # dates = DataFeed.get_all_possible_dates(2024)


    def run(self):

        start_time = time.time()
        
        cnt_packets = 0
        while  self.data_obj.counter < self.data_obj.max_length:

            packet = self.data_obj.next()
            cnt_packets+=1

            if(packet.close==-0.01):
                continue
            # print(packet.timestamp_seconds)
            
            # print("packet: ##################################################### ", cnt_packets+1)
            # print(packet.open, packet.close)
            # print(packet)
            # if (cnt_packets > 10):
            #     break
            
            self.exchange.on_data(packet)
            self.strategy.on_data(packet)

        # print(f"Straegy Finished : {self.universe} - {time.time() - start_time}, cnt_packets = {cnt_packets}")


def run_sim(locks, start_date, end_date, data_building_date, universe, build_data, all_data):
    # print(f"START_DATE:{stardate}, END_DATE:{end_date}, DATA_BUILDING_DATE:{data_building_date}, UNIVERSE:{universe}")
    runner_class = Infinity(locks, start_date, end_date, data_building_date, universe, build_data, all_data)
    runner_class.run()
    del runner_class


def delete_logs():
    current_log_path = f"{BASE_LOG_PATH}/{START_DATE}"
    os.system(f"rm -rf {current_log_path}")  # delete existing log
    os.mkdir(current_log_path)
# if __name__ == "__main__":

#         start = time.time()
#         all_data = DataStore(
#             universe=UNIVERSE,
#             start_date=START_DATE,
#             end_date=END_DATE,
#             data_building_date=DATA_BUILDING_DATE,
#             data_path = DATA_LOC
#         )
#         end = time.time()
#         print("alldata datastore time", end-start)

#         for u in UNIVERSE:
#             run_sim(START_DATE, END_DATE, DATA_BUILDING_DATE, [u], False, all_data.mkt_data)
#         end = time.time()
#         print("main:", end-start)


def get_universe(num_tokens):
    universe = []
    folder_path = f"{DATA_LOC}/{START_DATE}"
    if os.path.exists(folder_path):
        file_path = f"{folder_path}/nsemd_NSECM_1_{START_DATE}.csv"
        if os.path.isfile(file_path):
            data = pd.read_csv(f"{file_path}", engine="pyarrow")
            universe = data['token'].value_counts().head(num_tokens).index.astype(str).tolist()
    return universe


if __name__ == "__main__":

        start = time.time()
        if (NUM_TOKENS):
            UNIVERSE = get_universe(NUM_TOKENS)
        end = time.time()
        # print("get universe time", end-start)

        all_data = DataStore(
            universe=UNIVERSE,
            start_date=START_DATE,
            end_date=END_DATE,
            data_building_date=DATA_BUILDING_DATE,
            data_path = DATA_LOC
        )
        end = time.time()
        # print("alldata datastore and universe time", end-start)
        processes = []
        locks = []
        # lock[0]-- general logger stdout

        # lock[1]: order.csv Exchange
        # lock[2]: order_final.csv Exchange
        # lock[3]: lakshya.csv Strategy
        # lock[4]: lakshya_predictors.csv staretegy
        for i in range(5):
            locks.append(Lock())
        delete_logs()
        for u in UNIVERSE:
            p = Process(target=run_sim, args = (locks, START_DATE, END_DATE, DATA_BUILDING_DATE, [u], False, all_data.mkt_data))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()
            
        end = time.time()
        print(f"main: {end-start} NUM_TOKENS:{NUM_TOKENS}")


# from multiprocessing import Process

# if __name__ == "__main__":
#     start = time.time()
#     processes = []
#     for u in UNIVERSE:
#         p = Process(target=run_sim, args=(START_DATE, END_DATE, DATA_BUILDING_DATE, [u]))
#         p.start()
#         processes.append(p)

#     for p in processes:
#         p.join()
#     end = time.time()
#     print("main:", end-start)





# packet
# timestamp            1423214103259841034
# token                                757
# open                               -0.01
# high                               -0.01
# low                                -0.01
# close                              -0.01
# volume                                 0
# VWAP                                  -1
# LTP                              5369.95
# midprice                          5370.0
# l1_bid_vol                           125
# l1_ask_vol                            53
# timestamp_seconds    2025-02-06 09:15:03
# inst                                 757




   # def run(self):
    #     # Token IDs
    #     start_time = time.time()

    #     tokens = [int(x) for x in self.universe]

    #     # Create queues and threads for each token
    #     queues = {}
    #     threads = {}

    #     for token in tokens:
    #         q = queue.Queue()
    #         t = threading.Thread(target=self.worker, args=(q, token), daemon=True)
    #         t.start()
    #         queues[token] = q
    #         threads[token] = t

    #     available_packets = True
    #     cnt_packets = 0

    #     while available_packets:
    #         if self.data_obj.max_length <= self.data_obj.counter:
    #             available_packets = False
    #         else:
    #             packet = self.data_obj.next()
    #             cnt_packets += 1
    #             token = packet.get('token')

    #             if token in queues:
    #                 queues[token].put(packet)
    #             else:
    #                 print(f"Unknown token {token}, skipping...")

    #     # Wait for all queues to finish
    #     for q in queues.values():
    #         q.join()

    #     print(f"Processed {cnt_packets} packets.")
    #     print(f"Strategy Finished : Time Taken - {time.time() - start_time}")


    # # Worker function per thread
    # def worker(self, q):
    #     while True:
    #         packet = q.get()
    #         try:
    #             self.exchange.on_data(packet)
    #             self.strategy.on_data(packet)
    #         finally:
    #             q.task_done()