import random
import sys
from timeit import default_timer as timer
import asyncio
import time
from multiprocessing import Pool

import pandas as pd
from kafka.producer.future import FutureRecordMetadata

from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_graph import StateflowGraph
from styx.common.stateflow_ingress import IngressTypes
from styx.styx import Styx

from graph import (geo_operator, rate_operator, recommendation_operator, search_operator, order_operator,
                   user_operator, hotel_operator, flight_operator)

threads = int(sys.argv[1])
messages_per_second = int(sys.argv[2])
sleeps_per_second = 100
sleep_time = 0.0085
seconds = 60
STYX_HOST: str = 'localhost'
STYX_PORT: int = 8886
KAFKA_URL = 'localhost:9093'


def populate(styx_client: Styx):
    # populate the geo table
    timestamp_futures: dict[int, FutureRecordMetadata] = {}
    request_id, metadata = styx_client.send_kafka_event_no_wait(operator=geo_operator,
                                                                key=1,
                                                                function='create',
                                                                params=(37.7867, 0))
    timestamp_futures[request_id] = metadata
    request_id, metadata = styx_client.send_kafka_event_no_wait(operator=geo_operator,
                                                                key=2,
                                                                function='create',
                                                                params=(37.7854, -122.4005))
    timestamp_futures[request_id] = metadata
    request_id, metadata = styx_client.send_kafka_event_no_wait(operator=geo_operator,
                                                                key=3,
                                                                function='create',
                                                                params=(37.7867, -122.4071))
    timestamp_futures[request_id] = metadata
    request_id, metadata = styx_client.send_kafka_event_no_wait(operator=geo_operator,
                                                                key=4,
                                                                function='create',
                                                                params=(37.7936, -122.3930))
    timestamp_futures[request_id] = metadata
    request_id, metadata = styx_client.send_kafka_event_no_wait(operator=geo_operator,
                                                                key=5,
                                                                function='create',
                                                                params=(37.7831, -122.4181))
    timestamp_futures[request_id] = metadata
    request_id, metadata = styx_client.send_kafka_event_no_wait(operator=geo_operator,
                                                                key=6,
                                                                function='create',
                                                                params=(37.7863, -122.4015))
    timestamp_futures[request_id] = metadata
    for i in range(7, 81):
        lat: float = 37.7835 + i / 500.0 * 3
        lon: float = -122.41 + i / 500.0 * 4
        request_id, metadata = styx_client.send_kafka_event_no_wait(operator=geo_operator,
                                                                    key=i,
                                                                    function='create',
                                                                    params=(lat, lon))
        timestamp_futures[request_id] = metadata
    # populate rate
    request_id, metadata = styx_client.send_kafka_event_no_wait(operator=rate_operator,
                                                                key=1,
                                                                function='create',
                                                                params=("RACK",
                                                                        "2015-04-09",
                                                                        "2015-04-10", {"BookableRate": 190.0,
                                                                                       "Code": "KNG",
                                                                                       "RoomDescription": "King sized bed",
                                                                                       "TotalRate": 109.0,
                                                                                       "TotalRateInclusive": 123.17}))
    timestamp_futures[request_id] = metadata
    request_id, metadata = styx_client.send_kafka_event_no_wait(operator=rate_operator,
                                                                key=2,
                                                                function='create',
                                                                params=("RACK",
                                                                        "2015-04-09",
                                                                        "2015-04-10", {"BookableRate": 139.0,
                                                                                       "Code": "QN",
                                                                                       "RoomDescription": "Queen sized bed",
                                                                                       "TotalRate": 139.0,
                                                                                       "TotalRateInclusive": 153.09}))
    timestamp_futures[request_id] = metadata
    request_id, metadata = styx_client.send_kafka_event_no_wait(operator=rate_operator,
                                                                key=3,
                                                                function='create',
                                                                params=("RACK",
                                                                        "2015-04-09",
                                                                        "2015-04-10", {"BookableRate": 109.0,
                                                                                       "Code": "KNG",
                                                                                       "RoomDescription": "King sized bed",
                                                                                       "TotalRate": 109.0,
                                                                                       "TotalRateInclusive": 123.17}))
    timestamp_futures[request_id] = metadata
    for i in range(4, 80):
        if i % 3 == 0:
            hotel_id = i
            end_date = "2015-04-"
            rate = 109.0
            rate_inc = 123.17
            if i % 2 == 0:
                end_date += '17'
            else:
                end_date += '24'
            if i % 5 == 1:
                rate = 120.0
                rate_inc = 140.0
            elif i % 5 == 2:
                rate = 124.0
                rate_inc = 144.0
            elif i % 5 == 3:
                rate = 132.0
                rate_inc = 158.0
            elif i % 5 == 4:
                rate = 232.0
                rate_inc = 258.0
            request_id, metadata = styx_client.send_kafka_event_no_wait(operator=rate_operator,
                                                                        key=hotel_id,
                                                                        function='create',
                                                                        params=("RACK",
                                                                                "2015-04-09",
                                                                                end_date, {"BookableRate": rate,
                                                                                           "Code": "KNG",
                                                                                           "RoomDescription": "King sized bed",
                                                                                           "TotalRate": rate,
                                                                                           "TotalRateInclusive": rate_inc}))
            timestamp_futures[request_id] = metadata
    # populate recommendation
    request_id, metadata = styx_client.send_kafka_event_no_wait(operator=recommendation_operator,
                                                                key=1,
                                                                function='create',
                                                                params=(37.7867, -122.4112, 109.00, 150.00))
    timestamp_futures[request_id] = metadata
    request_id, metadata = styx_client.send_kafka_event_no_wait(operator=recommendation_operator,
                                                                key=2,
                                                                function='create',
                                                                params=(37.7854, -122.4005, 139.00, 120.00))
    timestamp_futures[request_id] = metadata
    request_id, metadata = styx_client.send_kafka_event_no_wait(operator=recommendation_operator,
                                                                key=3,
                                                                function='create',
                                                                params=(37.7834, -122.4071, 109.00, 190.00))
    timestamp_futures[request_id] = metadata
    request_id, metadata = styx_client.send_kafka_event_no_wait(operator=recommendation_operator,
                                                                key=4,
                                                                function='create',
                                                                params=(37.7936, -122.3930, 129.00, 160.00))
    timestamp_futures[request_id] = metadata
    request_id, metadata = styx_client.send_kafka_event_no_wait(operator=recommendation_operator,
                                                                key=5,
                                                                function='create',
                                                                params=(37.7831, -122.4181, 119.00, 140.00))
    timestamp_futures[request_id] = metadata
    request_id, metadata = styx_client.send_kafka_event_no_wait(operator=recommendation_operator,
                                                                key=6,
                                                                function='create',
                                                                params=(37.7863, -122.4015, 149.00, 200.00))
    timestamp_futures[request_id] = metadata
    for i in range(7, 80):
        hotel_id = i
        lat = 37.7835 + i / 500.0 * 3
        lon = -122.41 + i / 500.0 * 4
        rate = 135.00
        rate_inc = 179.00
        if i % 3 == 0:
            if i % 5 == 0:
                rate = 109.00
                rate_inc = 123.17
            elif i % 5 == 1:
                rate = 120.00
                rate_inc = 140.00
            elif i % 5 == 2:
                rate = 124.00
                rate_inc = 144.00
            elif i % 5 == 3:
                rate = 132.00
                rate_inc = 158.00
            elif i % 5 == 4:
                rate = 232.00
                rate_inc = 258.00
        request_id, metadata = styx_client.send_kafka_event_no_wait(operator=recommendation_operator,
                                                                    key=hotel_id,
                                                                    function='create',
                                                                    params=(lat, lon, rate, rate_inc))
        timestamp_futures[request_id] = metadata
    # populate user
    for i in range(501):
        username = f"Cornell_{i}"
        password = str(i) * 10
        request_id, metadata = styx_client.send_kafka_event_no_wait(operator=user_operator,
                                                                    key=username,
                                                                    function='create',
                                                                    params=(password,))
        timestamp_futures[request_id] = metadata
    # populate hotels
    for i in range(100):
        request_id, metadata = styx_client.send_kafka_event_no_wait(operator=hotel_operator,
                                                                    key=i,
                                                                    function='create',
                                                                    params=(10,))
        timestamp_futures[request_id] = metadata
    # populate flights
    for i in range(100):
        request_id, metadata = styx_client.send_kafka_event_no_wait(operator=flight_operator,
                                                                    key=i,
                                                                    function='create',
                                                                    params=(10,))
        timestamp_futures[request_id] = metadata

    timestamp_futures = {k: v.get().timestamp for k, v in timestamp_futures.items()}
    return timestamp_futures


async def submit_graph(styx):
    g = StateflowGraph('deathstar_hotel_reservations', operator_state_backend=LocalStateBackend.DICT)
    ####################################################################################################################
    g.add_operators(flight_operator,
                    geo_operator,
                    hotel_operator,
                    order_operator,
                    rate_operator,
                    recommendation_operator,
                    search_operator,
                    user_operator)
    print(list(g.nodes.values())[0].n_partitions)
    await styx.submit(g)
    print("Graph submitted")


async def deathstar_init(styx):
    await submit_graph(styx)
    time.sleep(2)
    populate(styx)
    print('Data populated')
    time.sleep(2)


def search_hotel(c):
    in_date = random.randint(9, 23)
    out_date = random.randint(in_date + 1, 24)
    if in_date < 10:
        in_date_str = f"2015-04-0{in_date}"
    else:
        in_date_str = f"2015-04-{in_date}"
    if out_date < 10:
        out_date_str = f"2015-04-0{in_date}"
    else:
        out_date_str = f"2015-04-{in_date}"
    lat = 38.0235 + (random.randint(0, 481) - 240.5) / 1000.0
    lon = -122.095 + (random.randint(0, 325) - 157.0) / 1000.0
    return search_operator, c, "nearby", (lat, lon, in_date_str, out_date_str)


def recommend(c):
    coin = random.random()
    if coin < 0.33:
        req_param = "dis"
    elif coin < 0.66:
        req_param = "rate"
    else:
        req_param = "price"
    lat = 38.0235 + (random.randint(0, 481) - 240.5) / 1000.0
    lon = -122.095 + (random.randint(0, 325) - 157.0) / 1000.0
    return recommendation_operator, c, "get_recommendations", (req_param, lat, lon)


def user_login():
    user_id = str(random.randint(0, 500))
    username = f"Cornell_{user_id}"
    password = user_id * 10
    return user_operator, username, "check_user", (password,)


def reserve_all(c):
    hotel_id = random.randint(0, 99)
    flight_id = random.randint(0, 99)
    user_id = "user1"
    return order_operator, c, "create", (hotel_id, flight_id, user_id)


def deathstar_workload_generator():
    search_ratio = 0.6
    recommend_ratio = 0.39
    user_ratio = 0.005
    reserve_ratio = 0.005
    c = 0
    while True:
        coin = random.random()
        if coin < search_ratio:
            yield search_hotel(c)
        elif coin < search_ratio + recommend_ratio:
            yield recommend(c)
        elif coin < search_ratio + recommend_ratio + user_ratio:
            yield user_login()
        else:
            yield reserve_all(c)
        c += 1


def benchmark_runner(proc_num) -> dict[int, dict]:
    print(f'Generator: {proc_num} starting')
    styx = Styx(STYX_HOST, STYX_PORT, IngressTypes.KAFKA, kafka_url=KAFKA_URL)
    deathstar_generator = deathstar_workload_generator()
    timestamp_futures: dict[int, dict] = {}
    start = timer()
    for _ in range(seconds):
        sec_start = timer()
        for i in range(messages_per_second):
            if i % (messages_per_second // sleeps_per_second) == 0:
                time.sleep(sleep_time)
            operator, key, func_name, params = next(deathstar_generator)
            request_id, metadata = styx.send_kafka_event_no_wait(operator=operator,
                                                                 key=key,
                                                                 function=func_name,
                                                                 params=params)
            timestamp_futures[request_id] = {"metadata": metadata, "op": f'{func_name} {key}->{params}'}
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f'Latency per second: {sec_end2 - sec_start}')
    end = timer()
    print(f'Average latency per second: {(end - start) / seconds}')
    time.sleep(2)
    timestamp_futures = {k: {"timestamp": v["metadata"].get().timestamp, "op": v["op"]}
                         for k, v in timestamp_futures.items()}
    return timestamp_futures


def main():
    styx_client = Styx(STYX_HOST, STYX_PORT, IngressTypes.KAFKA, kafka_url=KAFKA_URL)

    asyncio.run(deathstar_init(styx_client))

    with Pool(threads) as p:
        results = p.map(benchmark_runner, range(threads))

    results = {k: v for d in results for k, v in d.items()}
    pd.DataFrame({"request_id": list(results.keys()),
                  "timestamp": [res["timestamp"] for res in results.values()],
                  "op": [res["op"] for res in results.values()]
                  }).to_csv('client_requests.csv',
                            index=False)

    styx_client.sync_kafka_producer.flush()
    styx_client.sync_kafka_producer.close()


if __name__ == "__main__":
    main()
