import asyncio
import random
import time
import pandas as pd

import uvloop
from styx.common.stateflow_ingress import IngressTypes
from styx.styx import Styx

import consts
from graph import ycsb_operator, g
from zipfian_generator import ZipfGenerator


N_ROWS = 1000

STYX_HOST: str = 'localhost'
STYX_PORT: int = 8886
KAFKA_URL = 'localhost:9093'


keys: list[int] = list(range(N_ROWS))


async def main():
    styx = Styx(STYX_HOST, STYX_PORT,
                ingress_type=IngressTypes.KAFKA,
                kafka_url=KAFKA_URL)
    await styx.start()
    ####################################################################################################################
    # SUBMIT STATEFLOW GRAPH ###########################################################################################
    ####################################################################################################################
    await styx.submit(g, (consts, ))

    print('Graph submitted')

    timestamped_request_ids = {}

    time.sleep(1)

    zipf_gen = ZipfGenerator(items=N_ROWS, zipf_const=0.0)
    operations = ["r", "u", "t"]
    operation_mix = [0.0, 0.0, 1.0]
    operation_counts: dict[str, int] = {"r": 0, "u": 0, "t": 0}

    # INSERT
    tasks = []
    for i in keys:
        tasks.append(styx.send_kafka_event(operator=ycsb_operator,
                                           key=i,
                                           function='insert'))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    time.sleep(1)

    tasks = []
    for i in range(N_ROWS):
        key = keys[next(zipf_gen)]
        op = random.choices(operations, weights=operation_mix, k=1)[0]
        operation_counts[op] += 1
        if op == "r":
            tasks.append(styx.send_kafka_event(ycsb_operator, key, 'read'))
        elif op == "u":
            tasks.append(styx.send_kafka_event(ycsb_operator, key, 'update'))
        else:
            key2 = keys[next(zipf_gen)]
            while key2 == key:
                key2 = keys[next(zipf_gen)]
            tasks.append(styx.send_kafka_event(ycsb_operator, key, 'transfer', (key2, )))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    print(operation_counts)

    await styx.close()

    pd.DataFrame(timestamped_request_ids.items(), columns=['request_id', 'timestamp']).to_csv(
        '../demo/client_requests.csv',
        index=False)

if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
