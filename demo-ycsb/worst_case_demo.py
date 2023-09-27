import asyncio
import time
import pandas as pd

import uvloop
from styx.common.stateflow_ingress import IngressTypes
from styx.styx import Styx

from graph import ycsb_operator, g
import consts


N_ROWS = 1000

STYX_HOST: str = 'localhost'
STYX_PORT: int = 8886
KAFKA_URL = 'localhost:9093'


keys: list[int] = [0]


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
    for _ in range(N_ROWS):
        tasks.append(styx.send_kafka_event(ycsb_operator, keys[0], 'update'))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    await styx.close()

    pd.DataFrame(timestamped_request_ids.items(), columns=['request_id', 'timestamp']).to_csv(
        '../demo/client_requests.csv',
        index=False)

if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
