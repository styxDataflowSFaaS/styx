import asyncio
import time
import pandas as pd

import uvloop
from styx.common.stateflow_ingress import IngressTypes
from styx.styx import Styx

from demo.functions import graph
from demo.functions.graph import user_operator, stock_operator, order_operator

N_USERS = 5000
USER_STARTING_CREDIT = 1000
N_ITEMS = 5000
ITEM_DEFAULT_PRICE = 1
ITEM_STARTING_STOCK = 1000
N_ORDERS = 5000

STYX_HOST: str = 'localhost'
STYX_PORT: int = 8886
KAFKA_URL = 'localhost:9093'


async def main():
    styx = Styx(STYX_HOST, STYX_PORT,
                ingress_type=IngressTypes.KAFKA,
                kafka_url=KAFKA_URL)
    await styx.start()
    ####################################################################################################################
    # SUBMIT STATEFLOW GRAPH ###########################################################################################
    ####################################################################################################################
    print(user_operator.functions)
    print(stock_operator.functions)
    print(order_operator.functions)
    print([op.functions for op in graph.g.nodes.values()])
    await styx.submit(graph.g)

    print('Graph submitted')

    timestamped_request_ids = {}

    time.sleep(1)

    # CREATE USERS
    tasks = []
    for i in range(N_USERS):
        tasks.append(styx.send_kafka_event(operator=user_operator,
                                           key=i,
                                           function='create_user',
                                           params=(str(i), )))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    tasks = []
    for i in range(N_USERS):
        tasks.append(styx.send_kafka_event(user_operator, i, 'add_credit', (USER_STARTING_CREDIT, )))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    print('Users done')
    # CREATE ITEMS
    tasks = []
    for i in range(N_ITEMS):
        tasks.append(styx.send_kafka_event(stock_operator, i, 'create_item', (str(i), ITEM_DEFAULT_PRICE)))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    tasks = []
    for i in range(N_ITEMS):
        tasks.append(styx.send_kafka_event(stock_operator, i, 'add_stock', (ITEM_STARTING_STOCK, )))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp
    print('Items done')
    # CREATE ORDERS
    tasks = []
    for i in range(N_ORDERS):
        tasks.append(styx.send_kafka_event(order_operator, i, 'create_order', (i, )))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    tasks = []
    for i in range(N_ORDERS):
        order_key = item_key = i
        quantity, cost = 1, 1
        tasks.append(styx.send_kafka_event(order_operator, order_key, 'add_item', (item_key,
                                                                                   quantity, cost)))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp

    tasks = []
    for i in range(N_ORDERS):
        order_key = i
        tasks.append(styx.send_kafka_event(order_operator, order_key, 'checkout'))
    responses = await asyncio.gather(*tasks)
    for request_id, timestamp in responses:
        timestamped_request_ids[request_id] = timestamp
    await styx.close()

    pd.DataFrame(timestamped_request_ids.items(), columns=['request_id', 'timestamp']).to_csv('client_requests.csv',
                                                                                              index=False)

uvloop.install()
asyncio.run(main())
