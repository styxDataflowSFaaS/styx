import asyncio
import logging
import random
import time
from timeit import default_timer as timer

import uvloop
import aiohttp


logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
                    datefmt='%I:%M:%S')
logger = logging.getLogger(__name__)

NUMBER_0F_ITEMS = 10000
NUMBER_OF_USERS = 10000
NUMBER_OF_ORDERS = 10000

ORDER_URL = STOCK_URL = PAYMENT_URL = 'http://localhost:5000'


async def post_and_get_status(session, url):
    async with session.post(url) as resp:
        return resp.status


async def create_item(session, url):
    async with session.post(url) as resp:
        jsn = await resp.json()
        return jsn['item_key']


async def create_items(session, number_of_items: int, stock: int = 1000000000, price: int = 1):
    tasks = []
    # Create users
    for _ in range(number_of_items):
        create_item_url = f"{STOCK_URL}/stock/create"
        tasks.append(asyncio.ensure_future(create_item(session, create_item_url)))
    item_ids = await asyncio.gather(*tasks)
    tasks = []
    time.sleep(2)
    # Add funds
    for item_id in item_ids:
        create_item_url = f"{STOCK_URL}/stock/add_stock/{item_id}/{stock}"
        tasks.append(asyncio.ensure_future(post_and_get_status(session, create_item_url)))
    await asyncio.gather(*tasks)
    return item_ids


async def create_user(session, url):
    async with session.post(url) as resp:
        jsn = await resp.json()
        return jsn['user_key']


async def create_users(session, number_of_users: int, credit: int = 10000000000):
    tasks = []
    # Create users
    for _ in range(number_of_users):
        create_user_url = f"{PAYMENT_URL}/user/create"
        tasks.append(asyncio.ensure_future(create_user(session, create_user_url)))
    user_ids = await asyncio.gather(*tasks)
    time.sleep(2)
    tasks = []
    # Add funds
    for user_id in user_ids:
        add_funds_url = f"{PAYMENT_URL}/user/add_credit/{user_id}/{credit}"
        tasks.append(asyncio.ensure_future(post_and_get_status(session, add_funds_url)))
    await asyncio.gather(*tasks)
    return user_ids


async def create_order(session, url):
    async with session.post(url) as resp:
        jsn = await resp.json()
        return jsn['order_key']


async def create_orders(session, item_ids, user_ids, number_of_orders):
    tasks = []
    # Create orders
    for _ in range(number_of_orders):
        user_id = random.choice(user_ids)
        create_order_url = f"{ORDER_URL}/order/create_order/{user_id}"
        tasks.append(asyncio.ensure_future(create_order(session, create_order_url)))
    order_ids = await asyncio.gather(*tasks)
    time.sleep(2)
    tasks = []
    # Add items
    for order_id in order_ids:
        item_id = random.choice(item_ids)
        create_item_url = f"{ORDER_URL}/order/add_item/{order_id}/{item_id}"
        tasks.append(asyncio.ensure_future(post_and_get_status(session, create_item_url)))
    await asyncio.gather(*tasks)
    return order_ids


async def submit_dataflow_graph(session):
    submit_dataflow_graph_url = f"{ORDER_URL}/submit_dataflow_graph"
    status = await asyncio.ensure_future(post_and_get_status(session, submit_dataflow_graph_url))
    logger.info(f"Dataflow graph submission status: {status}")


async def populate_databases():
    async with aiohttp.ClientSession() as session:
        logger.info("Submitting dataflow graph ...")
        await submit_dataflow_graph(session)
        logger.info("Dataflow graph submitted")
        time.sleep(2)
        start = timer()
        logger.info("Creating items ...")
        item_ids = await create_items(session, NUMBER_0F_ITEMS)
        logger.info("Items created")
        time.sleep(2)
        logger.info("Creating users ...")
        user_ids = await create_users(session, NUMBER_OF_USERS)
        logger.info("Users created")
        time.sleep(2)
        logger.info("Creating orders ...")
        order_ids = await create_orders(session, item_ids, user_ids, NUMBER_OF_ORDERS)
        logger.info("Orders created")
        end = timer()
        logger.info(f'Time to populate: {end - start} seconds')
        time.sleep(2)
        await perform_checkouts(session, order_ids)


async def perform_checkouts(session, order_ids):
    tasks = []
    for order_id in order_ids:
        url = f"{ORDER_URL}/order/checkout/{order_id}"
        tasks.append(asyncio.ensure_future(post_and_get_status(session, url)))
    start_checkouts = timer()
    order_responses = await asyncio.gather(*tasks)
    end_checkouts = timer()
    logger.info(f'Checkout time for {NUMBER_OF_ORDERS} orders: {end_checkouts - start_checkouts} seconds')
    logger.info(f'{int(NUMBER_OF_ORDERS // (end_checkouts - start_checkouts))} transactions per second')
    return order_responses

if __name__ == '__main__':
    uvloop.install()
    asyncio.run(populate_databases())
