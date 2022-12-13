import os
import uuid

from sanic import Sanic
from sanic.response import json

from styx.common.operator import Operator
from styx.common.stateflow_graph import StateflowGraph
from styx.common.stateflow_ingress import IngressTypes

from demo.functions import order, stock, user
from styx.styx import Styx
from styx.common.local_state_backends import LocalStateBackend

app = Sanic(__name__)

STYX_HOST: str = os.environ['STYX_HOST']
STYX_PORT: int = int(os.environ['STYX_PORT'])


@app.after_server_start
async def init_styx_client(running_app):
    # styx = styx(styx_HOST, styx_PORT, IngressTypes.TCP,
    #                           tcp_ingress_host='ingress-load-balancer', tcp_ingress_port=4000)
    running_app.ctx.styx = Styx(STYX_HOST, STYX_PORT, IngressTypes.KAFKA,
                                kafka_url='kafka1:9092')

    running_app.ctx.user_operator = Operator('user', n_partitions=6)
    running_app.ctx.stock_operator = Operator('stock', n_partitions=6)
    running_app.ctx.order_operator = Operator('order', n_partitions=6)


@app.post('/submit_dataflow_graph')
async def submit_dataflow_graph(_):
    ####################################################################################################################
    # DECLARE A STATEFLOW GRAPH ########################################################################################
    ####################################################################################################################
    g = StateflowGraph('shopping-cart', operator_state_backend=LocalStateBackend.REDIS)
    ####################################################################################################################
    app.ctx.user_operator.register_stateful_functions(user.CreateUser, user.AddCredit, user.SubtractCredit)
    g.add_operator(app.ctx.user_operator)
    ####################################################################################################################
    app.ctx.stock_operator.register_stateful_functions(stock.CreateItem, stock.AddStock, stock.SubtractStock)
    g.add_operator(app.ctx.stock_operator)
    ####################################################################################################################
    app.ctx.order_operator.register_stateful_functions(order.CreateOrder, order.AddItem, order.Checkout)
    g.add_operator(app.ctx.order_operator)
    ####################################################################################################################
    g.add_connection(app.ctx.order_operator, app.ctx.user_operator, bidirectional=True)
    g.add_connection(app.ctx.order_operator, app.ctx.stock_operator, bidirectional=True)
    ####################################################################################################################
    # SUBMIT STATEFLOW GRAPH ###########################################################################################
    ####################################################################################################################
    await app.ctx.styx.submit(g, user, order, stock)
    return json('Graph submitted', status=200)


@app.post('/user/create')
async def create_user(_):
    user_key: str = str(uuid.uuid4())
    user_name: str = f'user-{user_key}'
    # await styx.send_tcp_event(operator=user_operator,
    #                                  key=user_key,
    #                                  function=user.CreateUser(),
    #                                  params=(user_key, user_name))
    await app.ctx.styx.send_kafka_event(operator=app.ctx.user_operator,
                                               key=user_key,
                                               function=user.CreateUser,
                                               params=(user_key, user_name))
    return json({'user_key': user_key})


@app.post('/user/add_credit/<user_key>/<amount>')
async def add_credit(_, user_key: str, amount: int):
    # await styx.send_tcp_event(user_operator, user_key, user.AddCredit(), (user_key, int(amount)))
    await app.ctx.styx.send_kafka_event(app.ctx.user_operator, user_key, user.AddCredit, (user_key, int(amount)))
    return json('Credit added', status=200)


@app.post('/stock/create')
async def create_item(_):
    item_key: str = str(uuid.uuid4())
    item_name: str = f'item-{item_key}'
    price: int = 1
    # await styx.send_tcp_event(stock_operator, item_key, stock.CreateItem(), (item_key, item_name, price))
    await app.ctx.styx.send_kafka_event(app.ctx.stock_operator, item_key, stock.CreateItem, (item_key, item_name, price))
    return json({'item_key': item_key})


@app.post('/stock/add_stock/<item_key>/<amount>')
async def add_stock(_, item_key: str, amount: int):
    # await styx.send_tcp_event(stock_operator, item_key, stock.AddStock(), (item_key, int(amount)))
    await app.ctx.styx.send_kafka_event(app.ctx.stock_operator, item_key, stock.AddStock, (item_key, int(amount)))
    return json({'item_key': item_key})


@app.post('/order/create_order/<user_key>')
async def create_order(_, user_key: str):
    order_key: str = str(uuid.uuid4())
    # await styx.send_tcp_event(order_operator, order_key, order.CreateOrder(), (order_key, user_key))
    await app.ctx.styx.send_kafka_event(app.ctx.order_operator, order_key, order.CreateOrder, (order_key, user_key))
    return json({'order_key': order_key})


@app.post('/order/add_item/<order_key>/<item_key>')
async def add_item(_, order_key: str, item_key: str):
    quantity, cost = 1, 1
    # await styx.send_tcp_event(order_operator, order_key, order.AddItem(), (order_key, item_key, quantity, cost))
    await app.ctx.styx.send_kafka_event(app.ctx.order_operator, order_key, order.AddItem, (order_key, item_key, quantity, cost))
    return json('Item added', status=200)


@app.post('/order/checkout/<order_key>')
async def checkout_order(_, order_key: str):
    # await styx.send_tcp_event(order_operator, order_key, order.Checkout(), (order_key,))
    await app.ctx.styx.send_kafka_event(app.ctx.order_operator, order_key, order.Checkout, (order_key,))
    return json('Checkout started', status=200)


if __name__ == '__main__':
    app.run(debug=False)
