from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


order_operator = Operator('order', n_partitions=4)


@order_operator.register
async def insert(ctx: StatefulFunction, order: dict):
    await ctx.put(order)


@order_operator.register
async def get_order(ctx: StatefulFunction):
    order = await ctx.get()
    return order
