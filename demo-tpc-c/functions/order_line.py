from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


order_line_operator = Operator('order_line', n_partitions=4)


@order_line_operator.register
async def insert(ctx: StatefulFunction, order_line: dict):
    await ctx.put(order_line)


@order_line_operator.register
async def get_order_line(ctx: StatefulFunction):
    order_line = await ctx.get()
    return order_line
