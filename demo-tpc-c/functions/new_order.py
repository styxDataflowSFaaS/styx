from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


new_order_operator = Operator('new_order', n_partitions=4)


@new_order_operator.register
async def insert(ctx: StatefulFunction, new_order_data: dict):
    await ctx.put(new_order_data)
