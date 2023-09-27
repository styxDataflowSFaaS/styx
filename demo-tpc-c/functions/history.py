from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


history_operator = Operator('history', n_partitions=4)


@history_operator.register
async def insert(ctx: StatefulFunction, history: dict):
    await ctx.put(history)


@history_operator.register
async def get_history(ctx: StatefulFunction):
    history = await ctx.get()
    return history
