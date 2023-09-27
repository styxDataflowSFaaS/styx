from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

movie_info_operator = Operator('movie_info', n_partitions=40)
# key: movie_id


@movie_info_operator.register
async def write(ctx: StatefulFunction, info: dict):
    await ctx.put(info)
    return ctx.key
