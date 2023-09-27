from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

plot_operator = Operator('plot', n_partitions=40)
# key: movie_id


@plot_operator.register
async def write(ctx: StatefulFunction, plot: str):
    await ctx.put(plot)
    return ctx.key
