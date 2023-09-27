from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

flight_operator = Operator('flight', n_partitions=4)


class NotEnoughSpace(Exception):
    pass


@flight_operator.register
async def create(ctx: StatefulFunction, cap: int):
    flight_data: dict = {
        'Cap': cap,
        'Customers': []
    }
    await ctx.put(flight_data)
    return ctx.key


@flight_operator.register
async def reserve(ctx: StatefulFunction):
    flight_data = await ctx.get()
    # flight_data['Cap'] -= - 1
    if flight_data['Cap'] < 0:
        raise NotEnoughSpace(f'Not enough space: for flight: {ctx.key}')
    await ctx.put(flight_data)
