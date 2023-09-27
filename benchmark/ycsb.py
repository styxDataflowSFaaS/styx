from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

ycsb_operator = Operator('ycsb', n_partitions=4)


class NotEnoughCredit(Exception):
    pass


@ycsb_operator.register
async def insert(ctx: StatefulFunction):
    value: int = 1_000_000
    await ctx.put(value)
    return ctx.key


@ycsb_operator.register
async def read(ctx: StatefulFunction):
    value: int = await ctx.get()
    return ctx.key, value


@ycsb_operator.register
async def update(ctx: StatefulFunction):
    new_value = await ctx.get()
    new_value += 1

    await ctx.put(new_value)
    return ctx.key, new_value


@ycsb_operator.register
async def transfer(ctx: StatefulFunction, key_b: str):
    value_a = await ctx.get()

    ctx.call_remote_async(
        operator_name='ycsb',
        function_name='update',
        key=key_b
    )

    value_a -= 1

    if value_a < 0:
        raise NotEnoughCredit(f'Not enough credit for'
                              f' user: {ctx.key}')

    await ctx.put(value_a)

    return ctx.key, value_a
