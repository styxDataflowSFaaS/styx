from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction
import consts

ycsb_operator = Operator('ycsb', n_partitions=10)


class NotEnoughCredit(Exception):
    pass


@ycsb_operator.register
async def insert(ctx: StatefulFunction):
    value: int = consts.initial_account_balance
    await ctx.put(value)
    return ctx.key,


@ycsb_operator.register
async def read(ctx: StatefulFunction):
    value: int = await ctx.get()
    return ctx.key, value


@ycsb_operator.register
async def update(ctx: StatefulFunction):
    new_value = await ctx.get()
    new_value += consts.transfer_amount

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

    value_a -= consts.transfer_amount

    if value_a < 0:
        raise NotEnoughCredit(f'Not enough credit for user: {ctx.key}')

    await ctx.put(value_a)

    return ctx.key, value_a
