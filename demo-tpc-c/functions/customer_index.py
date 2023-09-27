import logging
import math

from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


customer_idx_operator = Operator('customer_idx', n_partitions=4)
# value: list of dict that contains C_FIRST, C_ID, C_W_ID, C_D_ID


class CustomerDoesNotExist(Exception):
    pass


@customer_idx_operator.register
async def insert(ctx: StatefulFunction, customer_idx: list):
    await ctx.put(customer_idx)


@customer_idx_operator.register
async def pay(ctx: StatefulFunction, frontend_key, h_amount, d_id, w_id):
    index_data: list[dict[str, str | int]] = await ctx.get()
    if index_data is None:
        raise CustomerDoesNotExist(f'Customer with id: {ctx.key} does not exist in the index')
    index_data = sorted(index_data, key=lambda x: x['C_FIRST'])
    index = math.ceil((len(index_data) - 1) / 2)
    customer = index_data[index]
    customer_key = f"{customer['C_W_ID']}:{customer['C_D_ID']}:{customer['C_ID']}"
    ctx.call_remote_async(
        'customer',
        'pay',
        customer_key,
        # needed to get back the reply
        (frontend_key, h_amount, d_id, w_id)
    )
