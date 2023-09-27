from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


district_operator = Operator('district', n_partitions=4)
# Primary Key: (D_W_ID, D_ID)


@district_operator.register
async def insert(ctx: StatefulFunction, district: dict):
    await ctx.put(district)


@district_operator.register
async def get_district(ctx: StatefulFunction, frontend_key, w_id, d_id, c_id, o_entry_id, n_items, all_local):
    district_data = await ctx.get()
    ctx.call_remote_async(
        'new_order_txn',
        'get_district',
        frontend_key,
        (district_data, )
    )
    # --------------------
    # Update Order
    # --------------------
    d_next_o_id = district_data['D_NEXT_O_ID']
    order_key = f"{w_id}:{d_id}:{d_next_o_id}"
    order_params = {
        'O_C_ID': c_id,
        'O_ENTRY_D': o_entry_id,
        'O_CARRIER_ID': 0o0,
        'O_OL_CNT': n_items,
        'O_ALL_LOCAL': all_local
    }
    ctx.call_remote_async('order',
                          'insert',
                          order_key,
                          (order_params, ))
    # --------------------
    # Update New Order
    # --------------------
    new_order_key = f'{d_next_o_id}:{w_id}:{d_id}'
    new_order_data = {
        'NO_O_ID': d_next_o_id + 1,
        'NO_D_ID': d_id,
        'NO_W_ID': w_id,
    }
    ctx.call_remote_async(
        'new_order',
        'insert',
        new_order_key,
        (new_order_data, )
    )


@district_operator.register
async def pay(ctx: StatefulFunction, frontend_key, h_amount):
    district_data = await ctx.get()
    district_data['D_YTD'] = float(district_data['D_YTD']) + h_amount
    await ctx.put(district_data)
    ctx.call_remote_async(
        'payment_txn',
        'get_district',
        frontend_key,
        (district_data, )
    )
