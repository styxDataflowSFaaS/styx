from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

item_operator = Operator('item', n_partitions=4)


class TPCCException(Exception):
    pass


@item_operator.register
async def insert(ctx: StatefulFunction, item: dict):
    await ctx.put(item)


@item_operator.register
async def get_item(ctx: StatefulFunction,
                   frontend_key,
                   index,
                   w_id,
                   d_id,
                   o_entry_d,
                   i_qty,
                   i_w_id,
                   d_next_o_id):
    item_data = await ctx.get()
    # TPC-C defines 1% of neworder gives a wrong itemid, causing rollback.
    # Note that this will happen with 1% of transactions on purpose.
    # In Styx this means that we have a None.
    if item_data is None:
        # TPC-C 2.4.3.4 (page 31) says "tem number is not valid" must be displayed when new order rolls back.
        raise TPCCException('Item number is not valid')

    i_brand_generic = item_data['I_DATA'].find("original") != -1
    i_name = item_data['I_NAME']
    i_price = item_data['I_PRICE']
    stock_key = f'{i_w_id}:{ctx.key}'
    ctx.call_remote_async('stock',
                          'update_stock',
                          stock_key,
                          (
                              frontend_key,
                              index,
                              w_id,
                              d_id,
                              i_w_id,
                              o_entry_d,
                              i_qty,
                              d_next_o_id,
                              i_name,
                              i_price,
                              i_brand_generic)
                          )
