from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

new_order_txn_operator = Operator('new_order_txn', n_partitions=4)
# data contained are metadata needed for the response


def send_output(front_end_metadata):
    if (front_end_metadata['all_items_received'] and
            front_end_metadata['warehouse_data'] is not None and
            front_end_metadata['district_data'] is not None and
            front_end_metadata['customer_data'] is not None):
        return True
    return False


def pack_response(front_end_metadata):
    # Adjust the total for the discount
    w_tax = front_end_metadata['warehouse_data']['W_TAX']
    d_tax = front_end_metadata['district_data']['D_TAX']
    customer_data = front_end_metadata['customer_data']
    total = front_end_metadata['total'] * (1 - customer_data['C_DISCOUNT']) * (1 + w_tax + d_tax)
    # Pack up values the client is missing (see TPC-C 2.4.3.5)
    d_next_o_id = front_end_metadata['district_data']['D_NEXT_O_ID'] + 1
    misc = w_tax, d_tax, d_next_o_id, total
    return (customer_data, ) + misc + (front_end_metadata['item_replies'], )


@new_order_txn_operator.register
async def get_item_with_stock(ctx: StatefulFunction,
                              i_name,
                              i_price,
                              ol_amount,
                              s_quantity,
                              brand_generic):
    # --------------------
    # Get response from the item entity
    # --------------------
    front_end_metadata = await ctx.get()
    front_end_metadata['items_stock_response_count'] += 1
    front_end_metadata['total'] += ol_amount
    if front_end_metadata['items_stock_response_count'] == front_end_metadata['n_items']:
        front_end_metadata['all_items_received'] = True

    item_reply = (i_name, s_quantity, brand_generic, i_price, ol_amount)
    front_end_metadata['item_replies'].append(item_reply)

    await ctx.put(front_end_metadata)

    if send_output(front_end_metadata):
        response = pack_response(front_end_metadata)
        return response


@new_order_txn_operator.register
async def get_warehouse(ctx: StatefulFunction, warehouse_data: dict):
    # --------------------
    # Get response from the warehouse entity
    # --------------------
    front_end_metadata = await ctx.get()
    front_end_metadata['warehouse_data'] = warehouse_data
    await ctx.put(front_end_metadata)
    if send_output(front_end_metadata):
        response = pack_response(front_end_metadata)
        return response


@new_order_txn_operator.register
async def get_district(ctx: StatefulFunction, district_data: dict):
    # --------------------
    # Get response from the district entity
    # --------------------
    front_end_metadata = await ctx.get()
    front_end_metadata['district_data'] = district_data
    await ctx.put(front_end_metadata)
    # --------------------
    # Ask for item data
    # --------------------
    w_id: int = front_end_metadata['W_ID']
    d_id: int = front_end_metadata['D_ID']
    o_entry_d: str = front_end_metadata['O_ENTRY_D']
    i_w_ids: list[int] = front_end_metadata['I_W_IDS']
    i_qtys: list[int] = front_end_metadata['I_QTYS']
    d_next_o_id = district_data['D_NEXT_O_ID']
    for i, i_key in enumerate(front_end_metadata['I_IDS']):
        ctx.call_remote_async('item',
                              'get_item',
                              i_key,
                              # needed to get back the reply
                              (
                                  ctx.key,
                                  i,
                                  w_id,
                                  d_id,
                                  o_entry_d,
                                  i_qtys[i],
                                  i_w_ids[i],
                                  d_next_o_id)
                              )


@new_order_txn_operator.register
async def get_customer(ctx: StatefulFunction, customer_data: dict):
    front_end_metadata = await ctx.get()
    front_end_metadata['customer_data'] = customer_data
    await ctx.put(front_end_metadata)
    if send_output(front_end_metadata):
        response = pack_response(front_end_metadata)
        return response


@new_order_txn_operator.register
async def new_order(ctx: StatefulFunction, params: dict):
    # Initialize transaction parameters
    w_id: int = params['W_ID']
    d_id: int = params['D_ID']
    c_id: int = params['C_ID']
    o_entry_d: str = params['O_ENTRY_D']
    i_ids: list[int] = params['I_IDS']
    i_w_ids: list[int] = params['I_W_IDS']
    i_qtys: list[int] = params['I_QTYS']

    all_local = True
    for i_w_id in i_w_ids:
        if i_w_id != w_id:
            all_local = False
            break

    # Validate transaction parameters
    assert len(i_ids) > 0
    assert len(i_ids) == len(i_w_ids)
    assert len(i_ids) == len(i_qtys)

    # Init metadata
    init_data = {
        'W_ID': w_id,
        'D_ID': d_id,
        'C_ID': c_id,
        'I_IDS': i_ids,
        'I_QTYS': i_qtys,
        'I_W_IDS': i_w_ids,
        'O_ENTRY_D': o_entry_d,
        'items_stock_response_count': 0,
        'n_items': len(i_ids),
        'total': 0,
        'item_replies': [],
        'all_items_received': False,
        'warehouse_data': None,
        'district_data': None,
        'customer_data': None
    }

    await ctx.put(init_data)

    # --------------------
    # Get Customer, Warehouse and District information
    # --------------------
    ctx.call_remote_async(
        'warehouse',
        'get_warehouse',
        w_id,
        # needed to get back the reply
        (ctx.key, )
    )

    district_key = f'{w_id}:{d_id}'
    ctx.call_remote_async(
        'district',
        'get_district',
        district_key,
        (ctx.key, w_id, d_id, c_id, o_entry_d, len(i_ids), all_local)
    )

    customer_key = f'{w_id}:{d_id}:{c_id}'
    ctx.call_remote_async(
        'customer',
        'get_customer',
        customer_key,
        # needed to get back the reply
        (ctx.key, )
    )
