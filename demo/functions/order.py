from styx.common.operator import StatefulFunction, Operator

order_operator = Operator('order', n_partitions=6)


@order_operator.register
async def create_order(ctx: StatefulFunction, user_key: str):
    await ctx.put({'user_key': user_key, 'items': []})
    return ctx.key


@order_operator.register
async def add_item(ctx: StatefulFunction, item_key, quantity, cost):
    order_data = await ctx.get()
    order_data['items'].append({'item_key': item_key, 'quantity': quantity, 'cost': cost})
    await ctx.put(order_data)
    return order_data


@order_operator.register
async def checkout(ctx: StatefulFunction):
    order_data = await ctx.get()
    total_cost = 0
    for item in order_data['items']:
        # call stock operator to subtract stock
        ctx.call_remote_async(operator_name='stock',
                              function_name='subtract_stock',
                              key=item['item_key'],
                              params=(item['quantity'], ))

        total_cost += item['quantity'] * item['cost']
        # call user operator to subtract credit
    ctx.call_remote_async(operator_name='user',
                          function_name='subtract_credit',
                          key=order_data['user_key'],
                          params=(total_cost, ))
    return order_data
