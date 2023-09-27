from styx.common.operator import StatefulFunction, Operator

stock_operator = Operator('stock', n_partitions=6)


class NotEnoughStock(Exception):
    pass


@stock_operator.register
async def create_item(ctx: StatefulFunction, name: str, price: int):
    await ctx.put({'name': name, 'price': price, 'stock': 0})
    return ctx.key


@stock_operator.register
async def add_stock(ctx: StatefulFunction, stock: int):
    item_data = await ctx.get()
    item_data['stock'] += stock
    await ctx.put(item_data)
    return item_data


@stock_operator.register
async def subtract_stock(ctx: StatefulFunction, stock: int):
    item_data = await ctx.get()
    item_data['stock'] -= stock
    if item_data['stock'] < 0:
        raise NotEnoughStock(f'Not enough stock: {item_data["stock"]} for item: {ctx.key}')
    await ctx.put(item_data)
    return item_data
