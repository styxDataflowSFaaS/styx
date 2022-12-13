from styx.common.operator import StatefulFunction, Operator

user_operator = Operator('user', n_partitions=6)


class NotEnoughCredit(Exception):
    pass


@user_operator.register
async def create_user(ctx: StatefulFunction, name: str):
    await ctx.put({'name': name, 'credit': 0})
    return ctx.key


@user_operator.register
async def add_credit(ctx: StatefulFunction, credit: int):
    user_data = await ctx.get()
    user_data['credit'] += credit
    await ctx.put(user_data)
    return user_data


@user_operator.register
async def subtract_credit(ctx: StatefulFunction, credit: int):
    user_data = await ctx.get()
    user_data['credit'] -= credit
    if user_data['credit'] < 0:
        raise NotEnoughCredit(f'Not enough credit: {user_data["credit"]} for user: {ctx.key}')
    await ctx.put(user_data)
    return user_data
