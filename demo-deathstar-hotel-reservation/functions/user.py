from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

user_operator = Operator('user', n_partitions=4)
# key: username
# value: password


@user_operator.register
async def create(ctx: StatefulFunction, password: str):
    await ctx.put(password)
    return ctx.key


@user_operator.register
async def check_user(ctx: StatefulFunction, password: str):
    stored_password: str = await ctx.get()
    return stored_password == password
