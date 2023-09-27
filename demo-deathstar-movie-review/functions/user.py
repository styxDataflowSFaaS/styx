from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction
# from styx.common.logging import logging


user_operator = Operator('user', n_partitions=40)
# key: username


@user_operator.register
async def register_user(ctx: StatefulFunction, user_data: dict):
    await ctx.put(user_data)
    return ctx.key


@user_operator.register
async def upload_user(ctx: StatefulFunction, req_id: str):
    user_data = await ctx.get()
    # logging.warning(f'key: {ctx.key}: {user_data}')
    ctx.call_remote_async(operator_name='compose_review',
                          function_name='upload_user_id',
                          key=req_id,
                          params=(user_data["userId"], )
                          )
