from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction

compose_review_operator = Operator('compose_review', n_partitions=40)
# key: req_id


# @compose_review_operator.register
# async def upload_req(ctx: StatefulFunction):
#     data = {"counter": 0}
#     await ctx.put(data)


@compose_review_operator.register
async def upload_unique_id(ctx: StatefulFunction, review_id: str):
    compose_review_data = await ctx.get()
    if compose_review_data is None:
        compose_review_data = {"review_id": review_id}
    else:
        compose_review_data["review_id"] = review_id
    await ctx.put(compose_review_data)


@compose_review_operator.register
async def upload_user_id(ctx: StatefulFunction, user_id: str):
    compose_review_data = await ctx.get()
    if compose_review_data is None:
        compose_review_data = {"userId": user_id}
    else:
        compose_review_data["userId"] = user_id
    await ctx.put(compose_review_data)


@compose_review_operator.register
async def upload_movie_id(ctx: StatefulFunction, movie_id: str):
    compose_review_data = await ctx.get()
    if compose_review_data is None:
        compose_review_data = {"movieId": movie_id}
    else:
        compose_review_data["movieId"] = movie_id
    await ctx.put(compose_review_data)


@compose_review_operator.register
async def upload_rating(ctx: StatefulFunction, rating: int):
    compose_review_data = await ctx.get()
    if compose_review_data is None:
        compose_review_data = {"rating": rating}
    else:
        compose_review_data["rating"] = rating
    await ctx.put(compose_review_data)


@compose_review_operator.register
async def upload_text(ctx: StatefulFunction, text: str):
    compose_review_data = await ctx.get()
    if compose_review_data is None:
        compose_review_data = {"text": text}
    else:
        compose_review_data["text"] = text
    await ctx.put(compose_review_data)
