import os
import time
from timeit import default_timer as timer
import pandas as pd
from kafka.producer.future import FutureRecordMetadata

from sanic import Sanic
from sanic.response import json
from styx.common.local_state_backends import LocalStateBackend
from styx.common.operator import Operator
from styx.common.stateflow_graph import StateflowGraph
from styx.common.stateflow_ingress import IngressTypes
from styx.common.stateful_function import StatefulFunction

from zipfian_generator import ZipfGenerator
from styx.styx import Styx

app = Sanic(__name__)

STYX_HOST: str = os.environ['STYX_HOST']
STYX_PORT: int = int(os.environ['STYX_PORT'])
KAFKA_URL: str = os.environ['KAFKA_URL']


@app.post('/hello')
async def hello(_):
    return json('Hey', status=200)


@app.post('/submit/<n_partitions:int>')
async def submit_dataflow_graph(_, n_partitions: int):
    print("INIT STARTING")
    app.ctx.styx = Styx(STYX_HOST, STYX_PORT, IngressTypes.KAFKA, kafka_url=KAFKA_URL)
    g = StateflowGraph('ycsb-benchmark', operator_state_backend=LocalStateBackend.DICT)
    app.ctx.ycsb_operator = Operator('ycsb', n_partitions=n_partitions)

    class NotEnoughCredit(Exception):
        pass

    @app.ctx.ycsb_operator.register
    async def insert(ctx: StatefulFunction):
        value: int = 1_000_000
        await ctx.put(value)
        return ctx.key

    @app.ctx.ycsb_operator.register
    async def read(ctx: StatefulFunction):
        value: int = await ctx.get()
        return ctx.key, value

    @app.ctx.ycsb_operator.register
    async def update(ctx: StatefulFunction):
        new_value = await ctx.get()
        new_value += 1

        await ctx.put(new_value)
        return ctx.key, new_value

    @app.ctx.ycsb_operator.register
    async def transfer(ctx: StatefulFunction, key_b: str):
        value_a = await ctx.get()

        ctx.call_remote_async(
            operator_name='ycsb',
            function_name='update',
            key=key_b
        )

        value_a -= 1

        if value_a < 0:
            raise NotEnoughCredit(f'Not enough credit for user: {ctx.key}')

        await ctx.put(value_a)

        return ctx.key, value_a
    g.add_operators(app.ctx.ycsb_operator)
    await app.ctx.styx.submit(g)
    print("Graph submitted")
    return json('Graph submitted', status=200)


@app.post('/init_entities/<n_keys:int>')
async def init_entities(_, n_keys: int):
    timestamp_futures: dict[int, FutureRecordMetadata] = {}
    for i in range(n_keys):
        request_id, metadata = app.ctx.styx.send_kafka_event_no_wait(operator=app.ctx.ycsb_operator,
                                                                     key=i,
                                                                     function='insert')
        timestamp_futures[request_id] = metadata
    timestamp_futures = {k: v.get().timestamp for k, v in timestamp_futures.items()}
    return json('Entities initialized', status=200)


def transactional_ycsb_generator(keys, n: int, zipf_const: float) -> [int, str, tuple[int, ]]:
    zipf_gen = ZipfGenerator(items=n, zipf_const=zipf_const)
    uniform_gen = ZipfGenerator(items=n, zipf_const=0.0)
    while True:
        key = keys[next(uniform_gen)]
        key2 = keys[next(zipf_gen)]
        while key2 == key:
            key2 = keys[next(zipf_gen)]
        yield key, 'transfer', (key2, )


@app.post('/start_benchmark/<n_entities:int>/<messages_per_second:int>'
          '/<sleeps_per_second:int>/<sleep_time:float>/<seconds:int>/<zipf_const:float>')
async def start_benchmark(_, n_entities: int, messages_per_second: int,
                          sleeps_per_second: int, sleep_time: float, seconds: int, zipf_const: float):
    timestamp_futures: dict[int, FutureRecordMetadata] = {}
    key_list = list(range(n_entities))
    ycsb_generator = transactional_ycsb_generator(key_list, n_entities, zipf_const=zipf_const)
    start = timer()
    for _ in range(seconds):
        sec_start = timer()
        for i in range(messages_per_second):
            if i % (messages_per_second // sleeps_per_second) == 0:
                time.sleep(sleep_time)
            key, func_name, params = next(ycsb_generator)
            request_id, metadata = app.ctx.styx.send_kafka_event_no_wait(operator=app.ctx.ycsb_operator,
                                                                         key=key,
                                                                         function=func_name,
                                                                         params=params)
            timestamp_futures[request_id] = metadata
        sec_end = timer()
        print(f'Latency per second: {sec_end - sec_start}')
    end = timer()
    print(f'Average latency per second: {(end - start) / seconds}')
    time.sleep(2)
    timestamp_futures = {k: v.get().timestamp for k, v in timestamp_futures.items()}
    response = {'lat_p_s': (end - start) / seconds} | \
               pd.DataFrame(timestamp_futures.items(), columns=['request_id', 'timestamp']).to_dict()
    return json(response, status=200)


if __name__ == '__main__':
    app.run(debug=False)
