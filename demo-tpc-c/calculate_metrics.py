import math

import pandas as pd
import numpy as np


input_msgs = pd.read_csv('client_requests.csv', dtype={'request_id': np.uint64,
                                                       'timestamp': np.uint64}).sort_values('timestamp')
output_msgs = pd.read_csv('output.csv', dtype={'request_id': np.uint64,
                                               'timestamp': np.uint64}, low_memory=False).sort_values('timestamp')

# verif_messages = output_msgs.tail(n_keys)
output_run_messages = output_msgs

joined = pd.merge(input_msgs, output_run_messages, on='request_id', how='outer').dropna()

runtime = joined['timestamp_y'] - joined['timestamp_x']

print(f'min latency: {min(runtime)}ms')
print(f'max latency: {max(runtime)}ms')
print(f'average latency: {np.average(runtime)}ms')
print(f'99%: {np.percentile(runtime, 99)}ms')
print(f'95%: {np.percentile(runtime, 95)}ms')
print(f'90%: {np.percentile(runtime, 90)}ms')
print(f'75%: {np.percentile(runtime, 75)}ms')
print(f'60%: {np.percentile(runtime, 60)}ms')
print(f'50%: {np.percentile(runtime, 50)}ms')
print(f'25%: {np.percentile(runtime, 25)}ms')
print(f'10%: {np.percentile(runtime, 10)}ms')
print(np.argmax(runtime))
print(np.argmin(runtime))

missed = joined[joined['response'].isna()]

if len(missed) > 0:
    print('--------------------')
    print('\nMISSED MESSAGES!\n')
    print('--------------------')
    print(missed)
    print('--------------------')
else:
    print('\nNO MISSED MESSAGES!\n')

start_time = -math.inf
throughput = {}
bucket_id = -1

# 1 second (ms) (i.e. bucket size)
granularity = 1000

for t in output_msgs['timestamp']:
    if t - start_time > granularity:
        bucket_id += 1
        start_time = t
        throughput[bucket_id] = 1
    else:
        throughput[bucket_id] += 1

# HINT: in this example we don't have constant load that's why the spikes
print(throughput)
real_throughput = list(throughput.values())[1:-2]

print(f'Max throughput: {max(real_throughput)}')
print(f'Average throughput: {sum(real_throughput) / len(real_throughput)}')

req_ids = output_msgs['request_id']
dup = output_msgs[req_ids.isin(req_ids[req_ids.duplicated()])].sort_values("request_id")

print(f'Number of input messages: {len(input_msgs)}')
print(f'Number of output messages: {len(output_msgs)}')
print(f'Number of duplicate messages: {len(dup)}')

if len(dup) > 0:
    print('--------------------')
    print('\nDUPLICATE MESSAGES!\n')
    print('--------------------')
    print(dup)
    print('--------------------')
else:
    print('\nNO DUPLICATE MESSAGES!\n')
