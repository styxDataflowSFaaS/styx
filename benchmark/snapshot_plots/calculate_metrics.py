import math
import re

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib import rcParams
import pandas as pd
import numpy as np

plt.rcParams.update({'font.size': 20})
rcParams['figure.figsize'] = [8, 8]

n_keys = 1_000_000
# n_keys = 100
starting_money = 1_000_000


input_msgs = pd.read_csv('client_requests.csv', dtype={'request_id': np.uint64,
                                                       'timestamp': np.uint64}).sort_values('timestamp')
output_msgs = pd.read_csv('output.csv', dtype={'request_id': np.uint64,
                                               'timestamp': np.uint64}, low_memory=False).sort_values('timestamp')

init_messages = output_msgs.head(n_keys)
verif_messages = output_msgs.tail(n_keys)
output_run_messages = output_msgs.iloc[n_keys:len(output_msgs)-n_keys]

joined = pd.merge(input_msgs, output_run_messages, on='request_id', how='outer')

runtime = joined['timestamp_y'] - joined['timestamp_x']
runtime_no_nan = runtime
print(f'min latency: {min(runtime_no_nan)}ms')
print(f'max latency: {max(runtime_no_nan)}ms')
print(f'average latency: {np.average(runtime_no_nan)}ms')
print(f'99%: {np.percentile(runtime_no_nan, 99)}ms')
print(f'95%: {np.percentile(runtime_no_nan, 95)}ms')
print(f'90%: {np.percentile(runtime_no_nan, 90)}ms')
print(f'75%: {np.percentile(runtime_no_nan, 75)}ms')
print(f'60%: {np.percentile(runtime_no_nan, 60)}ms')
print(f'50%: {np.percentile(runtime_no_nan, 50)}ms')
print(f'25%: {np.percentile(runtime_no_nan, 25)}ms')
print(f'10%: {np.percentile(runtime_no_nan, 10)}ms')
print(np.argmax(runtime_no_nan))
print(np.argmin(runtime_no_nan))

missed = joined[joined['response'].isna()]

if len(missed) > 0:
    print('--------------------')
    print('\nMISSED MESSAGES!\n')
    print('--------------------')
    print(missed)
    print('--------------------')
else:
    print('\nNO MISSED MESSAGES!\n')


def use_regex(input_text):
    pattern = re.compile(r"coordinator_1  \| [0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?"
                         r"([Zz]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?\.[0-9]+ WARNING:  Worker: ([0-9]+) \| "
                         r"Completed snapshot: ([0-9]+) \| started at: ([0-9]*\.[0-9]+) \| "
                         r"ended at: ([0-9]*\.[0-9]+) \| took: ([0-9]*\.[0-9]+ms)", re.IGNORECASE)
    return pattern.match(input_text)


snapshot_starts: dict[int, int] = {}
snapshot_ends: dict[int, int] = {}

with open('coordinator_logs.txt') as file:
    for line in file:
        r_groups = use_regex(line).groups()
        worker_id = int(r_groups[5])
        sn_id = int(r_groups[6])
        start = int(round(float(r_groups[7])))
        end = int(round(float(r_groups[8])))
        total = r_groups[9]
        # print(worker_id, sn_id, start, end, total)
        if sn_id in snapshot_starts:
            snapshot_starts[sn_id] = min(snapshot_starts[sn_id], start)
        else:
            snapshot_starts[sn_id] = start
        if sn_id in snapshot_ends:
            snapshot_ends[sn_id] = max(snapshot_ends[sn_id], end)
        else:
            snapshot_ends[sn_id] = end


def update_ticks(x, pos):
    # if x == 0:
    #     return 'Mean'
    # elif pos == 6:
    #     return 'pos is 6'
    # else:
    #     return x
    return int(x / 10)


def update_throughput_ticks(x, pos):
    return int(x / 1000)


def calculate_throughput(output_timestamps: list[int], granularity_ms: int,
                         sn_start: dict[int, int], sn_end: dict[int, int]):
    second_coefficient = 1000 / granularity_ms
    output_timestamps.sort()
    start_time = output_timestamps[0]
    end_time = output_timestamps[-1]
    bucket_boundaries = list(range(start_time, end_time, granularity_ms))
    bucket_boundaries = [(bucket_boundaries[i], bucket_boundaries[i + 1]) for i in range(len(bucket_boundaries)-1)]
    bucket_counts: dict[int, int] = {i: 0 for i in range(len(bucket_boundaries))}
    bucket_counts_input: dict[int, int] = {i: 0 for i in range(len(bucket_boundaries))}
    for t in output_timestamps:
        for i, boundaries in enumerate(bucket_boundaries):
            if boundaries[0] <= t < boundaries[1]:
                bucket_counts[i] += 1 * second_coefficient

    for t in input_msgs['timestamp']:
        for i, boundaries in enumerate(bucket_boundaries):
            if boundaries[0] <= t < boundaries[1]:
                bucket_counts_input[i] += 1 * second_coefficient

    normalized_sn_starts: list[float] = []
    sn_starts = [sn_t for sn_t in sn_start.values() if start_time <= sn_t <= end_time]
    for t in sn_starts:
        for i, boundaries in enumerate(bucket_boundaries):
            if boundaries[0] <= t < boundaries[1]:
                normalized_sn_starts.append(((t - boundaries[0]) / granularity_ms) + i)

    normalized_sn_ends: list[float] = []
    sn_ends = [sn_t for sn_t in sn_end.values() if start_time <= sn_t <= end_time]
    for t in sn_ends:
        for i, boundaries in enumerate(bucket_boundaries):
            if boundaries[0] <= t < boundaries[1]:
                normalized_sn_ends.append(((t - boundaries[0]) / granularity_ms) + i)

    _, ax = plt.subplots()
    ax.plot(bucket_counts.values(), linewidth=2.5, label='Output Throughput')
    ax.plot(bucket_counts_input.values(), linewidth=2.5, label='Input Throughput', alpha=0.7)
    ax.vlines(normalized_sn_starts,
              ymin=0, ymax=5000, colors='green', linestyle='--', linewidth=3, label='Snapshot Start')
    ax.vlines(normalized_sn_ends,
              ymin=0, ymax=5000, colors='red', linestyle='--', linewidth=3, label='Snapshot End')
    # ax.axhline(y=3200, color='orange', linestyle='-')
    ax.set_xlabel('Time (seconds)')
    ax.set_ylabel('K Transactions per Second')
    ax.set_xlim([100, 200])
    ax.set_ylim([0, 4900])
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(update_throughput_ticks))
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(update_ticks))
    ax.legend(bbox_to_anchor=(0.5, 1.08), loc="center", ncol=2)
    plt.grid(linestyle='--', linewidth=0.5)
    plt.savefig("throughput_snapshot.pdf")
    plt.show()
    return bucket_counts, normalized_sn_ends, normalized_sn_starts


bc, normalized_sn_ends, normalized_sn_starts = calculate_throughput(list(output_run_messages['timestamp']),
                                                                    100,
                                                                    snapshot_starts,
                                                                    snapshot_ends)

print(bc)

req_ids = output_msgs['request_id']
dup = output_msgs[req_ids.isin(req_ids[req_ids.duplicated()])].sort_values("request_id")

print(f'Number of input messages: {len(input_msgs)}')
print(f'Number of output messages: {len(output_msgs) - 2 * n_keys}')
print(f'Number of duplicate messages: {len(dup)}')

if len(dup) > 0:
    print('--------------------')
    print('\nDUPLICATE MESSAGES!\n')
    print('--------------------')
    print(dup)
    print('--------------------')
else:
    print('\nNO DUPLICATE MESSAGES!\n')


# Consistency test
verification_state_reads = {int(e[0]): int(e[1]) for e in [res.strip('][').split(', ')
                                                           for res in output_msgs['response'].tail(n_keys)]}


transaction_operations = [(int(op[0]), int(op[1])) for op in [op.split(' ')[1].split('->') for op in input_msgs['op']]]
true_res = {key: starting_money for key in range(n_keys)}

for op in transaction_operations:
    send_key, rcv_key = op
    true_res[send_key] -= 1
    true_res[rcv_key] += 1

print(f'Are we consistent: {true_res == verification_state_reads}')

missing_verification_keys = []
for res in true_res.items():
    key, value = res
    if key in verification_state_reads and verification_state_reads[key] != value:
        print(f'For key: {key} the value should be {value} but it is {verification_state_reads[key]}')
    elif key not in verification_state_reads:
        missing_verification_keys.append(key)

print(f'\nMissing {len(missing_verification_keys)} keys in the verification')


# plt.plot([val / 10 for val in throughput.keys()], [val for val in throughput.values()])
# plt.vlines([val / 1000 for val in snapshot_starts.values()], ymin=0, ymax=30000,
#            colors='green', linestyle='--', linewidth=1, label='Snapshot Start')
# plt.vlines([val / 1000 for val in snapshot_ends.values()], ymin=0, ymax=30000,
#            colors='red', linestyle='--', linewidth=1, label='Snapshot End')
# plt.title('Output Throughput')
# plt.xlabel('Seconds')
# plt.ylabel('Transactions')
# # plt.xlim([0, 9])
# # plt.ylim([0, 1500])
# plt.grid(linestyle='--', linewidth=0.5)
# plt.show()
#

timestamps = list(output_run_messages['timestamp'])
latencies = list(runtime)
start_time = timestamps[0]
end_time = timestamps[-1]
granularity_ms = 100
bucket_boundaries = list(range(start_time, end_time, granularity_ms))
bucket_boundaries = [(bucket_boundaries[i], bucket_boundaries[i + 1]) for i in range(len(bucket_boundaries) - 1)]

bucket_latencies: dict[int, list[float]] = {i: [] for i in range(len(bucket_boundaries))}

for i, t in enumerate(timestamps):
    for j, boundaries in enumerate(bucket_boundaries):
        if boundaries[0] <= t < boundaries[1]:
            bucket_latencies[j].append(latencies[i])

for k, v in bucket_latencies.items():
    if not v:
        bucket_latencies[k].append(np.nan)

bucket_latencies_99: dict[int, float] = {k: np.percentile(v, 99) for k, v in bucket_latencies.items() if v}
bucket_latencies_50: dict[int, float] = {k: np.percentile(v, 50) for k, v in bucket_latencies.items() if v}

normalized_sn_starts: list[float] = []
sn_starts = [sn_t for sn_t in snapshot_starts.values() if start_time <= sn_t <= end_time]
for t in sn_starts:
    for i, boundaries in enumerate(bucket_boundaries):
        if boundaries[0] <= t < boundaries[1]:
            normalized_sn_starts.append(((t - boundaries[0]) / granularity_ms) + i)

normalized_sn_ends: list[float] = []
sn_ends = [sn_t for sn_t in snapshot_ends.values() if start_time <= sn_t <= end_time]
for t in sn_ends:
    for i, boundaries in enumerate(bucket_boundaries):
        if boundaries[0] <= t < boundaries[1]:
            normalized_sn_ends.append(((t - boundaries[0]) / granularity_ms) + i)


# def update_latency_ticks(x, pos):
#     return x / 1000


_, ax = plt.subplots()
ax.plot(bucket_latencies_99.keys(), bucket_latencies_99.values(), linewidth=2.5, label='99p')
ax.plot(bucket_latencies_50.keys(), bucket_latencies_50.values(), linewidth=2.5, label='50p')
ax.vlines(normalized_sn_starts, ymin=0, ymax=200,
           colors='green', linestyle='--', linewidth=3, label='Snapshot Start')
ax.vlines(normalized_sn_ends, ymin=0, ymax=200,
           colors='red', linestyle='--', linewidth=3, label='Snapshot End')
ax.set_xlabel('Time (seconds)')
ax.set_ylabel('Latency (ms)')
ax.xaxis.set_major_formatter(mticker.FuncFormatter(update_ticks))
# ax.yaxis.set_major_formatter(mticker.FuncFormatter(update_latency_ticks))
ax.set_ylim([0, 200])
ax.set_xlim([100, 200])
plt.grid(linestyle='--', linewidth=0.5)
ax.legend(bbox_to_anchor=(0.5, 1.08), loc="center", ncol=2)
plt.savefig("latency_snapshot.pdf")
plt.show()
