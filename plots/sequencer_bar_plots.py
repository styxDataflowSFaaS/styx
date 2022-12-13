import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rcParams

# rcParams['figure.figsize'] = [8, 4]

# creating the dataset
# data_kafka = {'Calvin': 12.5, 'STYX': 5.5}
# y_err_kafka = [(0, 0), (18, 10)]

data = {'Calvin': 5.65, 'STYX': 0.0171}
y_err = [(0, 0), (12.1, 0.0698)]

courses = list(data.keys())
values = list(data.values())

# fig = plt.figure(figsize=(10, 5))
fig = plt.figure()
# creating the bar plot
plt.bar(courses, values, color=('#118ab2', "#ffd166"), width=0.5,
        yerr=y_err, capsize=4, edgecolor="black")

plt.xlabel("Sequencers")
plt.ylabel("Sequencing time (ms)")
# plt.title("Students enrolled in different courses")
plt.show()



# import matplotlib.pyplot as plt
# import numpy as np
# from matplotlib import rcParams, rc
#
# rcParams['figure.figsize'] = [8, 4]
# labels = ["Calvin", "STYX"]
#
# statefun_means = [52]
# statefun_err = [(0, 0), (103, 103)]
#
# universalis_means = [25, 20]
# universalis_err = [(0, 0), (48, 37)]
#
# width = 0.23  # the width of the bars
# x1 = np.arange(len(labels))  # the label locations
# # x2 = [x + width for x in x1]
#
# fig, ax = plt.subplots()
#
# rects1 = ax.bar(
#     x1, statefun_means, width, label="Statefun", yerr=statefun_err, capsize=4,
#     color="#ffd166", edgecolor="black"
# )
# # rects3 = ax.bar(x2, universalis_means, width, label="Stateflow", yerr=universalis_err, capsize=4,
# #                 color="#118ab2", edgecolor="black")
# ax.set_ylabel("Latency (ms)")
# ax.set_xticks([r + width for r in range(len(labels))])
# ax.set_xticklabels(labels)
# plt.ylim([0, 200])
# plt.xlabel("method")
#
# fig.tight_layout()
# plt.savefig("sequencer_bar_plots.pdf")
# plt.show()
