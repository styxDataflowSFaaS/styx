import matplotlib.pyplot as plt
import numpy as np
from matplotlib import rcParams, rc

# rcParams['figure.figsize'] = [8, 6]
plt.rcParams.update({'font.size': 13})

# 1.5k lat 2000 throughput for subsec 50th
means = [5, 518, 6533.81, 43843.92]
errors = [(0, 0, 0, 0), (11, 1346, 7490.29, 84265.19)]

labels = ["Styx", "T-Statefun", "Boki", "Beldi"]

plt.subplot(1, 2, 1)

plt.bar(labels,
        means,
        width=0.5,
        color=['#882255', '#005F20', '#332288', '#BD7105'],
        yerr=errors,
        capsize=4,
        edgecolor="black", zorder=3)

plt.title("Latency@1500TPS (ms)")
plt.yscale('log')
plt.xticks(rotation=45)
plt.grid(which="both", ls="--", zorder=0)

plt.subplot(1, 2, 2)

throughput = [30000, 2000, 250, 200]

plt.bar(labels,
        throughput,
        width=0.5,
        color=['#882255', '#005F20', '#332288', '#BD7105'],
        edgecolor="black", zorder=3)

plt.title("Throughput (TPS)")
plt.yscale('log')
plt.xticks(rotation=45)
plt.grid(which="both", ls="--", zorder=0)

plt.tight_layout()
plt.savefig("comparison_barplot.pdf")
plt.show()
