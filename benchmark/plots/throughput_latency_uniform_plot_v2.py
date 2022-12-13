import matplotlib.pyplot as plt
import numpy as np
from matplotlib import rcParams, rc

rcParams['figure.figsize'] = [10, 6]
plt.rcParams.update({'font.size': 16})
x = np.array([100, 300, 500, 700, 1000, 1200, 1500, 2000, 3000, 6000, 8000, 10000, 12000])
y_styx_50 = np.array([21, 28, 25, 26, 26, None, 24, 23, 22, 33, 50, 127, 639]).astype(np.double)
styx_50_mask = np.isfinite(y_styx_50)
y_beldi_50 = [269.90, 267.25, 220.37, 69.87, 78.17, 71.78, 256.39, None, None, None, None, None, None]
y_statefun_50 = [165, 169, 184, 164, 179, 191, 518, 733, 14199, None, None, None, None]

y_styx_99 = np.array([40, 48, 44, 50, 50, None, 44, 41, 41, 67, 281, 695, 1760]).astype(np.double)
styx_99_mask = np.isfinite(y_styx_99)
y_beldi_99 = [400.77, 431.11, 405.97, 21276.83, 46758.28, 53652.28, 57768.81, None, None, None, None, None, None]
y_statefun_99 = [327.01, 509, 608.02, 609, 742, 1002, 1346, 4316, 26266.01, None, None, None, None]

plt.grid(axis="y", linestyle="--")
plt.plot(x[styx_50_mask], y_styx_50[styx_50_mask], "-o", color="#5e3c99", label="Styx 50p")
plt.plot(x[styx_99_mask], y_styx_99[styx_99_mask], "--", marker="o", color="#5e3c99", label="Styx 99p")
plt.plot(x, y_beldi_50, "-^", color="#e66101", label="Beldi 50p")
plt.plot(x, y_beldi_99, "--", marker="^", color="#e66101", label="Beldi 99p")
plt.plot(x, y_statefun_50, "-x", color="#fdb863", label="T-Statefun 50p")
plt.plot(x, y_statefun_99, "--", marker="x", color="#fdb863", label="T-Statefun 99p")
plt.legend(bbox_to_anchor=(0.5, 1.2), loc="center", ncol=3)
# ax0.set_xlim([0, 3])
plt.ylim([0, 1000])
plt.ylabel("Latency (ms)")
plt.xlabel("Input Throughput (transactions/s)")

plt.tight_layout()
plt.savefig("throughput_latency_uniform.pdf")
plt.show()
