import matplotlib.pyplot as plt
import numpy as np
from matplotlib import rcParams, rc

rcParams['figure.figsize'] = [10, 6]
plt.rcParams.update({'font.size': 18})
x = np.array([100, 300, 500, 700, 1000, 1200, 1500, 2000, 3000, 6000, 8000, 10000, 12000, 15000,
              17000, 20000, 22000, 24000, 26000, 28000, 30000])
y_styx_50 = np.array([3, 4, 4, 4, 4, None, 5, 5, 6, 8, 10, 12, 30, 35,
                      125, 130, 137, 123, 252, 362, 795]).astype(np.double)
styx_50_mask = np.isfinite(y_styx_50)
y_beldi_50 = [127.16, 4516.11, 7253.37, 21682.44, 44128.56, 43848.15, 43843.92,
              None, None, None, None, None, None, None, None, None, None, None, None, None, None]
y_statefun_50 = np.array([165, 169, 184, 164, 179, 191, 518, 733, 14199,
                          None, None, None, None, None, None, None, None, None, None, None, None]).astype(np.double)
y_statefun_50_mask = np.isfinite(y_statefun_50)
y_boki_50 = np.array([36.98, 652.75, 3343.01, 7352.44, 7440.77, 6805.54, 6559.17,
                      None, None, None, None, None, None, None, None, None, None, None, None, None, None]).astype(np.double)
y_boki_50_mask = np.isfinite(y_boki_50)

y_styx_99 = np.array([5, 9, 9, 10, 10, None, 11, 11, 12, 14, 17, 26,
                      153, 157, 291, 330, 323, 334, 568, 1200, 6758]).astype(np.double)
styx_99_mask = np.isfinite(y_styx_99)
y_beldi_99 = [194.97, 6122.91, 22598.58, 34628.25, 59677.38, 79835.63, 84265.19,
              None, None, None, None, None, None, None, None, None, None, None, None, None, None]
y_statefun_99 = np.array([327.01, 509, 608.02, 609, 742, 1002, 1346, 4316, 26266.01,
                          None, None, None, None, None, None, None, None, None, None, None, None]).astype(np.double)
y_statefun_99_mask = np.isfinite(y_statefun_99)
y_boki_99 = np.array([3802.81, 4891.43, 6255.69, 7655.27, 7744.3, 6552.39, 7395.58,
                      None, None, None, None, None, None, None, None, None, None, None, None, None, None]).astype(np.double)
y_boki_99_mask = np.isfinite(y_boki_99)

line_width = 2.5
marker_size = 8
plt.grid(linestyle="--", linewidth=1.5)
plt.plot(x[styx_50_mask], y_styx_50[styx_50_mask], "-o", color="#882255", label="Styx 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[styx_99_mask], y_styx_99[styx_99_mask], "--", marker="o", color="#882255", label="Styx 99p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x, y_beldi_50, "-^", color="#BD7105", label="Beldi 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x, y_beldi_99, "--", marker="^", color="#BD7105", label="Beldi 99p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[y_statefun_50_mask], y_statefun_50[y_statefun_50_mask], "-x", color="#005F20", label="T-Statefun 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[y_statefun_99_mask], y_statefun_99[y_statefun_99_mask], "--", marker="x", color="#005F20",
         label="T-Statefun 99p", linewidth=line_width, markersize=marker_size)
plt.plot(x[y_boki_50_mask], y_boki_50[y_boki_50_mask], "-d", color="#332288", label="Boki 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[y_boki_99_mask], y_boki_99[y_boki_99_mask], "--", marker="d", color="#332288", label="Boki 99p",
         linewidth=line_width, markersize=marker_size)
plt.legend(bbox_to_anchor=(0.5, 1.2), loc="center", ncol=4)
# plt.ylim([1, 100000])
# plt.xlim([100, 50000])
plt.ylabel("Latency (ms)")
plt.xlabel("Input Throughput (transactions/s)")
plt.yscale('log', base=10)
plt.xscale('log', base=10)
plt.tight_layout()
plt.savefig("throughput_latency_uniform.pdf")
plt.show()
