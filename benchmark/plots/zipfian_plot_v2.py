import matplotlib.pyplot as plt
import numpy as np
from matplotlib import rcParams, rc

rcParams['figure.figsize'] = [14, 8]
plt.rcParams.update({'font.size': 22})

x_labels = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.99, 0.999]

# x = np.arange(len(x_labels))
x = np.array(x_labels)
y_styx_50 = [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4]
y_styx_4000_50 = [8, 8, 8, 8, 8, 8, 8, 10, 12, 16, 36, 44]
y_beldi_50 = np.array([7253.37, 7194.78, 10121.84, 7152.47, 9207.48, 10970.37,
                       10467.05, 10933.77, 12284.8, 11059.66, 13032.37, 10091.64]).astype(np.double)
y_boki_50 = np.array([4515.163333, 2815.11, 3065.68, 3157.18, 5638.406667, 5718.49, 3096.573333, 1925.15, 4242.546667,
                      3182.916667, 4459.65, 4130.14]).astype(np.double)
beldi_50_mask = np.isfinite(y_beldi_50)
boki_50_mask = np.isfinite(y_boki_50)
y_statefun_50 = np.array([184, 154, 192, 173, 174, 180, 177, 186, None, 111, None, None]).astype(np.double)
statefun_50_mask = np.isfinite(y_statefun_50)

y_styx_99 = [9, 9, 10, 9, 9, 9, 10, 10, 10, 10, 10, 10]
y_styx_4000_99 = [14, 14, 14, 14, 14, 14, 14, 24, 35, 47, 195, 544]
y_beldi_99 = np.array([22598.58, 22140.66, 21041.11, 18885.75, 19873.66, 20844.66, 20773.36,
                       22066.73, 22426.12, 21789.61, 22894.35, 20823.94]).astype(np.double)
y_boki_99 = np.array([6650.38, 5872.553333, 6732.29, 6224.84, 6740.8, 6985.323333, 6210.826667, 5806.313333,
                      6561.573333, 6236.543333, 6648.58, 6183.4]).astype(np.double)
beldi_99_mask = np.isfinite(y_beldi_99)
boki_99_mask = np.isfinite(y_boki_99)
y_statefun_99 = np.array([608.02, 619, 692, 528.01, 586.01, 634.03, 693.01, 625.01,
                          None, 2204.76, None, None]).astype(np.double)
statefun_99_mask = np.isfinite(y_statefun_99)

line_width = 2.5
marker_size = 8
plt.grid(linestyle="--", linewidth=1.5)
plt.plot(x, y_styx_50, "-p", color="#B9417D", label="Styx 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x, y_styx_99, "--", marker="p", color="#B9417D", label="Styx 99p",
         linewidth=line_width, markersize=marker_size)
# ax0.set_xticklabels(x_labels)
plt.plot(x, y_styx_4000_50, "-o", color="#882255", label="Styx@6K 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x, y_styx_4000_99, "--", marker="o", color="#882255", label="Styx@6K 99p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[beldi_50_mask], y_beldi_50[beldi_50_mask], "-^", color="#BD7105", label="Beldi 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[beldi_99_mask], y_beldi_99[beldi_99_mask], "--", marker="^", color="#BD7105", label="Beldi 99p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[statefun_50_mask], y_statefun_50[statefun_50_mask], "-x", color="#005F20", label="T-Statefun 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[statefun_99_mask], y_statefun_99[statefun_99_mask], "--", marker="x", color="#005F20",
         label="T-Statefun 99p", linewidth=line_width, markersize=marker_size)
plt.plot(x[boki_50_mask], y_boki_50[boki_50_mask], "-d", color="#332288", label="Boki 50p",
         linewidth=line_width, markersize=marker_size)
plt.plot(x[boki_99_mask], y_boki_99[boki_99_mask], "--", marker="d", color="#332288", label="Boki 99p",
         linewidth=line_width, markersize=marker_size)

# plt.ylim([0, 1000])
plt.ylabel("Latency (ms)")
plt.xlabel("Zipfian const")
plt.legend(bbox_to_anchor=(0.5, 1.2), loc="center", ncol=4)
plt.yscale('log', base=10)
plt.tight_layout()
plt.savefig("zipfian.pdf")
plt.show()
