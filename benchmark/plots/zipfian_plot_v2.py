import matplotlib.pyplot as plt
import numpy as np
from matplotlib import rcParams, rc

rcParams['figure.figsize'] = [12, 7]
plt.rcParams.update({'font.size': 16})

x_labels = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.99, 0.999]

# x = np.arange(len(x_labels))
x = np.array(x_labels)
y_styx_50 = [25, 26, 26, 26, 25, 26, 26, 26, 26, 26, 26, 26]
y_styx_4000_50 = [25, 28, 34, 27, 27, 31, 28, 29, 31, 56, 150, 198]
y_beldi_50 = np.array([220.3789565, 208.6210853, 249.6830524, 70.56663229, 71.86626438, 100.169896,
                       108.6091816, 66.44108574, 66.08338829, 76.86915857, 66.69177072, 71.11195405]).astype(np.double)
beldi_50_mask = np.isfinite(y_beldi_50)
y_statefun_50 = np.array([184, 154, 192, 173, 174, 180, 177, 186, None, 111, None, None]).astype(np.double)
statefun_50_mask = np.isfinite(y_statefun_50)

y_styx_99 = [44, 50, 45, 50, 44, 51, 45, 51, 47, 49, 54, 55]
y_styx_4000_99 = [47, 64, 68, 50, 53, 97, 62, 64, 58, 300, 3355, 12812]
y_beldi_99 = np.array([405.976592, 521.8913386, 412.0119059, 342.9947546, 350.7082529, 388.9832839, 385.5355674,
                       284.4004507, 318.5102448, 1084.654807, 6929.770413, 14583.53034]).astype(np.double)
beldi_99_mask = np.isfinite(y_beldi_99)
y_statefun_99 = np.array([608.02, 619, 692, 528.01, 586.01, 634.03, 693.01, 625.01, None, 2204.76, None, None]).astype(np.double)
statefun_99_mask = np.isfinite(y_statefun_99)

plt.grid(axis="y", linestyle="--")
plt.plot(x, y_styx_50, "-o", color="#b2abd2", label="Styx@500 50p")
plt.plot(x, y_styx_99, "--", marker="o", color="#b2abd2", label="Styx@500 99p")
# ax0.set_xticklabels(x_labels)
plt.plot(x, y_styx_4000_50, "-o", color="#5e3c99", label="Styx@4K 50p")
plt.plot(x, y_styx_4000_99, "--", marker="o", color="#5e3c99", label="Styx@4K 99p")
plt.plot(x[beldi_50_mask], y_beldi_50[beldi_50_mask], "-^", color="#e66101", label="Beldi@500 50p")
plt.plot(x[beldi_99_mask], y_beldi_99[beldi_99_mask], "--", marker="^", color="#e66101", label="Beldi@500 99p")
plt.plot(x[statefun_50_mask], y_statefun_50[statefun_50_mask], "-x", color="#fdb863", label="T-Statefun@500 50p")
plt.plot(x[statefun_99_mask], y_statefun_99[statefun_99_mask], "--", marker="x", color="#fdb863", label="T-Statefun500 99p")

plt.ylim([0, 1000])
plt.ylabel("Latency (ms)")
plt.xlabel("Zipfian const")
plt.legend(bbox_to_anchor=(0.5, 1.2), loc="center", ncol=4)
plt.tight_layout()
plt.savefig("zipfian.pdf")
plt.show()
