import matplotlib.pyplot as plt
import numpy as np
from matplotlib import rcParams, rc

rcParams['figure.figsize'] = [9, 5]
plt.rcParams.update({'font.size': 16})
x = [1, 2, 4, 5, 6, 7, 8, 9, 10, 12]
starting = 1378
y_styx = [1378, 3470, 7562, 8930, 9491, 10500, 11191, 11700, 12000, 13186]
y_styx = [measurement / starting for i, measurement in enumerate(y_styx)]
print(y_styx)
y_linear = x

plt.grid(axis="y", linestyle="--")
plt.plot(x, y_styx, "-o", color="#004488", label="Styx")
plt.plot(x, y_linear, "-^", color="#bb5566", label="Linear")
plt.legend()

# plt.xlim([1, 1500])
# plt.ylim([0, 1500])
plt.ylabel("Speedup")  # Max throughput
plt.xlabel("Workers")
plt.tight_layout()
plt.savefig("scalability.pdf")
plt.show()
