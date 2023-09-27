import matplotlib.pyplot as plt
import numpy as np
from matplotlib import rcParams, rc

rcParams['figure.figsize'] = [9, 5]
plt.rcParams.update({'font.size': 16})
x = [1, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20]
starting = 4185
y_styx = [4185,
7631,
14436,

21619,

27249,

34022,
38081,
43762,
46823,
51207,
56445]
y_styx = [measurement / starting for i, measurement in enumerate(y_styx)]
print(x)
print(y_styx)
y_linear = x

plt.grid(linestyle="--")
plt.plot(x, y_styx, "-o", color="#004488", label="Styx")
plt.plot(x, y_linear, "--", color="#bb5566", label="Linear")
plt.legend()
plt.xticks(x)
# plt.xlim([1, 20])
plt.ylim([1, 20])
plt.ylabel("Speedup")  # Max throughput
plt.xlabel("Workers")
plt.tight_layout()
plt.savefig("scalability.pdf")
plt.show()
