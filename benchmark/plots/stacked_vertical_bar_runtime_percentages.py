import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib import rcParams

rcParams['figure.figsize'] = [7, 2]
plt.rcParams.update({'font.size': 16})

y = [96.22, 1.86, 1.79, 0.13]
print(sum(y))
mylabels = ["Function Execution", "Sequencing", "Transactional Logic", "Snapshots"]
mycolors = ["#56B4E9", "#E69F00", "#009E73", "#0072B2"]

fig, ax = plt.subplots()

start = 0
x1 = y[0]
x2 = y[1]
x3 = y[2]
x4 = y[3]

ax.broken_barh([(start, x1),
                (x1, x1+x2),
                (x1+x2, x1+x2+x3),
                (x1+x2+x3, x1+x2+x3+x4)],
               [12, 12], facecolors=(mycolors[0], mycolors[1], mycolors[2], mycolors[3]))
ax.set_ylim(5, 15)
ax.set_xlim(0, 100)
ax.spines['left'].set_visible(False)
ax.spines['bottom'].set_visible(False)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.set_yticks([])
ax.set_xticks([])

ax.set_axisbelow(True)

ax.text(0, 15.2, f"{y[0]}%", color=mycolors[0])
ax.text(82, 15.2, f"{y[1]}%", color=mycolors[1])
ax.text(90, 10.7, f"{y[2]}%", color=mycolors[2])
ax.text(98, 15.2, f"{y[3]}%", color=mycolors[3])

leg1 = mpatches.Patch(color=mycolors[0], label='Function Execution')
leg2 = mpatches.Patch(color=mycolors[1], label='Sequencing')
leg3 = mpatches.Patch(color=mycolors[2], label='Transactional Logic')
leg4 = mpatches.Patch(color=mycolors[3], label='Snapshots')
ax.legend(handles=[leg1, leg2, leg3, leg4], bbox_to_anchor=(0.5, 0.3), loc="center",  ncol=2)

plt.savefig("runtime_percentages.pdf")
plt.show()
