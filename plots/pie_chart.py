import seaborn as sns
from matplotlib import pyplot as plt

tips = sns.load_dataset("tips")

sns.histplot(data=tips, x="day", hue="sex", multiple="dodge", shrink=.8)
plt.show()
