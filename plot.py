#print("Hello world!")
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

class Element:

    def __init__(self, s):
        s1, s2 = s.split("\t")
        self.id = int(s1)
        self.x, self.y, self.z = s2.split(" - ")
        self.x = float(self.x)
        self.y = float(self.y)
        self.z = float(self.z)

def main():
    #dataset = open("../OUTPUT_MALL_1/part-r-00000", "r")
    dataset = open("KMeans/part-r-00000", "r")

    x = [[],[],[],[]]
    y = [[],[],[],[]]
    z = [[],[],[],[]]
    n = [[],[],[],[]]

    elements = []

    for line in dataset.readlines():
        ele = Element(line)
        x[ele.id].append(ele.x)
        y[ele.id].append(ele.y)
        z[ele.id].append(ele.z)

    fig = plt.figure()
    ax = plt.axes(projection='3d')
    #ax.set_xlabel("Age")
    #ax.set_ylabel("Annual Income (k$)")
    #ax.set_zlabel("Spending Score")

    for i in range(len(x)):
        ax.scatter3D(x[i], y[i], z[i])
    #plt.plot(xdata, ydata)
    plt.show()

#def if __name__ == "__main__":
main()
