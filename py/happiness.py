import matplotlib.pyplot as plt
import numpy as np

from matplotlib.colors import LightSource
from mpl_toolkits.basemap import Basemap, shiftgrid, cm
from osgeo import gdal

print("Reading csv")
csv = np.genfromtxt('data/happiness.csv', delimiter=',')
dataByDate = {}

print("Grouping rows by date")
for row in csv:
    dateKey = str(int(row[2]))
    if dateKey not in dataByDate:
        dataByDate[dateKey] = []
    dataByDate[dateKey].append([row[0], row[1], row[3]])

print("Plotting data")
for dateKey in dataByDate.keys():
    print("Generating plot for " + dateKey)
    fig = plt.figure()

    # Main axis
    ax = fig.add_axes([0.1, 0.1, 0.8, 0.8])
    ax.set_title(dateKey[0:4] + "-" + dateKey[4:6] + "-" + dateKey[6:8] + " H" + dateKey[8:10])

    # Europe bounding box
    lon1 = -27
    lon2 = 45
    lat1 = 33
    lat2 = 73

    # Create the map
    m = Basemap(resolution="l", projection="merc", ax=ax, lat_ts=(lon1 + lon2) / 2,
                llcrnrlat=lat1, urcrnrlat=lat2, llcrnrlon=lon1, urcrnrlon=lon2)

    # Draw regions
    m.drawcoastlines()
    m.drawcountries()

    # Draw parallels and meridians
    m.drawparallels(np.arange(0, 90, 10), labels=[1, 0, 0, 1])
    m.drawmeridians(np.arange(0, 360, 15), labels=[1, 0, 0, 1])

    # Draw the levels as a color mesh
    scalef = 100000
    shape = (int(m.urcrnry / scalef), int(m.urcrnrx / scalef))
    data = np.zeros(shape=shape, dtype=float)

    for row in dataByDate[dateKey]:
        x, y = m(row[1], row[0])
        x = int(x / scalef)
        y = int(y / scalef)
        if y >= 0 and y < shape[0] and x >= 0 and x < shape[1]:
            data[y][x] = (data[y][x] + row[2]) / 2

    x = np.linspace(m.llcrnrx, m.urcrnrx, data.shape[1])
    y = np.linspace(m.llcrnry, m.urcrnry, data.shape[0])

    xx, yy = np.meshgrid(x, y)
    colormesh = m.pcolormesh(xx, yy, data, vmin=0.0, vmax=1.0, cmap="plasma", shading="gouraud")

    # Add a color bar
    cb = m.colorbar(colormesh, label="Polarity")

    # Save the plot
    plt.savefig("./out/" + dateKey + ".png")
    plt.close()
