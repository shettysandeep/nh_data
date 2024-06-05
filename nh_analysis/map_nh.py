# working with geocoded NH data
from pyspark.sql import SparkSession
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
import matplotlib.pyplot as plt
from pyproj import CRS

#  read file
spark = SparkSession.builder.appName("nh_mapping").getOrCreate()
# nh_geo = spark.read.csv("nh_LatLon_2024.csv")
nh_geo = spark.read.csv("nh_LatLon_deftags_2024.csv")
nh_geo = nh_geo.toPandas()
spark.stop()  # stop spark session - only to read files

# rename columns
cols = ['facility_id', 'year', 'lat', 'lon', 'def_tags']
zipped = dict(zip(nh_geo.columns.to_list(), cols))
nh_geo.rename(columns=zipped, inplace=True)
nh_geo = nh_geo.dropna()
nh_geo2 = nh_geo.groupby(
    ["facility_id", "year", "lat", "lon"]).count().reset_index()
# import US shape file
us_shp = gpd.read_file("tl_2023_us_state/tl_2023_us_state.shp")
# us_shp = us_shp.set_crs("EPSG:4326")

# designate coordinate system
crs = CRS("epsg:4326")
# zip x and y coordinates into single feature
geometry = [Point(xy) for xy in zip(nh_geo2['lon'], nh_geo2['lat'])]
# create GeoPandas dataframe
geo_df = gpd.GeoDataFrame(nh_geo2, crs=crs, geometry=geometry)

# create figure and axes, assign to subplot
fig, ax = plt.subplots(figsize=(15, 15))
# add .shp mapfile to axes
us_shp.plot(ax=ax, alpha=0.4, color='grey')
geo_df.plot(column='def_tags', ax=ax, alpha=0.5, legend=False, markersize=10)
# add title to graph
plt.title("Nursing Homes Inspected in Florida: 2018-2022",
          fontsize=15, fontweight="bold")
# set latitiude and longitude boundaries for map display
plt.xlim(-87.634938, -80.031362)
plt.ylim(23.523096, 31.000888)
# show map
plt.show()
