import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import when, col


# SPARK SESSION ACTIVATE
spark = SparkSession.builder.appName("NH_Data").getOrCreate()
spark.sql("set spark.sql.legacy.timeParserPolicy=CORRECTED")

# GET DATA
df_duped = spark.read.parquet("nh_set.parquet")
print(df_duped.show(2))

df_duped = df_duped.drop("inspection_date")

# EXPLORE DATA

# ~~ per state exploration
# count of def tags by state, year & facility
def_cnt = df_duped.groupBy("insp_year", "state", "facility_id").count()
def_cnt_st = def_cnt.groupBy("state", "facility_id").pivot(
    "insp_year").mean("count")

def_cnt_st.show()

# +-----+-----------+----+----+----+----+----+
# |state|facility_id|2018|2019|2020|2021|2022|
# +-----+-----------+----+----+----+----+----+
# |   NC|     345509|36.0|43.0|23.0|32.0|NULL|
# |   OH|     365552|30.0|24.0|NULL|NULL| 9.0|
# |   TX|     675924| 9.0|NULL| 3.0|NULL| 1.0|
# ...

# below table  shows which facility was inspected which year -
# def_cnt_st.withColumns(
# {"2018": when(col("2018").isNull(), 0).otherwise(1)}).show()

fac_insp_yrs = def_cnt_st.withColumns({cols: when(col(cols).isNull(), 0).otherwise(1)
                                       for cols in def_cnt_st.columns[2:]})
# years inspected
fac_insp_yrs = fac_insp_yrs.withColumn(
    "yrs_inspected", sum([col(c) for c in def_cnt_st.columns[2:]]))


fac_insp_yrs.toPandas().stack()
# stack(columns = ["2018","2019","2020","2021","2022"])

# def_cnt_st.drop("facility_id").groupBy("state").mean().show()

# SCOPE SEVERITY

scope_sev = df_duped.select("insp_year", "scope_severity").groupBy(
    "insp_year", "scope_severity").count().orderBy("scope_severity", "insp_year").toPandas()

# INSPECTION SUMMARY ~~~
# how many facilities per state inspected each year? by facility id
cnt_fac_st_yr = df_duped.dropDuplicates(
    ["state", "insp_year", "facility_id"]).groupBy(["state", "insp_year"]).count()

cnt_fac_st_yr.orderBy("state", "insp_year").show()
# +-----+---------+-----+
# |state|insp_year|count|
# +-----+---------+-----+
# |   AK|     2018|   17|
# |   AK|     2019|   13|
# |   AK|     2020|   18|
# |   AK|     2021|   20|
# |   AK|     2022|   15|
# ...

# get unique facilities list
list_fac_id = df_duped.dropDuplicates(
    ["facility_id"]).select(['facility_id']).collect()


# year counts.

# ~~ state-year counts of inspection - by eventid (multiple inspections)
insp_by_st_yr = (
    df_duped.dropDuplicates(["state", "insp_year", "facility_id", "eventid"])
    .groupBy(["state", "insp_year"])
    .count()
)

# ** ~~~ rate of change in inspections per year
win = Window.partitionBy('state').orderBy('insp_year')
insp_by_st_yr = insp_by_st_yr.withColumn(
    'p_chg', (insp_by_st_yr['count'] - lag(insp_by_st_yr['count']).over(win)))
insp_by_st_yr = insp_by_st_yr.withColumn(
    'per_ch', insp_by_st_yr['p_chg']/insp_by_st_yr['count'])


# ** ~~ drop in inspections
insp_by_st_yr[insp_by_st_yr.insp_year == 2020].sort_values("count_y")

# ** ~~ states with lowest inspection (by population, etc.)

# state-year counts of total def tags (across facilities)
tags_st_yr = df_duped.groupBy(["state", "insp_year"]).count()
tags_st_yr.filter(tags_st_yr.state == "TN").show()

st_list = tags_st_yr.groupBy("state").count().select("state").collect()


# *~~ merge with geocoded info
# plotting data for NH co-ordinates

nh_coded = spark.read.csv("all_nh_geocoded.csv", header=True)
nh_coded = nh_coded.drop("_c0", "facility_name",
                         "address", "city", "state", "zip")
insp_dt_latlon = df_duped.join(nh_coded, on="facility_id", how="left")
insp_dt_latlon = insp_dt_latlon.dropDuplicates()

# saving NH geocoded data for inspections
insp_dt_latlon.dropDuplicates(["state", "insp_year", "facility_id",
                               "deficiency_tag"]).select("facility_id", "insp_year", "lat",
                                                         "lon", "deficiency_tag").write.csv("nh_LatLon_deftags_2024.csv",
                                                                                            mode='overwrite')


# some plots
sns_fig = sns.relplot(
    data=scope_sev,
    kind="line",
    x="insp_year",
    y="count",
    col="scope_severity",
    height=2.5,
    col_wrap=3,
    sharex='row'
)

# sns_fig.savefig("output.png")

# calculating dispersion in geographic data.

plt.show()
