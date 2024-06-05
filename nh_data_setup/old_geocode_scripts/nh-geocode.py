# packages needed

import pandas as pd
import re
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import geopy
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="sshetty@air.org")

# data snapshot - saved locally
dat = pd.read_csv(
    "2567_nh_data/merged_data/text2567_20190501_cms_reg1.csv", index_col=0, dtype=str
)

# delete columns starting with
# Footnote, Performance, cycle


def del_columns(datname, colname2drop):
    return datname.drop(
        columns=datname.columns[datname.columns.str.startswith(colname2drop)]
    )


drop_cols = ("Footnote", "Performance", "Baseline", "cycle", "CYCLE")

dat_new = del_columns(dat, drop_cols)
dat_new.columns = dat_new.columns.str.lower()

# subset to obtain latitude and longitude
nh_homes = dat_new[~dat_new[["address", "city", "state", "zip"]].duplicated()].copy()
nh_homes = nh_homes[["address", "city", "state", "zip"]]

# get coordinate from Census API
URL = r"https://geocoding.geo.census.gov/geocoder/locations/address?street={}&city ={}&state={}&zip={}&benchmark=Public_AR_Census2020&format=json"


def combine_address_and_return_url(row, geo_coder=False):
    """Convert columns into the form needed to request via Census API
    Function applied to a row (of 4 values) at a time
    """
    add, city, state, zip_code = row
    x_list = add.split()
    street = "+".join(x_list)
    zip2 = str(zip_code)

    if geo_coder == True:
        x_list1 = x_list + [city, state, zip2]
        full_add = " ".join(x_list1)
        print(full_add)
        try:
            location = geolocator.geocode(full_add)
            return (location.longitude, location.latitude)
        except:
            None
    else:
        url_return = URL.format(street, city, state, zip_code)
        # print(url_return)
        if url_return:
            # request data via Census API
            content = requests.get(url_return)
            if content.status_code == 200:
                try:
                    if content.json()["result"]["addressMatches"] != []:
                        lon, lat = content.json()["result"]["addressMatches"][0][
                            "coordinates"
                        ].values()
                        return (lon, lat)
                except:
                    # sometime the json is corrupted this accounts for those cases
                    # other approach is to dig through the html. Skipping for now.
                    None


def lat_lon_getter(data_frame):
    """Function to request latitude and longitude
    Input: DataFrame with only 4 variables [street, city, state, zip]
    output: Dataframe with two additional variables (latitude, longitude)
    """
    zip_code = pd.DataFrame()
    for ind in data_frame.index:
        lon_lat = combine_address_and_return_url(data_frame.loc[ind].values)
        if lon_lat:
            zip_code.loc[ind, "lon"] = lon_lat[0]
            zip_code.loc[ind, "lat"] = lon_lat[1]
    return zip_code


nh_homes_merge = lat_lon_getter(nh_homes)


# nh_homes_merge = pd.merge(
# nh_homes, zip_code, how="left", left_index=True, right_index=True
# )
# nh_homes_merge.reset_index(inplace=True)
# nh_homes_merge.drop(columns="index", inplace=True)

nh_homes_merge.to_csv("2567_nh_data/nh_address.csv")
nh_homes_arr = pa.Table.from_pandas(nh_homes)
pq.write_table(nh_homes_arr, "2567_nh_data/nh_address.parquet")

# get all NH homes from the files
# get their lat/lon


all_nh_names = pd.DataFrame()
for csv in os.listdir("2567_nh_data/merged_data"):
    if "text" in csv:
        file_name = "2567_nh_data/merged_data/{}".format(csv)
        dat_read = pd.read_csv(file_name, index_col=0, dtype=str)
        nh_1 = dat_read[
            ~dat_read[
                ["facility_id", "facility_name", "address", "city", "state", "zip"]
            ].duplicated()
        ].copy()
        nh_1 = nh_1[["facility_id", "facility_name", "address", "city", "state", "zip"]]
        all_nh_names = pd.concat([all_nh_names, nh_1])


nh2 = pd.merge(
    all_nh_names, nh_homes, how="left", on=["address", "city", "state", "zip"]
)
# batch of NH addresses not pinged previously
nh2 = nh2[nh2.lon.isna()].copy()
cols_to_drop = ["lon", "lat"]  # , "city_y", "state_y", "zip_y"]
nh2.drop(columns=cols_to_drop, inplace=True)


# nh2_homes = lat_lon_getter(nh2)

zip_code = pd.DataFrame()
# for ind in nh_homes.index:
for ind in nh2.index:
    # print(nh2.loc[ind].values)
    lon_lat = combine_address_and_return_url(nh2.loc[ind].values)
    if lon_lat:
        # print(lon_lat)
        zip_code.loc[ind, "lon"] = lon_lat[0]
        zip_code.loc[ind, "lat"] = lon_lat[1]


# with batch 2

nh_add2 = pd.merge(nh2, zip_code, how="left", left_index=True, right_index=True)
nh_add_batch2 = pd.concat([nh_homes, nh_add2[~nh_add2.lat.isna()]])
nh_add_batch2.to_csv("2567_nh_data/nh_address2.csv")

# batch 3
nh_add3 = nh_add2[nh_add2.lat.isna()].copy()
nh_add3.sort_index(ascending=True, inplace=True)
nh_add3.drop(columns=cols_to_drop, inplace=True)

zip_code = pd.DataFrame()
count = 0
# for ind in nh_homes.index:
for ind in nh_add3.index:
    count += 1
    if count < 2500:
        lon_lat = combine_address_and_return_url(nh_add3.loc[ind].values)
        if lon_lat:
            # print(lon_lat)
            zip_code.loc[ind, "lon"] = lon_lat[0]
            zip_code.loc[ind, "lat"] = lon_lat[1]
        else:
            zip_code.loc[ind, "nolatlon"] = 1


nh_add3_1 = pd.merge(nh_add3, zip_code, how="left", left_index=True, right_index=True)
nh_add_batch2 = pd.concat([nh_homes, nh_add2[~nh_add2.lat.isna()]])
nh_add_batch2.to_csv("2567_nh_data/nh_address2.csv")
zip_code.loc[zip_code.nolatlon.isna()]
zip_code.drop(columns="nolatlon", inplace=True)


nh_add4 = nh_add3_1[((nh_add3_1.lon.isna()) & (nh_add3_1.nolatlon.isna()))].copy()
nh_add4.drop(columns=cols_to_drop, inplace=True)
nh_add4.drop(columns="nolatlon", inplace=True)

zip_code = pd.DataFrame()
count = 0
# for ind in nh_homes.index:
for ind in nh_add4.index:
    count += 1
    if count < 3500:
        lon_lat = combine_address_and_return_url(nh_add4.loc[ind].values)
        if lon_lat:
            # print(lon_lat)
            zip_code.loc[ind, "lon"] = lon_lat[0]
            zip_code.loc[ind, "lat"] = lon_lat[1]
        else:
            zip_code.loc[ind, "nolatlon"] = 1

nh_add5_1 = pd.merge(nh_add4, zip_code, how="left", left_index=True, right_index=True)
nh_add5_2 = nh_add5_1[((nh_add5_1.lon.isna()) & (nh_add5_1.nolatlon.isna()))].copy()


zip_code = pd.DataFrame()
count = 0
# for ind in nh_homes.index:
for ind in nh_add5_2.index:
    if count < 1500:
        lon_lat = combine_address_and_return_url(nh_add5_2.loc[ind].values[:-3])
        if lon_lat:
            # print(lon_lat)
            zip_code.loc[ind, "lon"] = lon_lat[0]
            zip_code.loc[ind, "lat"] = lon_lat[1]
        else:
            zip_code.loc[ind, "nolatlon"] = 1
    elif 1500 < count <= 2000:
        lon_lat = combine_address_and_return_url(
            nh_add5_2.loc[ind].values[:-3], geo_coder=True
        )
        if lon_lat:
            # print(lon_lat)
            zip_code.loc[ind, "lon"] = lon_lat[0]
            zip_code.loc[ind, "lat"] = lon_lat[1]
        else:
            zip_code.loc[ind, "nolatlon"] = 1
    count += 1

nh_5_address = pd.merge(
    nh_add5_2, zip_code, how="left", left_index=True, right_index=True
)

nh_5_add_data = nh_5_address[nh_5_address.lon.isna()].copy()
nh_5_add_data.drop(columns=["lon_x", "lat_x", "nolatlon_x"], inplace=True)


zip_code = pd.DataFrame()
count = 0
# for ind in nh_homes.index:
for ind in nh_5_add_data.index:
    if count < 1500:
        lon_lat = combine_address_and_return_url(nh_5_add_data.loc[ind].values[:-3])
        if lon_lat:
            # print(lon_lat)
            zip_code.loc[ind, "lon"] = lon_lat[0]
            zip_code.loc[ind, "lat"] = lon_lat[1]
        else:
            zip_code.loc[ind, "nolatlon"] = 1
    elif 1500 < count <= 7000:
        lon_lat = combine_address_and_return_url(
            nh_add5_2.loc[ind].values[:-3], geo_coder=True
        )
        if lon_lat:
            # print(lon_lat)
            zip_code.loc[ind, "lon"] = lon_lat[0]
            zip_code.loc[ind, "lat"] = lon_lat[1]
        else:
            zip_code.loc[ind, "nolatlon"] = 1
    count += 1

zip_code = pd.DataFrame()
count = 0
# for ind in nh_homes.index:
for ind in nh_add7.index:
    if count <= 700:
        lon_lat = combine_address_and_return_url(nh_add7.loc[ind].values[:-3])
        if lon_lat:
            # print(lon_lat)
            # zip_code.loc[ind, "lon"] = lon_lat[0]
            # zip_code.loc[ind, "lat"] = lon_lat[1]
            nh_add7.loc[ind, "lon"] = lon_lat[0]
            nh_add7.loc[ind, "lat"] = lon_lat[1]
        else:
            # zip_code.loc[ind, "nolatlon"] = 1
            nh_add7.loc[ind, "nolatlon"] = 1
    elif 700 < count <= 1491:
        lon_lat = combine_address_and_return_url(
            nh_add7.loc[ind].values[:-3], geo_coder=True
        )
        if lon_lat:
            # print(lon_lat)
            # zip_code.loc[ind, "lon"] = lon_lat[0]
            # zip_code.loc[ind, "lat"] = lon_lat[1]
            nh_add7.loc[ind, "lon"] = lon_lat[0]
            nh_add7.loc[ind, "lat"] = lon_lat[1]
        else:
            # zip_code.loc[ind, "nolatlon"] = 1
            nh_add7.loc[ind, "nolatlon"] = 1

    count += 1

# Joining All CSV files
# ~~~~~~~~~~~~~~~~~~~
all_nh_data = pd.DataFrame()
for csv in os.listdir("2567_nh_data/merged_data"):
    if "text" in csv:
        file_name = "2567_nh_data/merged_data/{}".format(csv)
        nh_1 = pd.read_csv(file_name, index_col=0, dtype=str)
        all_nh_data = pd.concat([all_nh_data, nh_1])

def del_columns(datname, colname2drop):
    return datname.drop(
        columns=datname.columns[datname.columns.str.startswith(colname2drop)]
    )

drop_cols = ("Footnote", "Performance", "Baseline", "cycle", "CYCLE")
nh = del_columns(all_nh_data, drop_cols)
nh.columns = nh.columns.str.lower()

nh_homes_data = pa.Table.from_pandas(nh)
pq.write_table(nh_homes_data, "2567_nh_data/all_nh_data.parquet")
