# Continuing with Geocoding of the NHs

nh_add = pq.read_table("all_nh_address.parquet").to_pandas()

import geopy
from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="sshetty@air.org")

count = 0
nh_miss = nh_add.loc[nh_add.lon.isna() & nh_add.no_lat_lon.isna()]
for ind in nh_miss.index:
        if nh_miss.loc[ind, 'lon']!=nh_miss.loc[ind, 'lon']:
            if count < 500:
                add_ress= nh_miss.loc[ind, 'address']+", "+nh_miss.loc[ind, 'city']+", "+nh_miss.loc[ind, 'city']+", "+nh_miss.loc[ind, 'zip']
                try:
                    location = geolocator.geocode(add_ress)
                    if location:
                        #print(location.longitude, ind)
                        nh_add.loc[ind, 'lon'] = location.longitude
                        nh_add.loc[ind, 'lat'] = location.latitude
                        nh_add.loc[ind,'no_lat_lon'] = 1
                    else:
                        nh_add.loc[ind,'no_lat_lon'] = 0
                except:
                    None
                count+=1

nh_add.lon.value_counts(dropna=False)

nh_add.drop(columns='no_lat_lon', inplace=True)

nh_add.no_lat_lon_found.value_counts(dropna=False)

nh_address = pa.Table.from_pandas(nh_add)
pq.write_table(nh_address, "all_nh_address.parquet")

nh_add.shape

nh_add.to_csv("all_nh_geocoded.csv")