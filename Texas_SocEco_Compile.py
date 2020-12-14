import pandas as pd
import geopandas as gpd
import os
from mylib.layerfuse import layerfuse

GIS_FOLDER = os.path.join('D:/', 'Wejo Project','Data', 'GIS')

industry_taz_dat = pd.read_csv(os.path.join(GIS_FOLDER, 'TX 2016 Industry TAZ.csv'), dtype=str).rename({'taz': 'taz_name'}, axis=1)
for col in industry_taz_dat.columns[industry_taz_dat.columns!='taz_name']:
    # I am doing this column by column with for loop because there is a bug in Python.
    # That does not change dtype of multiple columns simultaneously after assignment.
    # https://github.com/pandas-dev/pandas/issues/24269
    industry_taz_dat[col] = pd.to_numeric(industry_taz_dat[col].str.replace(',',''))

industry_taz_dat['taz'] = industry_taz_dat['taz_name'].str.extract('^TAZ ([0-9A-Z]+),')
industry_taz_dat['county'] = industry_taz_dat['taz_name'].str.split(',').str[1].str.strip()
industry_taz_dat['state'] = industry_taz_dat['taz_name'].str.split(',').str[-1].str.strip()

county_fips = pd.read_csv(os.path.join(GIS_FOLDER, 'County_FIPS.csv'))
county_fips = county_fips.loc[county_fips['fip'] // 1000 == 48].rename({'fip':'county_fip'}, axis=1) # Select only counties in Texas
county_fips['county_fip'] = county_fips['county_fip'].map(lambda x: '%03d' % (x % 48000))
industry_taz_dat = industry_taz_dat.merge(county_fips, how='inner', on='county')
renamer = {'total': 'employment',
           'Agriculture, forestry, fishing and hunting, and mining': 'emp_agriculture',
           'Construction': 'emp_construct', 
           'Manufacturing': 'emp_manufacture', 
           'Wholesale trade': 'emp_wholesale', 
           'Retail trade': 'emp_retail',
           'Transportation and warehousing, and utilities': 'emp_transport', 
           'Information': 'emp_information',
           'Finance, insurance, real estate and rental and leasing': 'emp_finance',
           'Professional, scientific, management, administrative,  and waste management services': 'emp_scientific',
           'Educational, health and social services': 'emp_education',
           'Arts, entertainment, recreation, accommodation and food services': 'emp_entertainment',
           'Other services (except public administration)': 'emp_other',
           'Public administration': 'emp_administration', 
           'Armed forces': 'emp_military'}
industry_taz_dat = industry_taz_dat.rename(renamer, axis=1)

taz_shp = gpd.read_file(os.path.join(GIS_FOLDER, 'tl_2011_48_taz10', 'tl_2011_48_taz10.shp'))
taz_shp = taz_shp.merge(industry_taz_dat.rename({'taz': 'TAZCE10', 'county_fip': 'COUNTYFP10'}, axis=1), 
                        how='inner', on=['TAZCE10', 'COUNTYFP10'])
bg_shp = gpd.read_file(os.path.join(GIS_FOLDER, 'Texas_BG_2018'))
taz_shp = taz_shp.to_crs(bg_shp.crs)

bg_shp = layerfuse(bg_shp, taz_shp, size_cols=list(renamer.values()), show_overlap=True)

acs_dat = pd.read_csv(os.path.join(GIS_FOLDER, 'ACS TX 2018', 'ACS_bg_extract.csv'))
acs_dat['GEOID'] = acs_dat.apply(lambda x: f'48{x["COUNTY"].astype(int):03d}{x["TRACT"].astype(int):06d}{x["BLKGRP"].astype(int)}'.format(x), axis=1)
acs_dat = acs_dat.drop(['COUNTY', 'TRACT', 'BLKGRP'], axis=1)

bg_shp = bg_shp.merge(acs_dat, on='GEOID')
bg_shp = bg_shp.merge(county_fips, left_on='COUNTYFP', right_on='county_fip')

bg_shp = bg_shp.loc[bg_shp['COUNTYFP'].isin(['453', '491', '209', '055', '021'])]
taz_shp = taz_shp.loc[taz_shp['COUNTYFP10'].isin(['453', '491', '209', '055', '021'])]

# ax = bg_shp.plot(column=bg_shp['employment']/bg_shp.to_crs(3857).area)
# ax.set_ylim(30,30.5)
# ax = taz_shp.plot(column=taz_shp['employment']/taz_shp.to_crs(3857).area)
# ax.set_ylim(30,30.5)

bg_shp.to_file(os.path.join(GIS_FOLDER, 'Austin_SocEco_BG', 'Austin_SocEco_BG.shp'))
