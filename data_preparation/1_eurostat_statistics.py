import pandas as pd
import geopandas as gpd
import re
import glob
import numpy as np
from functions import map_primes, fuzzy_match
from pathlib import Path

# Prepare NUTS shp
nuts_files = glob.glob('../data/eurostat/NUTS/*.shp')
nuts_years = [int(i.split('_')[3]) for i in nuts_files]
shp_nuts = pd.concat([gpd.read_file(i) for i in nuts_files], keys = nuts_years)
shp_nuts = shp_nuts.loc[
    ((shp_nuts["LEVL_CODE"] == 2) & (shp_nuts["CNTR_CODE"] != "CY"))
    | ((shp_nuts["CNTR_CODE"] == "CY") & (shp_nuts["LEVL_CODE"] == 0))
].copy()
shp_nuts = (
    shp_nuts.droplevel(1)
    .reset_index()
    .rename(columns={"index": "NUTS Year", "NAME_LATN": "NUTS Name"})
)

tbl_years = []
for sname, year in zip(['Sheet 1', 'Sheet 2'], [2015, 2020]):

    #Load statistical data
    #Source: https://doi.org/10.2908/VIT_T4
    #Documentation: https://ec.europa.eu/eurostat/cache/metadata/Annexes/vit_esqrs_an_1.pdf
    tbl_eu_orig = pd.read_excel(
        "../data/eurostat/vineyard_statistics.xlsx",
        sheet_name=sname,
        na_values=["cd", "c", "d", "e", ":"],
        skiprows=8,
        header=[0, 1]
    )
    tbl_eu = tbl_eu_orig.iloc[:,[0,1,2]].dropna(how = 'any').copy()
    tbl_eu.columns = ['Region', 'Variety', 'Area [ha]']
    tbl_eu = tbl_eu.loc[~( (tbl_eu['Variety'].str.startswith('Other')) | (tbl_eu['Variety'].str.startswith('Total')) )].copy()

    tbl_eu['NUTS Year'] = tbl_eu['Region'].map(lambda x: int(re.search(r"\(NUTS ([0-9]{4})\)", x).group(1)) if re.search(r"\(NUTS ([0-9]{4})\)", x) is not None else 2021)
    tbl_eu['NUTS Name'] = tbl_eu['Region'].map(lambda x: re.sub(r" \(NUTS [0-9]{4}\)", '', x))
    tbl_eu['Area Year'] = f"Area {year} [ha]"

    ##Manual corrections
    nuts_corr = {'Centre (FR)': 'Centre', 'Ile de France': "Ile-de-France", "Cyprus": "Kýpros"}
    tbl_eu['NUTS Name'] = tbl_eu['NUTS Name'].replace(nuts_corr)

    gpd_eu = shp_nuts.drop('geometry', axis = 1).merge(tbl_eu, on = ['NUTS Name', 'NUTS Year'], how = 'inner')
    gpd_eu.drop(['Region', 'NUTS_NAME'], axis = 1, inplace = True)

    print(f"{len(set(tbl_eu['NUTS Name']).difference(gpd_eu['NUTS Name'].unique()))} regions dropped in join")
    tbl_years.append(gpd_eu)

tbl_years = pd.concat(tbl_years).pivot(columns = 'Area Year', values = 'Area [ha]', index = ['CNTR_CODE', 'NUTS Name', 'NUTS Year', 'Variety', 'NUTS_ID']).reset_index()

##Variety mappings for eurostat primes
eurostat_synonym_list = pd.read_excel('../data/eurostat/eurostat_variety_names.xlsx').rename(columns = {'Label': 'Variety'})
eurostat_synonym_list['Number'] = eurostat_synonym_list['Number'].ffill()
eurostat_synonym_list = eurostat_synonym_list.loc[~( (eurostat_synonym_list['Variety'].str.startswith('Other')) | (eurostat_synonym_list['Variety'].str.startswith('Total')) )].copy()
eurostat_synonym_list['Synonym Letter'] = eurostat_synonym_list['Code'].map(lambda x: re.search(r"[0-9]([A-Z])$", x).group(1) if re.search(r"[0-9]([A-Z])$", x) is not None else "")
eurostat_synonym_list['Variety'] = eurostat_synonym_list['Variety'].map(lambda x: x.replace('(Red)', '(R)').replace('(White)', '(W)').replace('(Other colour)', '(O)'))

eurostat_primes = eurostat_synonym_list.loc[eurostat_synonym_list['Synonym Letter'] == ""].copy().rename(columns = {'Variety': 'Prime'})

eurostat_mapping = eurostat_synonym_list.merge(eurostat_primes[['Number', 'Prime']], on = 'Number', how = 'left')

##Aggregate all prime names from eurostat
tbl_years_agg = tbl_years.merge(eurostat_mapping[['Code', 'Variety', 'Prime']], on = 'Variety', how = 'left')
print(f"For {len(tbl_years_agg.loc[tbl_years_agg['Prime'].isna(), 'Variety'].drop_duplicates())} varieties, no Prime name was found in eurostat table")
tbl_years_agg['Prime'] = tbl_years_agg['Prime'].fillna(tbl_years_agg['Variety'])

tbl_years_agg = tbl_years_agg.groupby(['CNTR_CODE', 'NUTS Name', 'NUTS_ID', 'NUTS Year', 'Code', 'Prime'], as_index = False)[['Area 2015 [ha]', 'Area 2020 [ha]']].sum()
tbl_years_agg.rename(columns = {'Prime': 'Variety'}, inplace = True)

##2. Map the eurostat prime names to VIVC prime names
original_names = np.sort(tbl_years_agg['Variety'].unique())
original_names_c = [i.replace(' (O)', '').replace(' (R)', ' (N)').replace(' (W)', ' (B)') for i in original_names]

eurostat_mapping_out = Path("mappings/eurostat_name_map.csv")
if not eurostat_mapping_out.is_file():
    eurostat_primes = map_primes(original_names_c)
    eurostat_primes['Original Name'] = original_names
    eurostat_primes.to_csv(eurostat_mapping_out, index = False)
else:
    eurostat_primes = pd.read_csv(eurostat_mapping_out)

eurostat_primes['Primes VIVC'] = eurostat_primes['Primes VIVC'].map(lambda x: x.split('; ') if x == x else x)

####3. Select the prime name that is equal/highly similar to the original name
eurostat_primes[["Prime Name", "Score"]] = pd.DataFrame(
    eurostat_primes.apply(
        lambda x: fuzzy_match(x["Decoded Name"], x["Primes VIVC"]), axis=1
    ).tolist(),
    index=eurostat_primes.index,
)

###4. Manual corrections
eurostat_corrs = {
    "blaufrankisch (n)": "BLAUFRAENKISCH",
    "cserszegi fuszeres (b)": "CSERSZEGI FUESZERES",
    "muller thurgau weiss (b)": "MUELLER THURGAU WEISS",
    "silvaner grun (b)": "SILVANER GRUEN",
}
for a, b in eurostat_corrs.items():
    eurostat_primes.loc[eurostat_primes['Decoded Name'] == a, ['Prime Name', 'Score']] = (b, 100)

## Filter out unreliable mappings
eurostat_primes.loc[eurostat_primes['Score'] < 90, 'Prime Name'] = np.nan

nans = eurostat_primes['Prime Name'].isna().sum()
print(f"For {nans} varieties Prime Name could not be determined.")
print(eurostat_primes.loc[eurostat_primes['Prime Name'].isna()])

tbl_years_agg_primes = tbl_years_agg.merge(eurostat_primes[['Original Name', 'Prime Name', 'Score']].rename(columns = {'Original Name': 'Variety'}))
tbl_years_agg_primes[['Area 2015 [ha]', 'Area 2020 [ha]']] = tbl_years_agg_primes[['Area 2015 [ha]', 'Area 2020 [ha]']].replace({0: np.nan})
tbl_years_agg_primes.dropna(subset = ['Area 2015 [ha]', 'Area 2020 [ha]'], how = 'all', inplace = True)

shp_years = shp_nuts[['NUTS_ID', 'NUTS Year', 'geometry']].merge(tbl_years_agg_primes, on = ['NUTS_ID', 'NUTS Year'], validate = 'one_to_many')
shp_years.to_file('prepared_data/eurostat_2020.shp')
