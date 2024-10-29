import pandas as pd
import geopandas as gpd
import numpy as np
from unidecode import unidecode
from functions import fetch_vivc, fuzzy_match, map_primes

tbl_parker = pd.read_excel('../data/parker_2013.xlsx', sheet_name = 'Veraison')

original_names = tbl_parker['Variety'].unique()
prime_mapping = map_primes(original_names)

prime_mapping["Primes VIVC"] = prime_mapping["Primes VIVC"].map(
    lambda x: x.split("; ") if x == x else x
)

prime_mapping[["Prime Name", "Score"]] = pd.DataFrame(
    prime_mapping.apply(
        lambda x: fuzzy_match(x["Decoded Name"], x["Primes VIVC"]), axis=1
    ).tolist(),
    index=prime_mapping.index,
)

##Drop varieties with unsure mapping
drop_vars = ["Verdelho", "Pinot noir Cortaillod", "Gouais blanc"]
prime_mapping = prime_mapping.loc[~prime_mapping['Original Name'].isin(drop_vars)].copy()

##Manual corrections for some varieties
man_dict = {
    "Riesling": "RIESLING WEISS",
    "Cabernet-Sauvignon": "CABERNET SAUVIGNON",
    "Aghiorgitiko": "AGIORGITIKO",
    "Ugni blanc": "TREBBIANO TOSCANO",
    "Cornalin": "ROUGE DU PAYS",
    'Sauvignon': 'SAUVIGNON BLANC',
    'Grenache noir': "GARNACHA TINTA",
    'Chasselas': "CHASSELAS BLANC",
    #'Enfariné noir': "GRACIANO"
}
for a, b in man_dict.items():
    prime_mapping.loc[prime_mapping['Original Name'] == a, 'Prime Name'] = b

if prime_mapping.dropna(subset = 'Primes VIVC')['Prime Name'].duplicated().any():
    print(prime_mapping.loc[prime_mapping['Prime Name'].duplicated(keep = False)])
    raise ValueError('Duplicated Prime Names detected! Check mappings manually.')

prime_mapping.rename(columns = {'Original Name': 'Variety'}, inplace = True)
prime_mapping['Prime Name'] = prime_mapping['Prime Name'].replace({'': np.nan})
tbl_parker_primes = tbl_parker.merge(prime_mapping[['Variety', 'Prime Name']], on = 'Variety', how = 'left')

nans = tbl_parker_primes['Prime Name'].isna().sum()
print(f"For {nans} varieties Prime Name could not be determined.")
print(tbl_parker_primes.loc[tbl_parker_primes['Prime Name'].isna()])

# tbl_parker_primes.to_csv('prepared_data/parker_2013.csv')

##Compare names to Eurostat dataset
eurostat = gpd.read_file('prepared_data/eurostat_2020.shp')
missing_vars = set(tbl_parker_primes['Prime Name']).difference(set(eurostat['Prime Name']))

tbl_parker_primes.loc[tbl_parker_primes['Prime Name'].isin(missing_vars)].sort_values('Number of observations')
