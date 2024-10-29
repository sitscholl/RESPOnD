import pandas as pd
import re
import pycountry
from pathlib import Path
from functions import fuzzy_match, map_primes
import numpy as np

tbl_australia = pd.read_csv('../data/australian_wine_atlas/variety_area_2010.csv')
tbl_australia['Country'] = tbl_australia['Country'].replace({'Bulgaria_incBulgaria': 'Bulgaria', 'Korea, Rep.': 'Korea, Republic of', 'Turkey': 'Türkiye'})

country_map = tbl_australia[['Country']].drop_duplicates()
country_map['Country Code'] = [pycountry.countries.search_fuzzy(i)[0].alpha_2 for i in country_map['Country']]

tbl_australia = tbl_australia.merge(country_map, on = 'Country', how = 'left')
tbl_australia['Variety_color'] = tbl_australia['Variety_color'].replace({'W': 'B', 'R': 'N'})
tbl_australia['Original Name'] = tbl_australia['Variety'] + ' ' + tbl_australia['Variety_color']

australia_nam_corrections = {
 'Storgozia N': "storgoziya",
 'Meslier Saint-Francois B': "meslier saint francois b",
 'Saint-Pierre Dore B': "saint pierre dore b",
 'Morio-Muskat B': "morio muskat b",
 'Roviello Bianco B': "roviello b",
 'Rubinovy Magaracha N': "rubinovyi magaracha n",
 'Alb de Ialoveni B': "alb de ialoven b",
 'Tinta de Alcoa N': "tinta de alcobaca n"}

tbl_australia['Original Name'] = [australia_nam_corrections[i] if i in australia_nam_corrections.keys() else i for i in tbl_australia['Original Name']]

nams_australia = tbl_australia['Original Name'].unique()

australia_map_file = Path("mappings/australia_name_map.csv")
if not australia_map_file.is_file():
    australia_name_map = map_primes(nams_australia)
    australia_name_map.to_csv('mappings/australia_name_map.csv')
else:
    australia_name_map = pd.read_csv(australia_map_file).rename(columns = {'Variety Prime': 'Primes VIVC'})

australia_name_map["Primes VIVC"] = australia_name_map["Primes VIVC"].replace({"[Function error!]": np.nan})

australia_name_map["Primes VIVC"] = australia_name_map["Primes VIVC"].map(
    lambda x: x.split("; ") if x == x else x
)

australia_name_map[["Prime Name", "Score"]] = pd.DataFrame(
    australia_name_map.apply(
        lambda x: fuzzy_match(x["Decoded Name"], x["Primes VIVC"]), axis=1
    ).tolist(),
    index=australia_name_map.index,
)
australia_name_map['Prime Name'] = australia_name_map['Prime Name'].replace('', np.nan)

##Remove mappings with low score
australia_name_map.loc[australia_name_map['Score'] < 90, 'Prime Name'] = np.nan

##Check mappings for vars that intersect with parker manually
tbl_parker = pd.read_csv('prepared_data/parker_2013.csv').drop('Unnamed: 0', axis = 1)
if tbl_parker['Prime Name'].dropna().duplicated().any():
    raise ValueError('Duplicated Prime Names in tbl_parker!')

man_check = tbl_parker[['Variety', 'Prime Name']].rename(columns = {'Variety': 'Original Parker'}).merge(australia_name_map.dropna(subset = 'Prime Name'), on = 'Prime Name', how = 'left')
man_check.sort_values(['Original Parker', 'Score'], inplace = True)
man_check.drop(['Decoded Name'], axis = 1, inplace = True)

man_dict = {
    "riesling b": "RIESLING WEISS",
    "cornalin n": "ROUGE DU PAYS",
    'sauvignonasse b': 'FRIULANO',
    'Grenache noir': "GARNACHA TINTA",
    "cserszegi fűszeres g": "CSERSZEGI FUESZERES",
    "asirtiko red n": "ASSYRTIKO",
    "listain de huelva b": "MANTEUDO",
    'mavro n': np.nan
}
for a, b in man_dict.items():
    australia_name_map.loc[australia_name_map['Decoded Name'] == a, ['Prime Name', 'Score']] = (b, 100)

matches_count = len(set(tbl_parker['Prime Name']).intersection(australia_name_map['Prime Name'].unique()))
print(f"For {matches_count} varieties a match between the parker dataset and the anderson dataset was found")

nans = australia_name_map['Prime Name'].isna().sum()
print(f"For {nans} varieties, no Prime name could be determined.")

tbl_australia_prime = tbl_australia.merge(australia_name_map[['Original Name', 'Prime Name']], on = 'Original Name', how = 'left', validate = 'many_to_one')
tbl_australia_prime.drop(['Variety'], axis = 1, inplace = True)

tbl_australia_prime.to_csv('prepared_data/anderson_2010.csv', index = False)
