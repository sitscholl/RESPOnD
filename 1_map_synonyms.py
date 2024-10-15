import pandas as pd
import re
from unidecode import unidecode
import pycountry
from pathlib import Path
from functions import intersect_cells, fetch_vivc, pdo_nam_corrections, australia_nam_corrections

pdo_analytical = pd.read_excel('../data/PDO/EU_PDO_id_varieties.xlsx', na_values = 'na')
pdo_melt = pdo_analytical.melt(id_vars = ['Country', 'PDOid', 'PDOnam'], value_vars = ['Main_vine_varieties', 'Other_vine_varieties'], value_name='Original Name').dropna(subset = 'Original Name')
pdo_melt['Original Name'] = pdo_melt['Original Name'].str.split('; ')
pdo_melt = pdo_melt.explode('Original Name').rename(columns = {'Country': 'Country Code'})

original_names = pdo_melt['Original Name'].unique()

chars = list(set(''.join(original_names)))
chars_map = {i: unidecode(i, errors = 'strict') for i in chars if (i != unidecode(i))}
chars_map.update({'ä':'ae', 'ü':'ue', 'ö':'oe'})
dict_translate = str.maketrans(chars_map)

var_names_decode = [i.lower().translate(dict_translate) for i in original_names]
##Manual corrections
var_names_decode = [pdo_nam_corrections[i] if i in pdo_nam_corrections.keys() else i for i in var_names_decode]

# var_names_strip = [re.sub(' (b|g|gr|n|r|rg|rs)$', '', i) for i in var_names_decode]

##1. For each variety in the PDO Dataset, find all possible Prime names from the VIVC database
pdo_map_file = Path('mappings/pdo_name_map.csv')
if not pdo_map_file.is_file():
    prime_names = [fetch_vivc(i) for i in var_names_decode]
    pdo_name_map = pd.DataFrame(zip(original_names, var_names_decode, prime_names), columns = ['Original Name', 'Decoded Name', 'Primes VIVC'])
    pdo_name_map.to_csv(pdo_map_file, index = False)
else:
    pdo_name_map = pd.read_csv(pdo_map_file)

pdo_name_map['Primes VIVC'] = pdo_name_map['Primes VIVC'].map(lambda x: set(x.split('; ')) if x == x else x)
miss_pdo = pdo_name_map.loc[pdo_name_map['Primes VIVC'].isna(), 'Decoded Name'].to_list()
print(f"{len(miss_pdo)} variety names from PDO dataset without possible prime names.")

names_regions = pdo_melt.merge(pdo_name_map, on = 'Original Name', how = 'left', validate = 'many_to_one')
names_regions.isna().sum()

###Where multiple possible prime names for one original name:
####1. Select the prime name(s) which also appear in Australian dataset for this country
tbl_australia = pd.read_csv('../data/australian_wine_atlas/variety_area_2010.csv')
tbl_australia['Country'] = tbl_australia['Country'].replace({'Bulgaria_incBulgaria': 'Bulgaria', 'Korea, Rep.': 'Korea, Republic of', 'Turkey': 'Türkiye'})

country_map = tbl_australia[['Country']].drop_duplicates()
country_map['Country Code'] = [pycountry.countries.search_fuzzy(i)[0].alpha_2 for i in country_map['Country']]

missing_cntrs = set(pdo_melt['Country Code']).difference(country_map['Country Code'])
print(f'Countries missing in Australian Dataset: {", ".join(missing_cntrs)}')

tbl_australia = tbl_australia.merge(country_map, on = 'Country', how = 'left')
tbl_australia['Variety_color'] = tbl_australia['Variety_color'].replace({'W': 'B', 'R': 'N'})
tbl_australia['Original Name'] = tbl_australia['Variety'] + ' ' + tbl_australia['Variety_color']

nams_australia = tbl_australia['Original Name'].unique()
nams_australia_decode = [i.lower().translate(dict_translate) for i in nams_australia]
##Remove brackets from some variety names
nams_australia_decode = [re.sub(r'\([^)]*\) ?', '', i).rstrip(' ') for i in nams_australia_decode]
##Manual corrections
nams_australia_decode = [australia_nam_corrections[i] if i in australia_nam_corrections.keys() else i for i in nams_australia_decode]

australia_map_file = Path("mappings/australia_name_map.csv")
if not australia_map_file.is_file():
    primes_australia = [fetch_vivc(i, select_prime=True) for i in nams_australia_decode]
    australia_name_map = pd.DataFrame(zip(nams_australia, nams_australia_decode, primes_australia), columns = ['Original Name', 'Decoded Name', 'Variety Prime'])
    australia_name_map.to_csv(australia_map_file, index = False)
else:
    australia_name_map = pd.read_csv(australia_map_file)

tbl_australia_prime = tbl_australia.merge(australia_name_map, on = 'Original Name', how = 'left')
tbl_australia_cntr = tbl_australia_prime.groupby('Country Code', as_index = False)['Variety Prime'].agg(lambda x: '; '.join(x.dropna()))
tbl_australia_cntr.rename(columns = {'Variety Prime': 'Primes Anderson'}, inplace = True)
tbl_australia_cntr['Primes Anderson'] = tbl_australia_cntr['Primes Anderson'].map(lambda x: set(x.split('; ')) if x == x else x)

names_regions = names_regions.merge(tbl_australia_cntr, on = 'Country Code', how = 'left')
names_regions['Primes Intersect'] = names_regions.apply(lambda x: intersect_cells(x['Primes VIVC'], x['Primes Anderson']), axis = 1)

####2. Select the prime name which appears in other PDOs of the same country
# pdo_names_cntr = names_regions.groupby('Country Code', as_index = False)['Primes Intersect'].apply(lambda x: set().union(*x))
# primes_pdos = []
# for cntr, pid in zip(names_regions['Country Code'], names_regions['PDOid']):
#     rows_other = names_regions.loc[(names_regions['Country Code'] == cntr) & (names_regions['PDOid'] != pid)]
#     primes_pdos.append(set().union(*rows_other['Primes Intersect']))
# names_regions['Primes PDOs'] = primes_pdos
# names_regions['Primes Intersect2'] = names_regions.apply(lambda x: intersect_cells(x['Primes Intersect'], x['Primes PDOs']), axis = 1)

####3. Select the prime name that is equal/highly similar to the original name
from thefuzz import process
def fuzzy_match(x, choices):
    if not choices == choices:
        return(('', 0))
    elif len(choices) == 1:
        return((list(choices)[0], 100))
    else:
        x_strip = re.sub(' (b|g|gr|n|r|rg|rs)$', '', x) #remove color code
        return(process.extractOne(x_strip, choices))
names_regions[["Primes fuzzy match", "Score"]] = pd.DataFrame(
    names_regions.apply(
        lambda x: fuzzy_match(x["Decoded Name"], x["Primes Intersect"]), axis=1
    ).tolist(),
    index=names_regions.index,
)

names_strings = names_regions.copy()
for col in ['Primes VIVC', 'Primes Anderson', 'Primes Intersect']:
    names_strings[col] = names_strings[col].map(lambda x: '; '.join(sorted(list(x))) if x == x else '')
comparison_names = names_strings[['Original Name', 'Decoded Name', 'Primes Intersect', 'Primes fuzzy match', 'Score']].drop_duplicates()

#comparison_names.to_csv('mappings/pdo_names_matches.csv')
#names_strings.to_csv('mappings/pdos_primes.csv')