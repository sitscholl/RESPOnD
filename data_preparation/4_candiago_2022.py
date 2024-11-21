import pandas as pd
from pathlib import Path
from functions import map_primes, fuzzy_match
import numpy as np

##############
##PDOs and Australian Atlas
##############
pdo_analytical = pd.read_excel('../data/PDO/EU_PDO_id_varieties.xlsx', na_values = 'na')
pdo_melt = pdo_analytical.melt(id_vars = ['Country', 'PDOid', 'PDOnam'], value_vars = ['Main_vine_varieties', 'Other_vine_varieties'], value_name='Original Name').dropna(subset = 'Original Name')
pdo_melt['Original Name'] = pdo_melt['Original Name'].str.split('; ')
pdo_melt = pdo_melt.explode('Original Name').rename(columns = {'Country': 'Country Code'})

pdo_nam_corrections = {
    'Meslier Saint-François B': 'meslier saint francois b',
    'Vernaccia Di S. Giminiano B': 'vernaccia di san gimignano b',
    'Moschato Alexandreias B': 'moschato alexandrias b',
    'Moscatel Aromática (Maybe Synonym Of Moscatel De Alejandria?)': 'moscatel aromatico',
    "Avana' N": "avana n",
    "Ruche' N": "ruche n",
    'Roter Vetliner B': 'roter veltliner b',
    "Giro' N": "giro n",
    'Piculit-Neri N': 'piculit neri n',
    'Cesanese D Affile N': "cesanese d'affile n",
    'Moscato D Amburgo N': "moscato d'amburgo n",
    'Fetească Neagrăn': 'feteasca neagra',
    'Băbeasacă Neagră N': 'babeasca neagra n',
    'Băbeasacă Gri G': 'babeasca gris g',
    'Blaufrankisch N': 'blaufränkisch n',
    'Bilan Bijeli B': 'bilan b',
    'Croatina Crna': 'croatina n',
    'Ošljevina Bijela B': 'osljevina b',
    'Rušljin Crni': 'rusljin n',
    'Trojšćina Crvena N': 'trojiscina crvena n',
    'Veltliner Roter Rs': 'veltliner rot rs',
    'Belina Hižakovo': 'belina hizakovec',
    'Dišeća Belina Bijela': 'diseca belina b',
    'Roter Räuschling Rg': 'raeuschling rot rg',
    'Saint-Côme B': 'saint come b',
    "Montu' B": "montu b",
    'Incrocio Manzoni 2-14 N': 'incrocio manzoni 2.14 n',
    "Uva Del Tunde' N": "uva del tunde n",
    "Scimiscia' B": "scimiscia b"
    }

##Manual corrections
pdo_melt['Original Name'] = [pdo_nam_corrections[i] if i in pdo_nam_corrections.keys() else i for i in pdo_melt['Original Name']]

##1. For each variety in the PDO Dataset, find all possible Prime names from the VIVC database
pdo_map_file = Path('mappings/pdo_name_map_old.csv')
if not pdo_map_file.is_file():
    pdo_name_map = map_primes(pdo_melt['Original Name'].unique())
    pdo_name_map.to_csv(pdo_map_file, index = False)
else:
    pdo_name_map = pd.read_csv(pdo_map_file)

pdo_name_map["Primes VIVC"] = pdo_name_map["Primes VIVC"].replace({"[Function error!]": np.nan})
pdo_name_map['Primes VIVC'] = pdo_name_map['Primes VIVC'].map(lambda x: x.split('; ') if x == x else x)

####2. Select the prime name that is equal/highly similar to the original name
pdo_name_map[["Prime Name", "Score"]] = pd.DataFrame(
    pdo_name_map.apply(
        lambda x: fuzzy_match(x["Decoded Name"], x["Primes VIVC"]), axis=1
    ).tolist(),
    index=pdo_name_map.index,
)
pdo_name_map['Prime Name'] = pdo_name_map['Prime Name'].replace('', np.nan)

###3. Remove mappings with low score
pdo_name_map.loc[pdo_name_map['Score'] < 90, 'Prime Name'] = np.nan

###4. Check mappings for vars that intersect with parker manually
tbl_parker = pd.read_csv('prepared_data/parker_2013.csv').drop('Unnamed: 0', axis = 1)
if tbl_parker['Prime Name'].dropna().duplicated().any():
    raise ValueError('Duplicated Prime Names in tbl_parker!')

man_check = tbl_parker[['Variety', 'Prime Name']].rename(columns = {'Variety': 'Original Parker'}).merge(pdo_name_map.dropna(subset = 'Prime Name'), on = 'Prime Name', how = 'left')
man_check.sort_values(['Original Parker', 'Score'], inplace = True)
#man_check.drop(['Original Name'], axis = 1, inplace = True)

man_dict = {
    "carignane n": "CARIGNAN NOIR",
    "tramin cerveny rs": "GEWUERZTRAMINER",
    "roter traminer rs": "GEWUERZTRAMINER",
    "traminer roz rs": "GEWUERZTRAMINER",
    "tinta n": np.nan,
    "orbois b": "ARBOIS BLANC",
    "weisser riesling b": "RIESLING WEISS",
    "sauvignonasse": "FRIULANO",
    "zeleni sauvignon b": "FRIULANO",
    "sauvignon": "SAUVIGNON BLANC",
    "ugni blanc b": "TREBBIANO TOSCANO",
    "trebbiano": np.nan,
    'mavro n': np.nan,
    "riesling b": "RIESLING WEISS",
    'grenache noir': "GARNACHA TINTA",
    'grenache noir n': "GARNACHA TINTA",
    "blauer elbling n": "ELBLING BLAU",
    "pikolit b": "PICOLIT",
    "svrdlovina crna n": "SVERDLOVINA",
    "cirfandli r": "ZIERFANDLER ROT"
}
for a, b in man_dict.items():
    pdo_name_map.loc[pdo_name_map['Decoded Name'] == a, ['Prime Name', 'Score']] = (b, 100)

matches_count = len(set(tbl_parker['Prime Name']).intersection(pdo_name_map['Prime Name'].unique()))
print(f"For {matches_count} varieties a match between the PDO dataset and the parker dataset was found")

miss_pdo = pdo_name_map.loc[pdo_name_map['Prime Name'].isna()]
print(f"{len(miss_pdo)} variety names from PDO dataset without prime name.")

names_regions = pdo_melt.merge(pdo_name_map[['Original Name', 'Prime Name']], on = 'Original Name', how = 'left', validate = 'many_to_one')

names_regions.to_csv('prepared_data/candiago_2022.csv')
