import pandas as pd
import geopandas as gpd
import numpy as np

parker = pd.read_csv('prepared_data/parker_2013.csv').rename(columns = {'Variety': 'Variety Parker'})
eurostat = gpd.read_file('prepared_data/eurostat_2020.shp').rename(columns = {'Variety': 'Variety Eurostat'})
candiago = pd.read_csv('prepared_data/candiago_2022.csv').rename(columns = {'Original Name': 'Variety Candiago'})

cmp = parker[["Prime Name", "Variety Parker"]].merge(
    eurostat[["Prime Name", "Variety Eurostat"]]
    .drop_duplicates()
    .dropna(subset="Prime Name"),
    on="Prime Name",
    how="left",
)
cmp = cmp.merge(
    candiago[["Prime Name", "Variety Candiago"]]
    .drop_duplicates()
    .dropna(subset="Prime Name"),
    on="Prime Name",
    how="left",
)

cmp.dropna(subset = ['Prime Name', 'Variety Candiago'], inplace = True)
cmp.sort_values('Prime Name', inplace = True)

n_vars = len(set(cmp['Prime Name']))
print(f'For {n_vars} Varieties, both the temperature sum and the cultivation locations from the PDOs could be identified.')

n_vars_area = len(set(cmp.dropna(subset = 'Variety Eurostat')['Prime Name']))
print(f'For {n_vars_area} Varieties, also data about the cultivation area is present.')

cmp.set_index('Prime Name').to_excel('mappings/dataset_comparison.xlsx')
