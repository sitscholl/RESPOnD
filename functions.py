import re
import pandas as pd
import numpy as np

def intersect_cells(x, y):

    if (x != x):
        return(set()) 

    else:
        if type(x) == str:
            x = set(x.split('; '))
        if len(x) == 1:
            return(set(x))

        if (y != y):
            return(set(x))
        elif type(y) == str:
            y = set(y.split('; '))

        inters = x.intersection(y)

        if len(inters) == 0: 
            return(set(x))
        else:
            return(inters)

def fetch_vivc(s, select_prime = False):

    vivc_url = "https://www.vivc.de/index.php?per-page=50&CultivarnameSearch%5Bcultivarname%5D=&CultivarnameSearch%5Bleitname%5D=&CultivarnameSearch%5Bid%5D=&CultivarnameSearch%5Bspecies%5D=&CultivarnameSearch%5Bb_farbe%5D={skin_color}&CultivarnameSearch%5Bland%5D=&r=cultivarname%2Findex&CultivarnameSearch%5Bcultivarnames%5D=&CultivarnameSearch%5Bcultivarnames%5D=cultivarn&CultivarnameSearch%5Btext%5D={species}"

    color_mapping = {
        "b": "green",
        "g": "grey",
        "gr": "grey",
        "n": "black",
        "r": "red",
        "rg": "red",
        "rs": "rose",
    }

    if re.search(r" (b|g|gr|n|r|rg|rs)$", s):
        species_nam = re.sub(' (b|g|gr|n|r|rg|rs)$', '', s)
        skin_col = color_mapping[re.findall(r" (b|g|gr|n|r|rg|rs)$", s)[-1]]
    else:
        species_nam = s
        skin_col = ''

    # Generate search url and download possible matches
    try:
        url = vivc_url.format(species = species_nam.replace(' ', '+'), skin_color = skin_col)
        tbl_vivc = pd.read_html(url)[0]
        tbl_vivc.columns = tbl_vivc.columns.get_level_values(1)

        #Try search without color if no matches
        if (skin_col != '') and (tbl_vivc['Prime name'].unique()[0] == 'No results found.'):
            url = vivc_url.format(species = species_nam.replace(' ', '+'), skin_color = '')
            tbl_vivc = pd.read_html(url)[0]
            tbl_vivc.columns = tbl_vivc.columns.get_level_values(1)
        
        if (select_prime) and (species_nam in tbl_vivc['Prime name'].str.lower().unique()):
            return('; '.join(tbl_vivc.loc[tbl_vivc['Prime name'].str.lower() == species_nam, 'Prime name'].unique()))

        # Get all synonyms the match variety name exactly (if any) and then extract all possible prime names
        if any(tbl_vivc['Cultivar name'].str.lower() == species_nam):
            tbl_vivc = tbl_vivc.loc[tbl_vivc['Cultivar name'].str.lower() == species_nam]

        prime = '; '.join(tbl_vivc['Prime name'].unique())

        prime = np.nan if prime == 'No results found.' else prime
    except:
        prime = 'Function error!'

    return(prime)

pdo_nam_corrections = {
    'meslier saint-francois b': 'meslier saint francois b',
    'vernaccia di s. giminiano b': 'vernaccia di san gimignano b',
    'moschato alexandreias b': 'moschato alexandrias b',
    'moscatel aromatica (maybe synonym of moscatel de alejandria?)': 'moscatel aromatico',
    "avana' n": "avana n",
    "ruche' n": "ruche n",
    'roter vetliner b': 'roter veltliner b',
    "giro' n": "giro n",
    'piculit-neri n': 'piculit neri n',
    'cesanese d affile n': "cesanese d'affile n",
    'moscato d amburgo n': "moscato d'amburgo n",
    'feteasca neagran': 'feteasca neagra',
    'babeasaca neagra n': 'babeasca neagra n',
    'babeasaca gri g': 'babeasca gris g',
    'blaufrankisch n': 'blaufränkisch n',
    'bilan bijeli b': 'bilan b',
    'croatina crna': 'croatina n',
    'osljevina bijela b': 'osljevina b',
    'rusljin crni': 'rusljin n',
    'trojscina crvena n': 'trojiscina crvena n',
    'veltliner roter rs': 'veltliner rot rs',
    'belina hizakovo': 'belina hizakovec',
    'diseca belina bijela': 'diseca belina b',
    'roter raeuschling rg': 'raeuschling rot rg',
    'saint-come b': 'saint come b',
    "montu' b": "montu b",
    'incrocio manzoni 2-14 n': 'incrocio manzoni 2.14 n',
    "uva del tunde' n": "uva del tunde n",
    "scimiscia' b": "scimiscia b"
    }

australia_nam_corrections = {
 'storgozia n': "storgoziya",
 'meslier saint-francois b': "meslier saint francois b",
 'saint-pierre dore b': "saint pierre dore b",
 'morio-muskat b': "morio muskat b",
 'roviello bianco b': "roviello b",
 'rubinovy magaracha n': "rubinovyi magaracha n",
 'alb de ialoveni b': "alb de ialoven b",
 'tinta de alcoa n': "tinta de alcobaca n"}