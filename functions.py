import re
import pandas as pd
import numpy as np
from thefuzz import process
from unidecode import unidecode

def fetch_vivc(s):

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
        species_nam = re.sub(r' (b|g|gr|n|r|rg|rs)$', '', s)
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
        
        # Get all synonyms that match variety name exactly (if any) and then extract all possible prime names
        if any(tbl_vivc['Cultivar name'].str.lower() == species_nam):
            tbl_vivc = tbl_vivc.loc[tbl_vivc['Cultivar name'].str.lower() == species_nam]

        prime = '; '.join(tbl_vivc['Prime name'].unique())

        prime = np.nan if prime == 'No results found.' else prime
    except:
        prime = 'Function error!'

    return(prime)

def map_primes(nams_list):

    nams_lower = [i.lower() for i in nams_list]

    chars = list(set(''.join(nams_lower)))
    chars_map = {i: unidecode(i, errors = 'strict') for i in chars if (i != unidecode(i))}
    chars_map.update({'ä':'ae', 'ü':'ue', 'ö':'oe'})
    dict_translate = str.maketrans(chars_map)

    nams_decode = [i.translate(dict_translate) for i in nams_lower]
    nams_primes = [fetch_vivc(i) for i in nams_decode]

    return( pd.DataFrame(
        zip(nams_list, nams_decode, nams_primes),
        columns=["Original Name", "Decoded Name", "Primes VIVC"],
        )
    )

def fuzzy_match(x, choices):
    if not choices == choices:
        return(('', 0))
    elif len(choices) == 1:
        return((list(choices)[0], 100))
    else:
        x_strip = re.sub(' (b|g|gr|n|r|rg|rs)$', '', x) #remove color code
        return(process.extractOne(x_strip, choices))
